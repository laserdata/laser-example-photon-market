use tracing::{Event, Level, Metadata};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::{Context, Filter, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const NARRATION_TARGET: &str = "photon_shared::output";

// The default filter keeps the iggy transport at warn so the business story
// stays readable, and only then is the benign-noise suppression below applied.
// An explicit RUST_LOG takes the whole pipeline verbatim: the operator asked
// for exactly those events, so nothing is suppressed on top.
pub fn init_tracing() {
    let explicit = std::env::var_os(EnvFilter::DEFAULT_ENV).is_some();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,iggy=warn"));
    let ansi = crate::output::ansi_enabled();
    let narration = tracing_subscriber::fmt::layer()
        .with_ansi(ansi)
        .with_ansi_sanitization(false)
        .without_time()
        .with_level(false)
        .with_target(false)
        .with_filter(filter_fn(is_narration))
        .with_filter(filter.clone());
    let application = tracing_subscriber::fmt::layer()
        .with_ansi(ansi)
        .with_filter(filter_fn(|metadata| !is_narration(metadata)))
        .with_filter(filter);
    if explicit {
        let _ = tracing_subscriber::registry()
            .with(narration)
            .with(application)
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(narration)
            .with(application.with_filter(BenignTransportNoise))
            .try_init();
    }
}

fn is_narration(metadata: &Metadata<'_>) -> bool {
    metadata.target() == NARRATION_TARGET
}

// Three transport-layer ERROR lines the iggy client emits on paths the SDK treats
// as expected, so they would otherwise stain an otherwise-clean run:
//
// - `invalid_command`: raw Apache Iggy's reply to a managed-command probe, which
//   the builder reads as "managed surfaces unavailable" and downgrades to the
//   open feature set. One per connection on every raw-Iggy run.
// - `was cancelled`: an in-flight request whose future is dropped when its
//   connection closes during shutdown drain, which is the intended graceful
//   stop, not a failure.
// - `consumer_group_member_not_found`: the tail of the same drain, a final poll
//   racing the member's own leave after the group entry is already gone.
//
// The iggy client logs a plain message with no structured error-code field.
// Match its complete, target-specific message shape, only at ERROR. Every other
// error still prints, and an explicit RUST_LOG bypasses this filter entirely.
struct BenignTransportNoise;

const TCP_TARGET: &str = "iggy::tcp::tcp_client";
const QUIC_TARGET: &str = "iggy::quic::quic_client";
const INVALID_RESPONSE_PREFIX: &str = "Received an invalid response with status: ";

impl<S> Filter<S> for BenignTransportNoise {
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        true
    }

    fn event_enabled(&self, event: &Event<'_>, _cx: &Context<'_, S>) -> bool {
        let meta = event.metadata();
        if *meta.level() != Level::ERROR {
            return true;
        }
        let mut message = MessageField::default();
        event.record(&mut message);
        !message
            .rendered
            .as_deref()
            .is_some_and(|message| is_benign_transport_message(meta.target(), message))
    }
}

#[derive(Default)]
struct MessageField {
    rendered: Option<String>,
}

impl tracing::field::Visit for MessageField {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.rendered = Some(format!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.rendered = Some(value.to_owned());
        }
    }
}

fn is_benign_transport_message(target: &str, rendered: &str) -> bool {
    if target != TCP_TARGET && target != QUIC_TARGET {
        return false;
    }
    let message = rendered
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(rendered);
    is_expected_response(message, "invalid_command")
        || is_expected_response(message, "consumer_group_member_not_found")
        || is_cancelled_transport_task(target, message)
}

fn is_expected_response(message: &str, code: &str) -> bool {
    let suffix = format!(" ({code}).");
    message
        .strip_prefix(INVALID_RESPONSE_PREFIX)
        .and_then(|rest| rest.strip_suffix(&suffix))
        .is_some_and(|status| {
            !status.is_empty() && status.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn is_cancelled_transport_task(target: &str, message: &str) -> bool {
    let protocol = if target == TCP_TARGET { "TCP" } else { "QUIC" };
    let prefix = format!("Task execution failed during {protocol} request: task ");
    message
        .strip_prefix(&prefix)
        .and_then(|rest| rest.strip_suffix(" was cancelled"))
        .is_some_and(|task_id| {
            !task_id.is_empty() && task_id.bytes().all(|byte| byte.is_ascii_digit())
        })
}

#[cfg(test)]
mod tests {
    use super::BenignTransportNoise;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::error;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct Counting(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> Layer<S> for Counting {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn given_the_probe_rejection_when_logged_by_iggy_then_should_be_suppressed() {
        let seen = count_after(|| {
            error!(
                target: "iggy::tcp::tcp_client",
                "Received an invalid response with status: 3 (invalid_command)."
            );
        });
        assert_eq!(seen, 0);
    }

    #[test]
    fn given_the_shutdown_cancellation_when_logged_by_iggy_then_should_be_suppressed() {
        let seen = count_after(|| {
            error!(
                target: "iggy::tcp::tcp_client",
                "Task execution failed during TCP request: task 42 was cancelled"
            );
        });
        assert_eq!(seen, 0);
    }

    #[test]
    fn given_a_quic_shutdown_cancellation_when_logged_then_should_be_suppressed() {
        let seen = count_after(|| {
            error!(
                target: "iggy::quic::quic_client",
                "Task execution failed during QUIC request: task 7 was cancelled"
            );
        });
        assert_eq!(seen, 0);
    }

    #[test]
    fn given_the_drain_member_race_when_logged_by_iggy_then_should_be_suppressed() {
        let seen = count_after(|| {
            error!(
                target: "iggy::tcp::tcp_client",
                "Received an invalid response with status: 5006 (consumer_group_member_not_found)."
            );
        });
        assert_eq!(seen, 0);
    }

    #[test]
    fn given_an_unrelated_iggy_error_when_logged_then_should_pass_through() {
        let seen = count_after(|| {
            error!(target: "iggy::tcp::tcp_client", "Connection refused (os error 61)");
        });
        assert_eq!(seen, 1);
    }

    #[test]
    fn given_an_iggy_error_containing_a_benign_code_when_logged_then_should_pass_through() {
        let seen = count_after(|| {
            error!(
                target: "iggy::tcp::tcp_client",
                "server described invalid_command while closing the connection"
            );
        });
        assert_eq!(seen, 1);
    }

    #[test]
    fn given_a_similar_cancellation_when_logged_then_should_pass_through() {
        let seen = count_after(|| {
            error!(
                target: "iggy::tcp::tcp_client",
                "Task execution failed during TCP request: upload was cancelled"
            );
        });
        assert_eq!(seen, 1);
    }

    #[test]
    fn given_an_application_error_with_the_needle_when_logged_then_should_pass_through() {
        let seen = count_after(|| {
            error!(target: "photon_orders::saga", "the booking was cancelled by the carrier");
        });
        assert_eq!(seen, 1);
    }

    #[test]
    fn given_a_needle_in_a_non_message_field_when_logged_then_should_pass_through() {
        let seen = count_after(|| {
            error!(target: "iggy::tcp::tcp_client", reason = "was cancelled", "request failed");
        });
        assert_eq!(seen, 1);
    }

    fn count_after(emit: impl FnOnce()) -> usize {
        let counter = Counting::default();
        let seen = counter.0.clone();
        let subscriber =
            tracing_subscriber::registry().with(counter.with_filter(BenignTransportNoise));
        tracing::subscriber::with_default(subscriber, emit);
        seen.load(Ordering::SeqCst)
    }
}
