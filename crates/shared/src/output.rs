use std::io::IsTerminal;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{debug, info};

const PHASE_RULE_WIDTH: usize = 44;
const SGR_PHASE: &str = "1;36";
const SGR_BRAND: &str = "1;96";
const SGR_BULLET: &str = "36";
const SGR_LIVE: &str = "32";
const SGR_SKIP: &str = "2";
const SGR_LABEL: &str = "2";
const SGR_VALUE: &str = "1";

static ANSI: OnceLock<bool> = OnceLock::new();

/// One process-wide answer for narration and the tracing layer alike, resolved
/// once over stdout, the stream every runtime line rides (version banner,
/// narration, and the tracing formatter all write there).
pub fn ansi_enabled() -> bool {
    *ANSI.get_or_init(|| {
        resolve_ansi(
            std::env::var_os("NO_COLOR").is_some(),
            std::env::var("CLICOLOR_FORCE").ok().as_deref(),
            std::io::stdout().is_terminal(),
        )
    })
}

pub fn phase(title: &str) {
    let rendered = paint(SGR_PHASE, &phase_banner(title));
    info!("{rendered}");
}

pub fn version_banner(version: &str) -> String {
    format!(
        "{}  {}  {}",
        paint(SGR_BRAND, "PHOTON MARKET"),
        paint(SGR_VALUE, &format!("v{version}")),
        paint(SGR_LABEL, "commerce agents on one durable log")
    )
}

pub fn act(title: &str) {
    let bullet = paint(SGR_BULLET, "▸");
    info!("{bullet} {title}");
}

pub fn fact(label: &str, value: impl std::fmt::Display) {
    let label = paint(SGR_LABEL, &format!("{label:<12}"));
    let value = paint(SGR_VALUE, &value.to_string());
    info!("  {label}  {value}");
}

pub fn live_status(uptime: Duration, status: &str) {
    let marker = paint(SGR_LIVE, "◆ LIVE");
    let uptime = format_duration(uptime);
    info!("{marker} {uptime} | {status}");
}

/// The demo report's capability checklist line: one green live line or one dim
/// skip pointer per managed surface, printed exactly once per run.
pub fn managed_or_skip(feature: &str, enabled: bool) -> bool {
    if enabled {
        let marker = paint(SGR_LIVE, "✓");
        info!("{marker} {feature}");
    } else {
        let pointer =
            format!("○ {feature}: point LASER_CONNECTION_STRING at LaserData Cloud to run it live");
        let pointer = paint(SGR_SKIP, &pointer);
        info!("{pointer}");
    }
    enabled
}

/// The service-side capability gate: same decision, debug-level narration, so
/// a standalone service can explain itself under RUST_LOG=debug while the demo
/// report stays the one visible checklist.
pub fn gate(feature: &str, enabled: bool) -> bool {
    if !enabled {
        debug!("{feature} needs LaserData Cloud, using the local application path");
    }
    enabled
}

// NO_COLOR wins, a nonzero CLICOLOR_FORCE forces color on, otherwise color
// follows whether stdout is a terminal, so piped output stays plain.
fn resolve_ansi(no_color: bool, clicolor_force: Option<&str>, tty: bool) -> bool {
    if no_color {
        return false;
    }
    match clicolor_force {
        Some(force) if force != "0" => true,
        _ => tty,
    }
}

fn phase_banner(title: &str) -> String {
    let rule = "━".repeat(PHASE_RULE_WIDTH.saturating_sub(title.chars().count()));
    format!("━━ {title} {rule}")
}

fn paint(sgr: &str, text: &str) -> String {
    if ansi_enabled() {
        format!("\x1b[{sgr}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

#[cfg(test)]
mod tests {
    use super::{format_duration, phase_banner, resolve_ansi};
    use std::time::Duration;

    #[test]
    fn given_a_tty_when_no_overrides_then_should_enable_ansi() {
        assert!(resolve_ansi(false, None, true));
        assert!(!resolve_ansi(false, None, false));
    }

    #[test]
    fn given_no_color_when_set_then_should_win_over_everything() {
        assert!(!resolve_ansi(true, Some("1"), true));
    }

    #[test]
    fn given_clicolor_force_when_nonzero_then_should_enable_ansi_off_tty() {
        assert!(resolve_ansi(false, Some("1"), false));
        assert!(!resolve_ansi(false, Some("0"), false));
    }

    #[test]
    fn given_a_long_title_when_rendered_then_should_not_underflow() {
        let long = "a".repeat(200);
        let banner = phase_banner(&long);
        assert!(banner.starts_with("━━ "));
        assert!(banner.contains(&long));
    }

    #[test]
    fn given_a_short_title_when_rendered_then_should_pad_to_the_rule_width() {
        let banner = phase_banner("acts");
        assert!(banner.chars().filter(|c| *c == '━').count() > 30);
    }

    #[test]
    fn given_an_uptime_when_rendered_then_should_use_a_fixed_clock_width() {
        assert_eq!(format_duration(Duration::from_secs(3_661)), "01:01:01");
    }
}
