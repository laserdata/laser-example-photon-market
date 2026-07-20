use crate::error::SagaError;
use crate::events;
use crate::fulfillment::FulfillmentTask;
use crate::inventory::Inventory;
use crate::quotes::{self, QuotePanel};
use laser_sdk::agent::OnTimeout;
use laser_sdk::prelude::full::{Budget, InboxRoute, Router, StepContext};
use laser_sdk::prelude::{AgentTopic, Laser, LaserError};
use photon_shared::ShutdownWatch;
use photon_shared::domain::Timestamp;
use photon_shared::domain::order::{OrderEvent, OrderEventKind, PlaceOrder};
use photon_shared::domain::shipping::Booking;
use photon_shared::names::{AppAgent, KvSpace, WorkflowStep};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, warn};

// Fires when the configured boundary step's output is consumed to build the
// next step, which proves the boundary's journal record is already on the log.
// The run future is then dropped mid-flight: no compensations, no cleanup,
// exactly what a killed coordinator leaves behind. The journal is what a
// restarted coordinator resumes from.
struct CrashSwitch {
    after: Option<WorkflowStep>,
    trigger: Mutex<Option<oneshot::Sender<WorkflowStep>>>,
}

impl CrashSwitch {
    fn new(after: Option<WorkflowStep>, trigger: oneshot::Sender<WorkflowStep>) -> Self {
        Self {
            after,
            trigger: Mutex::new(Some(trigger)),
        }
    }

    fn reached(&self, completed: WorkflowStep) {
        if self.after == Some(completed)
            && let Some(trigger) = self
                .trigger
                .lock()
                .expect("crash trigger lock is not poisoned")
                .take()
        {
            let _ = trigger.send(completed);
        }
    }
}

/// Drive one order's fulfillment saga and settle its aftermath. Failure runs
/// compensations and releases inventory. Shutdown and simulated process death
/// leave the run incomplete so startup can resume its journal.
pub async fn supervise(
    laser: Laser,
    command: PlaceOrder,
    inventory: Arc<dyn Inventory>,
    crash_after: Option<WorkflowStep>,
    publisher: events::Publisher,
    mut shutdown: ShutdownWatch,
) {
    let result = tokio::select! {
        _ = shutdown.cancelled() => {
            let order = command.order;
            debug!("Paused fulfillment for order {order} at shutdown. The journal will resume it on restart.");
            return;
        }
        result = run_saga(&laser, &command, crash_after) => result,
    };
    match result {
        Ok(()) | Err(SagaError::Crashed(_)) => {}
        Err(SagaError::Failed(error)) => {
            let order = command.order;
            warn!("Fulfillment failed for order {order}. Releasing its reservation. {error}");
            if let Err(error) = inventory.release(command.order, &command.sku).await {
                warn!("Could not release the inventory reservation for order {order}. {error}");
                return;
            }
            let released = OrderEvent::new(
                &command,
                Timestamp::now(),
                OrderEventKind::InventoryReleased,
            );
            if let Err(error) = publisher.emit(&laser, &released).await {
                warn!(
                    "Released inventory for order {order}, but could not publish the release event. {error}"
                );
            }
        }
    }
}

pub async fn run_saga(
    laser: &Laser,
    command: &PlaceOrder,
    crash_after: Option<WorkflowStep>,
) -> Result<(), SagaError> {
    let run_id = command
        .order
        .to_string()
        .parse()
        .map_err(|_| LaserError::Invalid("order id is not a valid run id".to_owned()))?;
    let amount_cents = command
        .total()
        .ok_or_else(|| LaserError::Invalid("the order total overflows Money".to_owned()))?
        .cents();

    let charge = command.clone();
    let refund = command.clone();
    let quote = command.clone();
    let book = command.clone();
    let release = command.clone();
    let dispatch = command.clone();

    let (crash_tx, crash_rx) = oneshot::channel();
    let switch = Arc::new(CrashSwitch::new(crash_after, crash_tx));
    let (quote_switch, book_switch, dispatch_switch) =
        (switch.clone(), switch.clone(), switch.clone());

    let mut workflow = laser
        .workflow("fulfillment")
        .run_id(run_id)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .budget(
            Budget::unlimited()
                .invocations(8)
                .wall_clock(Duration::from_secs(60)),
        );
    // Register the run so `laser.runs()` can answer "what happened to order X",
    // only when the deployment serves the run registry. Raw Apache Iggy does not,
    // so the run stays local there.
    if laser.capabilities().await.agent_workflow {
        workflow = workflow.registered();
    }
    let charge_step = workflow
        .step(
            "charge",
            Router::to(AppAgent::Fulfillment.id()),
            move |_ctx: &StepContext<'_>| {
                task_bytes(&FulfillmentTask::Charge {
                    order: charge.order,
                    customer: charge.customer.clone(),
                    sku: charge.sku.clone(),
                    quantity: charge.quantity,
                    amount_cents,
                })
            },
        )
        .compensate_with(move |_ctx: &StepContext<'_>| {
            task_bytes(&FulfillmentTask::Refund {
                order: refund.order,
                customer: refund.customer.clone(),
                sku: refund.sku.clone(),
                quantity: refund.quantity,
                amount_cents,
            })
        });
    let charge_step = if laser.capabilities().await.kv.cas_fenced {
        charge_step
            .exclusive_in(KvSpace::Charges.to_string())
            .on_timeout(OnTimeout::Reassign)
    } else {
        charge_step
    };
    let run = charge_step
        .step(
            "quote",
            Router::to(AppAgent::Fulfillment.id()),
            move |_ctx: &StepContext<'_>| {
                quote_switch.reached(WorkflowStep::Charge);
                task_bytes(&FulfillmentTask::Quote {
                    order: quote.order,
                    quantity: quote.quantity,
                    ship_to: quote.ship_to.clone(),
                })
            },
        )
        .after("charge")
        .verify_with(quotes::verify_panel)
        .step(
            "book",
            Router::to(AppAgent::Fulfillment.id()),
            move |ctx: &StepContext<'_>| {
                book_switch.reached(WorkflowStep::Quote);
                task_bytes(&FulfillmentTask::Book {
                    order: book.order,
                    customer: book.customer.clone(),
                    sku: book.sku.clone(),
                    quantity: book.quantity,
                    panel: decode_panel(ctx),
                })
            },
        )
        .after("quote")
        .compensate_with(move |ctx: &StepContext<'_>| {
            let (carrier, booking) = decode_booking(ctx);
            task_bytes(&FulfillmentTask::Release {
                order: release.order,
                customer: release.customer.clone(),
                sku: release.sku.clone(),
                quantity: release.quantity,
                carrier,
                booking,
            })
        })
        .step(
            "dispatch",
            Router::to(AppAgent::Fulfillment.id()),
            move |ctx: &StepContext<'_>| {
                dispatch_switch.reached(WorkflowStep::Book);
                let (carrier, booking) = decode_booking(ctx);
                task_bytes(&FulfillmentTask::Dispatch {
                    order: dispatch.order,
                    customer: dispatch.customer.clone(),
                    sku: dispatch.sku.clone(),
                    quantity: dispatch.quantity,
                    carrier,
                    booking,
                })
            },
        )
        .after("book")
        .run();

    tokio::select! {
        outcome = run => {
            let outcome = outcome?;
            let run = outcome.run_id;
            let order = command.order;
            debug!("Fulfillment run {run} completed for order {order}");
            Ok(())
        }
        completed = crash_rx => {
            let step = completed.expect("the crash switch outlives the run");
            let order = command.order;
            warn!("Simulating a coordinator crash for order {order} after '{step}'. The run remains incomplete for recovery.");
            Err(SagaError::Crashed(step))
        }
    }
}

fn task_bytes(task: &FulfillmentTask) -> Vec<u8> {
    serde_json::to_vec(task).expect("fulfillment task serializes")
}

fn decode_panel(ctx: &StepContext<'_>) -> QuotePanel {
    ctx.outputs
        .get("quote")
        .and_then(|bytes| serde_json::from_slice(bytes).ok())
        .unwrap_or_default()
}

fn decode_booking(ctx: &StepContext<'_>) -> (String, String) {
    ctx.outputs
        .get("book")
        .and_then(|bytes| serde_json::from_slice::<Booking>(bytes).ok())
        .map(|booking| (booking.carrier, booking.booking))
        .unwrap_or_default()
}
