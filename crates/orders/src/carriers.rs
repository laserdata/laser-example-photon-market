use laser_sdk::prelude::{AgentCtx, AgentHandler, AgentMessage, LaserError};
use photon_shared::domain::shipping::{
    BookRequest, Booking, CarrierRequest, Quote, QuoteRequest, ReleaseRequest,
};
use photon_shared::domain::{ContractVersion, OrderId};
use photon_shared::names::AppAgent;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Carrier {
    pub carrier: AppAgent,
    base_price_cents: i64,
    eta_days: u32,
    bookings: Arc<Mutex<HashMap<OrderId, Booking>>>,
}

impl Carrier {
    pub fn hermes() -> Self {
        Self::new(AppAgent::Hermes, 3_900, 5)
    }

    pub fn atlas() -> Self {
        Self::new(AppAgent::Atlas, 4_600, 2)
    }

    fn new(carrier: AppAgent, base_price_cents: i64, eta_days: u32) -> Self {
        Self {
            carrier,
            base_price_cents,
            eta_days,
            bookings: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn quote(&self, request: &QuoteRequest) -> Quote {
        Quote {
            version: ContractVersion::CURRENT,
            carrier: self.carrier.to_string(),
            order: request.order,
            price_cents: self.base_price_cents + i64::from(request.quantity) * 250,
            eta_days: self.eta_days,
        }
    }

    // Idempotent: a redelivered booking for the same order returns the same
    // confirmation rather than booking twice.
    fn book(&self, request: &BookRequest) -> Booking {
        let mut bookings = self
            .bookings
            .lock()
            .expect("carrier bookings lock is not poisoned");
        bookings
            .entry(request.order)
            .or_insert_with(|| Booking {
                version: ContractVersion::CURRENT,
                carrier: self.carrier.to_string(),
                order: request.order,
                booking: format!("bk-{}-{}", self.carrier, request.order),
            })
            .clone()
    }

    // Idempotent: releasing an unknown or already-released booking acknowledges
    // the same way, so a redelivered compensation cannot fail.
    fn release(&self, request: &ReleaseRequest) -> Booking {
        let mut bookings = self
            .bookings
            .lock()
            .expect("carrier bookings lock is not poisoned");
        bookings.remove(&request.order).unwrap_or_else(|| Booking {
            version: ContractVersion::CURRENT,
            carrier: self.carrier.to_string(),
            order: request.order,
            booking: format!("bk-{}-{}", self.carrier, request.order),
        })
    }
}

impl AgentHandler for Carrier {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let request: CarrierRequest = serde_json::from_slice(message.body()).map_err(|error| {
            LaserError::Invalid(format!("undecodable carrier request: {error}"))
        })?;
        let version = match &request {
            CarrierRequest::Quote(request) => request.version,
            CarrierRequest::Book(request) => request.version,
            CarrierRequest::Release(request) => request.version,
        };
        version
            .ensure_current()
            .map_err(|error| LaserError::Invalid(error.to_string()))?;
        let reply = match request {
            CarrierRequest::Quote(quote) => serde_json::to_vec(&self.quote(&quote)),
            CarrierRequest::Book(book) => serde_json::to_vec(&self.book(&book)),
            CarrierRequest::Release(release) => serde_json::to_vec(&self.release(&release)),
        }
        .map_err(|error| LaserError::Invalid(format!("cannot encode carrier reply: {error}")))?;
        ctx.respond(reply).await
    }
}
