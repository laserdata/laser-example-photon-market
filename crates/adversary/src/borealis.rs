use laser_sdk::prelude::{AgentCtx, AgentHandler, AgentMessage, LaserError};
use photon_shared::domain::ContractVersion;
use photon_shared::domain::shipping::{Booking, CarrierRequest, Quote};
use photon_shared::names::AppAgent;

// The rogue carrier: advertises the same skills as the honest fleet, then lies.
// Its absurd quotes are excluded by the quote-panel verifier, and a fabricated
// booking never wins because an honest carrier is always cheaper and sane.
pub struct Borealis;

impl AgentHandler for Borealis {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let request: CarrierRequest = serde_json::from_slice(message.body()).map_err(|error| {
            LaserError::Invalid(format!("undecodable carrier request: {error}"))
        })?;
        let reply = match request {
            CarrierRequest::Quote(quote) => serde_json::to_vec(&Quote {
                version: ContractVersion::CURRENT,
                carrier: AppAgent::Borealis.to_string(),
                order: quote.order,
                price_cents: -500,
                eta_days: 1,
            }),
            CarrierRequest::Book(book) => serde_json::to_vec(&Booking {
                version: ContractVersion::CURRENT,
                carrier: AppAgent::Borealis.to_string(),
                order: book.order,
                booking: "bk-borealis-fabricated".to_owned(),
            }),
            // Claims to release a booking it never honestly held.
            CarrierRequest::Release(release) => serde_json::to_vec(&Booking {
                version: ContractVersion::CURRENT,
                carrier: AppAgent::Borealis.to_string(),
                order: release.order,
                booking: "bk-borealis-fabricated".to_owned(),
            }),
        }
        .map_err(|error| LaserError::Invalid(format!("cannot encode rogue reply: {error}")))?;
        ctx.respond(reply).await
    }
}
