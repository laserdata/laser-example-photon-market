use laser_sdk::prelude::{Laser, LaserError};
use laser_sdk::query::SchemaSource;

pub const ORDER_EVENT_SCHEMA_NAME: &str = "photon_order_event";
pub const ORDER_EVENT_SCHEMA_VERSION: u32 = 1;

pub const ORDER_EVENT_JSON_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["v", "order", "customer", "sku", "quantity", "at_micros", "kind"],
  "properties": {
    "v": {"const": 1},
    "order": {"type": "string", "minLength": 1},
    "customer": {"type": "string", "minLength": 1},
    "sku": {"type": "string", "minLength": 1},
    "quantity": {"type": "integer", "minimum": 1},
    "at_micros": {"type": "integer", "minimum": 0},
    "kind": {
      "type": "object",
      "required": ["type"],
      "properties": {
        "type": {
          "enum": [
            "received", "inventory_reserved", "inventory_released", "accepted",
            "rejected", "charged", "charge_refunded", "shipment_booked",
            "booking_released", "shipped", "delivered", "refunded"
          ]
        },
        "reason": {"type": "string"},
        "amount_cents": {"type": "integer", "minimum": 0},
        "carrier": {"type": "string"},
        "booking": {"type": "string"},
        "tracking": {"type": "string"}
      },
      "additionalProperties": false
    }
  }
}"#;

pub async fn ensure_order_event_schema(laser: &Laser) -> Result<u32, LaserError> {
    let source = SchemaSource::JsonSchema {
        schema: ORDER_EVENT_JSON_SCHEMA.to_owned(),
    };
    if let Some(existing) = laser.schemas().list().await?.into_iter().find(|info| {
        !info.dropped
            && info.schema.name.as_deref() == Some(ORDER_EVENT_SCHEMA_NAME)
            && info.schema.version == Some(ORDER_EVENT_SCHEMA_VERSION)
            && info.schema.source == source
    }) {
        return Ok(existing.schema.id);
    }
    laser
        .schemas()
        .register(source)
        .name(ORDER_EVENT_SCHEMA_NAME)
        .version(ORDER_EVENT_SCHEMA_VERSION)
        .send()
        .await
}
