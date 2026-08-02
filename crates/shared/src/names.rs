use laser_sdk::iggy::prelude::Identifier;
use laser_sdk::prelude::{AgentId, AgentTopic, Laser, Topic};
use std::sync::LazyLock;
use strum::{Display, EnumIter, EnumString};

pub const STREAM: &str = "photon";
pub const TRAFFIC_STREAM: &str = "photon-traffic";
pub const FRAUD_GRAPH: &str = "fraud";

static SUPPORT_TICKETS_ID: LazyLock<Identifier> = LazyLock::new(|| {
    Identifier::named(&BusinessTopic::SupportTickets.to_string())
        .expect("support tickets is a valid topic identifier")
});

pub fn support_tickets_agent_topic() -> AgentTopic<'static> {
    AgentTopic::Custom(&SUPPORT_TICKETS_ID)
}

#[derive(Clone, Copy, Debug, Display, EnumIter, EnumString, Eq, PartialEq)]
pub enum BusinessTopic {
    #[strum(serialize = "catalog.commands")]
    CatalogCommands,
    #[strum(serialize = "order.commands")]
    OrderCommands,
    #[strum(serialize = "order.events")]
    OrderEvents,
    #[strum(serialize = "risk.events")]
    RiskEvents,
    #[strum(serialize = "shop.events")]
    ShopEvents,
    #[strum(serialize = "support.tickets")]
    SupportTickets,
}

impl BusinessTopic {
    pub fn stream(&self) -> &'static str {
        match self {
            BusinessTopic::ShopEvents => TRAFFIC_STREAM,
            _ => STREAM,
        }
    }

    pub fn topic(&self, laser: &Laser) -> Topic {
        laser.stream(self.stream()).topic(self.to_string())
    }
}

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum Index {
    ShopEvents,
    Orders,
    Tickets,
    RiskCases,
}

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum Skill {
    ScreenOrder,
    QuoteShipment,
    BookShipment,
}

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum KvSpace {
    Inventory,
    Refunds,
    Bookings,
    Charges,
}

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum MemorySpace {
    Support,
    Risk,
}

#[derive(Clone, Copy, Debug, Display, EnumIter, EnumString, Eq, Hash, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum AppAgent {
    Operator,
    Storefront,
    Orders,
    Fulfillment,
    Hermes,
    Atlas,
    Risk,
    Support,
    Reviewer,
    Insights,
    Borealis,
    Mallory,
}

impl AppAgent {
    pub fn id(&self) -> AgentId {
        self.to_string()
            .parse()
            .expect("app agent name is a valid AgentId")
    }
}

#[derive(Clone, Copy, Debug, Default, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum AdversaryMode {
    #[default]
    Off,
    Scripted,
    Random,
}

#[derive(Clone, Copy, Debug, Default, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum LlmProvider {
    #[default]
    Mock,
    Anthropic,
    #[strum(serialize = "openai")]
    OpenAi,
}

#[derive(Clone, Copy, Debug, Default, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum GovernorMode {
    #[default]
    Observe,
    Enforce,
}

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum WorkflowStep {
    Charge,
    Quote,
    Book,
    Dispatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn given_a_business_topic_when_rendered_and_parsed_then_should_round_trip() {
        for topic in BusinessTopic::iter() {
            assert_eq!(
                topic
                    .to_string()
                    .parse::<BusinessTopic>()
                    .expect("topic parses"),
                topic
            );
        }
        assert_eq!(BusinessTopic::OrderEvents.to_string(), "order.events");
    }

    #[test]
    fn given_shop_events_when_asked_for_its_stream_then_should_be_the_traffic_stream() {
        assert_eq!(BusinessTopic::ShopEvents.stream(), TRAFFIC_STREAM);
        assert_eq!(BusinessTopic::OrderEvents.stream(), STREAM);
    }

    #[test]
    fn given_an_app_agent_when_rendered_and_parsed_then_should_round_trip() {
        for agent in AppAgent::iter() {
            assert_eq!(
                agent.to_string().parse::<AppAgent>().expect("agent parses"),
                agent
            );
        }
        assert_eq!(AppAgent::Hermes.id().to_string(), "hermes");
    }

    #[test]
    fn given_knob_modes_when_rendered_then_should_match_their_wire_spelling() {
        assert_eq!(LlmProvider::OpenAi.to_string(), "openai");
        assert_eq!(
            "openai".parse::<LlmProvider>().expect("provider parses"),
            LlmProvider::OpenAi
        );
        assert_eq!(GovernorMode::Enforce.to_string(), "enforce");
    }
}
