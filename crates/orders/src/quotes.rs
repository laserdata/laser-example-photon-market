use photon_shared::domain::ContractVersion;
use photon_shared::domain::shipping::Quote;
use serde::{Deserialize, Serialize};

const MAX_PRICE_CENTS: i64 = 1_000_000;
const MAX_ETA_DAYS: u32 = 30;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AttributedQuote {
    pub carrier: String,
    pub quote: Quote,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct QuotePanel {
    pub quotes: Vec<AttributedQuote>,
}

pub fn verify_panel(bytes: &[u8]) -> bool {
    serde_json::from_slice::<QuotePanel>(bytes)
        .map(|panel| panel.winner().is_some())
        .unwrap_or(false)
}

impl QuotePanel {
    pub fn winner(&self) -> Option<&AttributedQuote> {
        self.ranked().into_iter().next()
    }

    pub fn runner_up(&self) -> Option<&AttributedQuote> {
        self.ranked().into_iter().nth(1)
    }

    pub fn ranked(&self) -> Vec<&AttributedQuote> {
        let mut sane: Vec<&AttributedQuote> = self
            .quotes
            .iter()
            .filter(|attributed| is_sane(&attributed.quote))
            .collect();
        sane.sort_by_key(|attributed| attributed.quote.price_cents);
        sane
    }
}

fn is_sane(quote: &Quote) -> bool {
    quote.version == ContractVersion::CURRENT
        && quote.price_cents > 0
        && quote.price_cents <= MAX_PRICE_CENTS
        && (1..=MAX_ETA_DAYS).contains(&quote.eta_days)
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_shared::domain::ContractVersion;

    #[test]
    fn given_a_panel_with_an_absurd_and_a_negative_quote_when_ranked_then_should_pick_the_cheapest_sane_one()
     {
        let panel = QuotePanel {
            quotes: vec![
                quote("borealis", -500, 2),
                quote("hermes", 4_200, 3),
                quote("atlas", 3_900, 5),
                quote("borealis", 9_999_999, 1),
            ],
        };
        assert_eq!(
            panel.winner().map(|attributed| attributed.carrier.as_str()),
            Some("atlas")
        );
        assert_eq!(
            panel
                .runner_up()
                .map(|attributed| attributed.carrier.as_str()),
            Some("hermes")
        );
    }

    #[test]
    fn given_a_panel_with_no_sane_quote_when_ranked_then_should_have_no_winner() {
        let panel = QuotePanel {
            quotes: vec![quote("borealis", -1, 2), quote("borealis", 0, 0)],
        };
        assert!(panel.winner().is_none());
    }

    fn quote(carrier: &str, price_cents: i64, eta_days: u32) -> AttributedQuote {
        AttributedQuote {
            carrier: carrier.to_owned(),
            quote: Quote {
                version: ContractVersion::CURRENT,
                carrier: carrier.to_owned(),
                order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                    .parse()
                    .expect("order id parses"),
                price_cents,
                eta_days,
            },
        }
    }
}
