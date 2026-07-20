use photon_shared::domain::shop::ShopEventKind;
use photon_shared::domain::{CustomerId, Money, OrderId, SessionId, Sku};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ulid::Ulid;

pub(crate) const CATALOG: &[(&str, u64)] = &[
    ("sku-photon-phone", 79_900),
    ("sku-photon-laptop", 149_900),
    ("sku-photon-buds", 12_900),
    ("sku-photon-watch", 39_900),
    ("sku-photon-charger", 2_900),
];

// Small reused pools so distinct customers share devices, addresses, and cards,
// which is what lets the fraud graph find rings later.
const DEVICES: &[&str] = &[
    "device-aurora",
    "device-borealis",
    "device-comet",
    "device-dawn",
];
const ADDRESSES: &[&str] = &["addr-north", "addr-south", "addr-east", "addr-west"];
const CARDS: &[&str] = &["card-1111", "card-2222", "card-3333", "card-4444"];
const CUSTOMERS: u32 = 12;

pub struct Checkout {
    pub order: OrderId,
    pub sku: Sku,
    pub quantity: u32,
    pub unit_price: Money,
    pub device: String,
    pub ship_to: String,
    pub card_fingerprint: String,
}

pub struct Step {
    pub kind: ShopEventKind,
    pub sku: Sku,
    pub price: Money,
}

pub struct Session {
    pub session: SessionId,
    pub customer: CustomerId,
    pub steps: Vec<Step>,
    pub checkout: Option<Checkout>,
}

pub struct ShopperWalk {
    rng: StdRng,
    tick: u64,
    epoch: u64,
}

impl ShopperWalk {
    /// `seed` drives every behavioral choice, so a fixed seed and epoch replay
    /// the identical walk. `epoch` offsets the minted identities: a fresh run
    /// against a log carrying earlier runs mints new order ids instead of
    /// replaying old ones into idempotent no-ops.
    pub fn new(seed: u64, epoch: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            tick: 0,
            epoch,
        }
    }

    pub fn next_session(&mut self) -> Session {
        self.tick += 1;
        let session = SessionId::new(Ulid::from_parts(self.epoch + self.tick, self.rng.random()));
        let customer = format!("cust-{:02}", self.rng.random_range(0..CUSTOMERS))
            .parse()
            .expect("customer id is non-empty");

        let browses = self.rng.random_range(1..=3);
        let mut steps = Vec::new();
        let mut last = self.pick_sku();
        for _ in 0..browses {
            last = self.pick_sku();
            steps.push(Step {
                kind: ShopEventKind::Browse,
                sku: last.0.clone(),
                price: last.1,
            });
        }

        let carted = self.rng.random_bool(0.6);
        if carted {
            steps.push(Step {
                kind: ShopEventKind::Cart,
                sku: last.0.clone(),
                price: last.1,
            });
        }

        let checkout = if carted && self.rng.random_bool(0.55) {
            let quantity = self.rng.random_range(1..=3);
            steps.push(Step {
                kind: ShopEventKind::Checkout,
                sku: last.0.clone(),
                price: last.1,
            });
            let order = OrderId::new(Ulid::from_parts(self.epoch + self.tick, self.rng.random()));
            Some(Checkout {
                order,
                sku: last.0.clone(),
                quantity,
                unit_price: last.1,
                device: self.pick(DEVICES),
                ship_to: self.pick(ADDRESSES),
                card_fingerprint: self.pick(CARDS),
            })
        } else {
            steps.push(Step {
                kind: ShopEventKind::Abandon,
                sku: last.0.clone(),
                price: last.1,
            });
            None
        };

        Session {
            session,
            customer,
            steps,
            checkout,
        }
    }

    fn pick_sku(&mut self) -> (Sku, Money) {
        let (sku, cents) = CATALOG[self.rng.random_range(0..CATALOG.len())];
        (sku.parse().expect("catalog sku is non-empty"), Money(cents))
    }

    fn pick(&mut self, pool: &[&str]) -> String {
        pool[self.rng.random_range(0..pool.len())].to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkout_ratio(seed: u64, count: usize) -> f64 {
        let mut walk = ShopperWalk::new(seed, 0);
        let checkouts = (0..count)
            .filter(|_| walk.next_session().checkout.is_some())
            .count();
        checkouts as f64 / count as f64
    }

    #[test]
    fn given_the_same_seed_when_walked_then_should_be_reproducible() {
        let mut left = ShopperWalk::new(7, 0);
        let mut right = ShopperWalk::new(7, 0);
        for _ in 0..50 {
            let (a, b) = (left.next_session(), right.next_session());
            assert_eq!(a.session, b.session);
            assert_eq!(a.customer, b.customer);
            assert_eq!(a.checkout.is_some(), b.checkout.is_some());
        }
    }

    #[test]
    fn given_a_long_walk_when_measured_then_checkout_ratio_stays_within_bounds() {
        let ratio = checkout_ratio(20260710, 2000);
        assert!(
            (0.20..0.45).contains(&ratio),
            "checkout ratio {ratio} out of bounds"
        );
    }

    #[test]
    fn given_a_checkout_when_produced_then_should_draw_from_the_shared_pools() {
        let mut walk = ShopperWalk::new(1, 0);
        for _ in 0..200 {
            if let Some(checkout) = walk.next_session().checkout {
                assert!(DEVICES.contains(&checkout.device.as_str()));
                assert!(CARDS.contains(&checkout.card_fingerprint.as_str()));
            }
        }
    }
}
