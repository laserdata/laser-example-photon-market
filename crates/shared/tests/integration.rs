use laser_sdk::iggy::prelude::{Identifier, StreamClient};
use photon_shared::names::{BusinessTopic, STREAM};
use photon_shared::testkit::TestIggy;
use photon_shared::topology;
use strum::IntoEnumIterator;

#[tokio::test]
async fn given_a_fresh_iggy_when_topics_are_bootstrapped_then_should_stay_open_only() {
    let iggy = TestIggy::start().await;
    let laser = iggy
        .factory()
        .connect(STREAM)
        .await
        .expect("laser connects");

    let stream_id = Identifier::named(STREAM).expect("stream identifier");
    laser
        .client()
        .get_stream(&stream_id)
        .await
        .expect("get stream before bootstrap");
    laser
        .client()
        .create_stream(STREAM)
        .await
        .expect("create stream before bootstrap");
    for topic in BusinessTopic::iter() {
        topology::ensure_owned(&laser, [topic])
            .await
            .unwrap_or_else(|error| panic!("{topic} bootstrap: {error:?}"));
    }
    topology::ensure_agent_topics(&laser)
        .await
        .expect("agent topics bootstrap");

    assert!(laser.capabilities().await.is_open_only());
}
