use photon_shared::names::STREAM;
use photon_shared::testkit::TestIggy;
use photon_shared::topology;

#[tokio::test]
async fn given_a_fresh_iggy_when_topics_are_bootstrapped_then_should_stay_open_only() {
    let iggy = TestIggy::start().await;
    let laser = iggy
        .factory()
        .connect(STREAM)
        .await
        .expect("laser connects");

    topology::bootstrap_all(&laser)
        .await
        .expect("topics bootstrap");

    assert!(laser.capabilities().await.is_open_only());
}
