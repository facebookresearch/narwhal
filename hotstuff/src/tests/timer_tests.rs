use super::*;

#[tokio::test]
async fn schedule() {
    let timer = Timer::new(100);
    let now = Instant::now();
    timer.await;
    assert!(now.elapsed().as_millis() > 95);
}
