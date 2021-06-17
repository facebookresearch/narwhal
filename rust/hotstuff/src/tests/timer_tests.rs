// use super::*;

// #[tokio::test]
// async fn schedule() {
//     // Make a timer and give it enough time to boot.
//     let mut timer = Timer::new();
//     sleep(Duration::from_millis(50)).await;

//     // schedule a timer.
//     let id = 1;
//     timer.schedule(50, id).await;
//     match timer.notifier.recv().await {
//         Some(id) => assert_eq!(id, id),
//         _ => assert!(false),
//     }
// }

// #[tokio::test]
// async fn cancel() {
//     // Make a timer and give it enough time to boot.
//     let mut timer = Timer::new();
//     sleep(Duration::from_millis(50)).await;

//     // schedule two timers.
//     let id_1 = 1;
//     timer.schedule(50, id_1).await;
//     let id_2 = 2;
//     timer.schedule(100, id_2).await;

//     // Cancel timer 1.
//     timer.cancel(id_1).await;

//     // Ensure we receive timer 2 (and not timer 1).
//     match timer.notifier.recv().await {
//         Some(id) => assert_eq!(id, id_2),
//         _ => assert!(false),
//     }
// }
