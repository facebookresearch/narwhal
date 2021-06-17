use super::*;

use std::error::Error;
use tokio::task;

use rocksdb::DB;
use std::str;

// Tokio testing reactor loop (single thread)
#[tokio::test]
async fn test_storage() -> Result<(), Box<dyn Error>> {
    // This spawns a new thread, even in the single threaded runtime!
    let _res = task::spawn_blocking(move || {
        // do some compute-heavy work or call synchronous code

        // NB: db is automatically closed at end of lifetime
        let db = DB::open_default(".test_storage").unwrap();
        match db.put(b"my key", b"my value") {
            Err(e) => return Err(e),
            _ => (),
        };
        match db.get(b"my key") {
            Ok(Some(value)) => println!("retrieved value {}", str::from_utf8(&value).unwrap()),
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {}", e),
        }
        db.delete(b"my key").unwrap();
        Ok(())
    })
    .await?;
    tokio::task::yield_now().await;
    Ok(())
}

#[tokio::test]
async fn test_own_store() -> Result<(), Box<dyn Error>> {
    let mut my_store = Store::new(".test_storage_own".to_string()).await;
    let res = my_store
        .write("Hello".as_bytes().to_vec(), "World".as_bytes().to_vec())
        .await;
    assert_eq!((), res.unwrap());

    let res = my_store.read("Unknown".as_bytes().to_vec()).await;
    assert_eq!(None, res.unwrap());

    let res = my_store.read("Hello".as_bytes().to_vec()).await;
    assert_eq!(Some("World".as_bytes().to_vec()), res.unwrap());

    // Notify read returns imediately if the value is in the store
    let res = my_store.notify_read("Hello".as_bytes().to_vec()).await;
    assert_eq!("World".as_bytes().to_vec(), res.unwrap());

    let res = my_store.delete("Hello".as_bytes().to_vec()).await;
    assert_eq!((), res.unwrap());

    let res = my_store.read("Hello".as_bytes().to_vec()).await;
    assert_eq!(None, res.unwrap());

    let mut store_copy = my_store.clone();
    let join_handle = tokio::spawn(async move {
        store_copy
            .notify_read("Notify_test".as_bytes().to_vec())
            .await
    });

    let _ = my_store
        .write(
            "Notify_test".as_bytes().to_vec(),
            "TEST".as_bytes().to_vec(),
        )
        .await;

    assert_eq!(
        join_handle.await.unwrap().unwrap(),
        "TEST".as_bytes().to_vec()
    );

    Ok(())
}
