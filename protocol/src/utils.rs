use std::time::Duration;

use tokio::time;

pub async fn retry_with_delay_option<F, Fut, T>(mut f: F, attempts: u8, sleep_ms: u64) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    for attempt in 1..=attempts {
        match f().await {
            Some(result) => return Some(result),
            None => {
                if attempt == attempts {
                    return None;
                }
                time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        }
    }
    unreachable!()
}