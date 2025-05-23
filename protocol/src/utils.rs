use std::time::Duration;

use tokio::time;

pub async fn retry_with_delay_result<F, Fut, T, E>(mut f: F, attempts: u8, sleep: u64) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    for attempt in 1..=attempts {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if attempt == attempts {
                    return Err(e);
                }
                time::sleep(Duration::from_secs(sleep)).await;
            }
        }
    }
    unreachable!()
}

pub async fn retry_with_delay_option<F, Fut, T>(mut f: F, attempts: u8, sleep: u64) -> Option<T>
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
                time::sleep(Duration::from_secs(sleep)).await;
            }
        }
    }
    unreachable!()
}