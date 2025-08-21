use std::{thread, time::Duration};

pub fn retry_with_delay_option_sync<F, T>(mut f: F, attempts: u8, sleep_ms: u64) -> Option<T>
where
    F: FnMut() -> Option<T>,
{
    for attempt in 1..=attempts {
        match f() {
            Some(result) => return Some(result),
            None => {
                if attempt == attempts {
                    return None;
                }
                thread::sleep(Duration::from_millis(sleep_ms));
            }
        }
    }
    unreachable!()
}
