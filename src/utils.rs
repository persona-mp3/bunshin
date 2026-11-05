use crossbeam::channel::{Receiver, Sender, select};
use rand::RngExt;
use std::time::{self, Duration, Instant};

pub fn start_timer(
    timeout: time::Duration,
    reset_timer_recv: Receiver<()>,
    timeout_elapsed_signal: Sender<()>,
) {
    let mut deadline = Instant::now() + timeout;
    loop {
        let remain = deadline.saturating_duration_since(time::Instant::now());
        select! {
            recv(reset_timer_recv) -> _s => {
                deadline = Instant::now() + timeout;
                println!("resetting timer...");
            },

            default(remain) => {
                match timeout_elapsed_signal.send_timeout((), time::Duration::from_secs(3)) {
                    Ok(_) => {},
                    Err(err) => {
                        println!("error occured: {}", err);
                    }
                };
                return;
            }
        }
    }
}

pub fn random_timeout() -> time::Duration {
    let mut rng = rand::rng();
    let timeout = rng.random_range(5..10);
    Duration::from_secs(timeout)
}
