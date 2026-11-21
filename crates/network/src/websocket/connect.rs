// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! TCP address selection for WebSocket connections.

use std::{future::Future, io, net::SocketAddr};
use tokio::net::{TcpStream, lookup_host};

pub(super) async fn connect_tcp(host: &str, port: u16) -> io::Result<TcpStream> {
    let addresses = lookup_host((host, port)).await?.collect();
    connect_addresses(addresses, |address| TcpStream::connect(address)).await
}

async fn connect_addresses<F, Fut>(addresses: Vec<SocketAddr>, mut dial: F) -> io::Result<TcpStream>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<TcpStream>>,
{
    // Interleave address families, preserving resolver order within each family.
    // A stalled first route must not consume the entire reconnect deadline.
    let first_v6 = addresses.first().is_some_and(SocketAddr::is_ipv6);
    let (mut v6, mut v4): (std::collections::VecDeque<_>, std::collections::VecDeque<_>) =
        addresses.into_iter().partition(SocketAddr::is_ipv6);
    let mut addresses = Vec::new();
    let mut prefer_v6 = first_v6;
    while !v6.is_empty() || !v4.is_empty() {
        let next = if prefer_v6 {
            v6.pop_front().or_else(|| v4.pop_front())
        } else {
            v4.pop_front().or_else(|| v6.pop_front())
        };
        addresses.extend(next);
        prefer_v6 = !prefer_v6;
    }
    let mut addresses = addresses.into_iter();
    let mut pending = futures_util::stream::FuturesUnordered::new();
    let mut last = io::Error::new(io::ErrorKind::InvalidInput, "No resolved socket addresses");
    let Some(first) = addresses.next() else {
        return Err(last);
    };
    pending.push(dial(first));
    let delay = std::time::Duration::from_millis(250);
    let next_attempt = tokio::time::sleep(delay);
    tokio::pin!(next_attempt);
    loop {
        tokio::select! {
            result = futures_util::StreamExt::next(&mut pending) => {
                match result {
                    Some(Ok(stream)) => return Ok(stream),
                    Some(Err(error)) => last = error,
                    None => return Err(last),
                }
                // An explicit failure need not wait for the stagger timer.
                if let Some(address) = addresses.next() {
                    pending.push(dial(address));
                    next_attempt.as_mut().reset(tokio::time::Instant::now() + delay);
                }
            }
            _ = &mut next_attempt, if addresses.len() > 0 => {
                pending.push(dial(addresses.next().expect("remaining address")));
                next_attempt.as_mut().reset(tokio::time::Instant::now() + delay);
            }
        }
        if pending.is_empty() {
            return Err(last);
        }
    }
    // No spawned tasks: return or caller cancellation drops every losing dial.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::net::TcpListener;

    struct PendingGuard(Arc<AtomicUsize>);
    impl Drop for PendingGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn stalled_address_does_not_hide_reachable_alternate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reachable = listener.local_addr().unwrap();
        let stalled: SocketAddr = "[::1]:1".parse().unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            connect_addresses(vec![stalled, reachable], |address| {
                let dropped = dropped.clone();
                async move {
                    if address == stalled {
                        let _guard = PendingGuard(dropped);
                        std::future::pending::<()>().await;
                    }
                    TcpStream::connect(address).await
                }
            }),
        )
        .await;
        assert!(
            result.is_ok(),
            "stalled first address consumed the overall deadline"
        );
        let stream = result.unwrap().unwrap();
        assert_eq!(stream.peer_addr().unwrap(), reachable);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "losing dial was not dropped"
        );
        let _accepted = listener.accept().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_connector_drops_pending_dials() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let result = tokio::time::timeout(
            Duration::from_millis(20),
            connect_addresses(vec!["[::1]:1".parse().unwrap()], |_| {
                let dropped = dropped.clone();
                async move {
                    let _guard = PendingGuard(dropped);
                    std::future::pending::<io::Result<TcpStream>>().await
                }
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn immediate_errors_try_alternate_without_delay() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            connect_addresses(
                vec!["[::1]:1".parse().unwrap(), "127.0.0.1:1".parse().unwrap()],
                |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err(io::Error::from(io::ErrorKind::ConnectionRefused)))
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn empty_resolution_does_not_dial() {
        let result = connect_addresses(vec![], |_| {
            panic!("empty resolution attempted a dial");
            #[allow(unreachable_code)]
            std::future::ready(Err(io::Error::from(io::ErrorKind::ConnectionRefused)))
        })
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn fast_first_success_does_not_start_alternate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let result = connect_addresses(vec![address, "[::1]:1".parse().unwrap()], |address| {
            calls.fetch_add(1, Ordering::SeqCst);
            TcpStream::connect(address)
        })
        .await
        .unwrap();
        assert_eq!(result.peer_addr().unwrap(), address);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn alternate_family_precedes_second_stalled_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reachable = listener.local_addr().unwrap();
        let first: SocketAddr = "[::1]:1".parse().unwrap();
        let second: SocketAddr = "[::1]:2".parse().unwrap();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let stream = tokio::time::timeout(
            Duration::from_secs(2),
            connect_addresses(vec![first, second, reachable], |address| {
                calls.lock().unwrap().push(address);
                async move {
                    if address.is_ipv6() {
                        std::future::pending::<()>().await;
                    }
                    TcpStream::connect(address).await
                }
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stream.peer_addr().unwrap(), reachable);
        assert_eq!(*calls.lock().unwrap(), vec![first, reachable]);
    }

    #[tokio::test]
    async fn cancelling_race_drops_both_pending_dials() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            connect_addresses(
                vec!["[::1]:1".parse().unwrap(), "127.0.0.1:1".parse().unwrap()],
                |_| {
                    let dropped = dropped.clone();
                    async move {
                        let _guard = PendingGuard(dropped);
                        std::future::pending::<io::Result<TcpStream>>().await
                    }
                },
            ),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}
