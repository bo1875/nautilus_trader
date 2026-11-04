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

//! Bounded latest capture state. Ordinary lifecycle channels are independent.

use arc_swap::ArcSwapOption;
use nautilus_core::time::get_atomic_clock_realtime;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) const WINDOW: Duration = Duration::from_secs(60);
pub(super) const LIMIT: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "python", pyo3::pyclass(from_py_object, frozen, get_all))]
pub struct OKXCaptureStatus {
    pub schema_version: u8,
    pub scope: String,
    pub scope_id: String,
    pub connection_generation: Option<u64>,
    pub ts_observed_ns: u64,
    pub window_start_ns: u64,
    pub window_duration_ns: u64,
    pub window_limit: u64,
    pub window_attempts: u64,
    pub total_attempts: u64,
    pub observations_dropped: u64,
    pub status_sequence: u64,
    pub state: String,
}

pub(super) type CaptureSlot = Arc<ArcSwapOption<CaptureSnapshot>>;

#[derive(Clone)]
pub(super) struct CaptureSnapshot {
    pub status: OKXCaptureStatus,
    pub window_start: Instant,
    pub sequence: Arc<AtomicU64>,
}

impl CaptureSnapshot {
    pub fn read(&self, now: Instant, date: u64) -> OKXCaptureStatus {
        if self.status.state == "closed" {
            return self.status.clone();
        }
        let mut status = self.status.clone();
        // Pure projection of the same monotonic window the producer uses. A
        // getter cannot grant additional publication capacity or erase totals.
        let windows = now.saturating_duration_since(self.window_start).as_secs() / WINDOW.as_secs();
        if windows > 0 {
            status.window_attempts = 0;
            status.window_start_ns = status
                .window_start_ns
                .saturating_add(windows.saturating_mul(WINDOW.as_nanos() as u64));
        }
        status.state = if status.window_attempts >= LIMIT as u64 {
            "exhausted"
        } else {
            "active"
        }
        .into();
        status.ts_observed_ns = date;
        status.status_sequence = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            })
            .unwrap()
            .saturating_add(1);
        status
    }
}

pub(super) fn read_capture(slot: &CaptureSlot) -> Option<OKXCaptureStatus> {
    slot.load_full().map(|snapshot| {
        snapshot.read(
            Instant::now(),
            get_atomic_clock_realtime().get_time_ns().as_u64(),
        )
    })
}

pub(super) fn close_capture(slot: &CaptureSlot, scope_id: Option<&str>) {
    // CAS prevents an old handler closing a newer scope or a late ordinary
    // snapshot replacing the frozen final status.
    slot.rcu(|current| {
        let Some(snapshot) = current else { return None };
        if snapshot.status.state == "closed"
            || scope_id.is_some_and(|id| id != snapshot.status.scope_id)
        {
            return current.clone();
        }
        let mut closed = snapshot.as_ref().clone();
        closed.status = snapshot.read(
            Instant::now(),
            get_atomic_clock_realtime().get_time_ns().as_u64(),
        );
        closed.status.state = "closed".into();
        Some(Arc::new(closed))
    });
}
