use std::sync::atomic::Ordering as AtomicOrdering;

use crate::web::State;

impl State {
    pub fn notify_listings_changed(&self, changed_count: usize) {
        if changed_count > 0 {
            self.listings_revision_pending
                .store(true, AtomicOrdering::Relaxed);
            self.listings_revision_notify.notify_one();
        }
    }

    pub fn current_listings_revision(&self) -> u64 {
        self.listings_revision.load(AtomicOrdering::Relaxed)
    }
}
