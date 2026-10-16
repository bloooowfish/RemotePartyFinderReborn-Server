use mongodb::Collection;

use crate::listing_container::ListingContainer;
use crate::listings_snapshot::{
    ListingSnapshotRevisionStateDoc, ListingSourceStateDoc, MaterializedListingsSnapshotDoc,
};
use crate::player::Player;
use crate::web::State;

impl State {
    pub fn collection(&self) -> Collection<ListingContainer> {
        self.mongo.database("rpf").collection("listings")
    }

    pub fn players_collection(&self) -> Collection<Player> {
        self.mongo.database("rpf").collection("players")
    }

    pub fn parse_collection(&self) -> Collection<crate::mongo::ParseCacheDoc> {
        self.mongo.database("rpf").collection("parses")
    }

    pub fn fflogs_job_failure_state_collection(&self) -> Collection<mongodb::bson::Document> {
        self.mongo
            .database("rpf")
            .collection("fflogs_job_failure_state")
    }

    pub fn listings_snapshot_collection(&self) -> Collection<MaterializedListingsSnapshotDoc> {
        self.mongo
            .database("rpf")
            .collection(&self.listings_snapshot_collection_name)
    }

    pub fn listing_source_state_collection(&self) -> Collection<ListingSourceStateDoc> {
        self.mongo
            .database("rpf")
            .collection(&self.listing_source_state_collection_name)
    }

    pub fn listing_snapshot_revision_state_collection(
        &self,
    ) -> Collection<ListingSnapshotRevisionStateDoc> {
        self.mongo
            .database("rpf")
            .collection(&self.listing_snapshot_revision_state_collection_name)
    }
}
