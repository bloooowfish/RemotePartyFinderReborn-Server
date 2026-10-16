use anyhow::{Context, Result};
use futures_util::StreamExt;
use mongodb::{Collection, IndexModel, bson::Document, options::IndexOptions};
use std::time::Duration;

use crate::web::State;

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct PlayerDuplicateGroup {
    #[serde(rename = "_id")]
    pub(crate) content_id: i64,
    pub(crate) winner_id: mongodb::bson::oid::ObjectId,
    pub(crate) duplicate_ids: Vec<mongodb::bson::oid::ObjectId>,
    pub(crate) count: i32,
}

pub(crate) fn players_content_id_unique_index_model() -> IndexModel {
    IndexModel::builder()
        .keys(mongodb::bson::doc! {
            "content_id": 1,
        })
        .options(IndexOptions::builder().unique(true).build())
        .build()
}

pub(crate) fn parse_cache_ttl_index_model(ttl_hours: i64) -> IndexModel {
    IndexModel::builder()
        .keys(mongodb::bson::doc! {
            "updated_at": 1,
        })
        .options(
            IndexOptions::builder()
                .expire_after(Duration::from_secs((ttl_hours.max(1) as u64) * 60 * 60))
                .build(),
        )
        .build()
}

pub(crate) fn listing_ttl_index_model() -> IndexModel {
    IndexModel::builder()
        .keys(mongodb::bson::doc! {
            "expires_at": 1,
        })
        .options(
            IndexOptions::builder()
                .expire_after(Duration::from_secs(0))
                .build(),
        )
        .build()
}

pub(crate) fn is_namespace_not_found_error(error: &mongodb::error::Error) -> bool {
    matches!(
        &*error.kind,
        mongodb::error::ErrorKind::Command(command_error) if command_error.code == 26
    )
}

fn is_index_options_conflict(error: &mongodb::error::Error) -> bool {
    matches!(
        &*error.kind,
        mongodb::error::ErrorKind::Command(command_error) if command_error.code == 85
    )
}

pub(crate) fn players_content_id_dedupe_pipeline() -> Vec<mongodb::bson::Document> {
    vec![
        mongodb::bson::doc! {
            "$match": {
                "content_id": { "$exists": true },
            }
        },
        mongodb::bson::doc! {
            "$sort": {
                "content_id": 1,
                "identity_observed_at": -1,
                "last_seen": -1,
                "seen_count": -1,
                "_id": 1,
            }
        },
        mongodb::bson::doc! {
            "$group": {
                "_id": "$content_id",
                "winner_id": { "$first": "$_id" },
                "duplicate_ids": { "$push": "$_id" },
                "count": { "$sum": 1 },
            }
        },
        mongodb::bson::doc! {
            "$match": {
                "count": { "$gt": 1 },
            }
        },
    ]
}

pub(crate) fn duplicate_ids_to_remove(
    group: &PlayerDuplicateGroup,
) -> Vec<mongodb::bson::oid::ObjectId> {
    group
        .duplicate_ids
        .iter()
        .copied()
        .filter(|id| *id != group.winner_id)
        .collect()
}

impl State {
    pub(super) async fn ensure_indexes(&self) -> Result<()> {
        self.collection()
            .create_index(
                IndexModel::builder()
                    .keys(mongodb::bson::doc! {
                        "listing.id": 1,
                        "listing.last_server_restart": 1,
                        "listing.created_world": 1,
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .context("could not create unique index")?;

        self.ensure_listing_ttl_index().await?;

        self.parse_collection()
            .create_index(
                IndexModel::builder()
                    .keys(mongodb::bson::doc! {
                        "content_id": 1,
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .context("could not create parse index")?;

        self.ensure_parse_cache_ttl_index().await?;

        self.fflogs_job_failure_state_collection()
            .create_index(
                IndexModel::builder()
                    .keys(mongodb::bson::doc! {
                        "processor_kind": 1,
                        "final_for_processor": 1,
                        "content_id": 1,
                        "zone_id": 1,
                        "difficulty_id": 1,
                    })
                    .build(),
                None,
            )
            .await
            .context("could not create FFLogs job failure state index")?;

        self.ensure_players_content_id_unique_index().await?;

        Ok(())
    }

    async fn ensure_listing_ttl_index(&self) -> Result<()> {
        self.backfill_listing_expires_at().await?;
        self.drop_legacy_listing_updated_at_ttl_index().await?;

        let collection = self.collection().clone_with_type::<Document>();
        let index_model = listing_ttl_index_model();

        if let Err(error) = collection.create_index(index_model.clone(), None).await {
            if is_index_options_conflict(&error) {
                tracing::debug!(
                    "Index option conflict detected for listings.expires_at. Dropping old index and recreating..."
                );
                collection
                    .drop_index("expires_at_1", None)
                    .await
                    .context("could not drop conflicting listings expires_at index")?;

                collection
                    .create_index(index_model, None)
                    .await
                    .context("could not create listings expires_at ttl index after restart")?;
                tracing::info!("Index 'listings.expires_at' recreated with TTL options.");
            } else {
                return Err(error).context("could not create listings expires_at ttl index");
            }
        }

        Ok(())
    }

    async fn backfill_listing_expires_at(&self) -> Result<()> {
        self.collection()
            .clone_with_type::<Document>()
            .update_many(
                mongodb::bson::doc! {
                    "expires_at": { "$exists": false },
                },
                vec![mongodb::bson::doc! {
                    "$set": {
                        "expires_at": {
                            "$dateAdd": {
                                "startDate": "$updated_at",
                                "unit": "second",
                                "amount": { "$ifNull": ["$listing.seconds_remaining", 0] },
                            },
                        },
                    },
                }],
                None,
            )
            .await
            .context("could not backfill listing expires_at")?;

        Ok(())
    }

    async fn drop_legacy_listing_updated_at_ttl_index(&self) -> Result<()> {
        let collection = self.collection().clone_with_type::<Document>();
        let mut existing_indexes = match collection.list_indexes(None).await {
            Ok(indexes) => indexes,
            Err(error) if is_namespace_not_found_error(&error) => return Ok(()),
            Err(error) => return Err(error).context("could not list listings indexes"),
        };

        while let Some(index) = existing_indexes.next().await {
            let index = index.context("could not read listings index")?;
            let is_updated_at_index = index
                .keys
                .get_i32("updated_at")
                .ok()
                .is_some_and(|value| value == 1)
                && index.keys.len() == 1;
            let is_ttl_index = index
                .options
                .as_ref()
                .and_then(|options| options.expire_after)
                .is_some();

            if !is_updated_at_index || !is_ttl_index {
                continue;
            }

            let name = index
                .options
                .as_ref()
                .and_then(|options| options.name.as_deref())
                .unwrap_or("updated_at_1");
            collection
                .drop_index(name, None)
                .await
                .with_context(|| format!("could not drop legacy listings ttl index '{name}'"))?;
        }

        Ok(())
    }

    async fn ensure_parse_cache_ttl_index(&self) -> Result<()> {
        self.backfill_parse_cache_updated_at().await?;

        let index_model = parse_cache_ttl_index_model(self.parse_cache_ttl_hours);

        if let Err(error) = self
            .parse_collection()
            .create_index(index_model.clone(), None)
            .await
        {
            if is_index_options_conflict(&error) {
                tracing::debug!(
                    ttl_hours = self.parse_cache_ttl_hours,
                    "Index option conflict detected for parses.updated_at. Dropping old index and recreating..."
                );
                self.parse_collection()
                    .drop_index("updated_at_1", None)
                    .await
                    .context("could not drop conflicting parses updated_at index")?;

                self.parse_collection()
                    .create_index(index_model, None)
                    .await
                    .context("could not create parses.updated_at ttl index after restart")?;
                tracing::info!("Index 'parses.updated_at' recreated with new TTL options.");
            } else {
                return Err(error).context("could not create parses.updated_at ttl index");
            }
        }

        Ok(())
    }

    async fn backfill_parse_cache_updated_at(&self) -> Result<()> {
        self.parse_collection()
            .clone_with_type::<Document>()
            .update_many(
                mongodb::bson::doc! {
                    "updated_at": { "$exists": false },
                },
                vec![mongodb::bson::doc! {
                    "$set": { "updated_at": "$$NOW" },
                }],
                None,
            )
            .await
            .context("could not backfill parse cache updated_at")?;

        Ok(())
    }

    async fn ensure_players_content_id_unique_index(&self) -> Result<()> {
        let collection = self
            .players_collection()
            .clone_with_type::<mongodb::bson::Document>();
        self.dedupe_players_by_content_id(collection.clone())
            .await?;

        let mut has_unique_content_id_index = false;
        let mut existing_indexes = match collection.list_indexes(None).await {
            Ok(indexes) => indexes,
            Err(error) if is_namespace_not_found_error(&error) => {
                collection
                    .create_index(players_content_id_unique_index_model(), None)
                    .await
                    .context("could not create unique players content_id index")?;
                return Ok(());
            }
            Err(error) => return Err(error).context("could not list players indexes"),
        };
        while let Some(index) = existing_indexes.next().await {
            let index = index.context("could not read players index")?;
            let is_content_id_index = index
                .keys
                .get_i32("content_id")
                .ok()
                .is_some_and(|value| value == 1)
                && index.keys.len() == 1;
            if !is_content_id_index {
                continue;
            }

            let is_unique = index
                .options
                .as_ref()
                .and_then(|options| options.unique)
                .unwrap_or(false);
            let name = index
                .options
                .as_ref()
                .and_then(|options| options.name.as_deref())
                .unwrap_or("content_id_1");

            if is_unique {
                has_unique_content_id_index = true;
                continue;
            }

            collection.drop_index(name, None).await.with_context(|| {
                format!("could not drop non-unique players content_id index '{name}'")
            })?;
        }

        if !has_unique_content_id_index {
            collection
                .create_index(players_content_id_unique_index_model(), None)
                .await
                .context("could not create unique players content_id index")?;
        }

        Ok(())
    }

    async fn dedupe_players_by_content_id(
        &self,
        collection: Collection<mongodb::bson::Document>,
    ) -> Result<()> {
        let mut duplicates = collection
            .aggregate(players_content_id_dedupe_pipeline(), None)
            .await
            .context("could not scan duplicate players by content_id")?;

        while let Some(result) = duplicates.next().await {
            let Some(duplicate) = result
                .ok()
                .and_then(|doc| mongodb::bson::from_document::<PlayerDuplicateGroup>(doc).ok())
            else {
                continue;
            };
            let ids_to_remove = duplicate_ids_to_remove(&duplicate);
            if ids_to_remove.is_empty() {
                continue;
            }

            collection
                .delete_many(
                    mongodb::bson::doc! {
                        "_id": { "$in": ids_to_remove },
                    },
                    None,
                )
                .await
                .with_context(|| {
                    format!(
                        "could not delete duplicate player rows for content_id {}",
                        duplicate.content_id
                    )
                })?;
        }

        Ok(())
    }
}
