use anyhow::Context;
use chrono::Utc;
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    results::UpdateResult,
    Collection,
};

use crate::listing::PartyFinderListing;
use crate::listing_container::{ListingContainer, QueriedListing};

use super::is_valid_world_id;
fn strip_detail_managed_listing_fields(listing_doc: &mut Document) {
    listing_doc.remove("member_content_ids");
    listing_doc.remove("member_jobs");
    listing_doc.remove("detail_slot_flags");
    listing_doc.remove("leader_content_id");
}

fn listing_upsert_filter(listing: &PartyFinderListing) -> Document {
    doc! {
        "listing.id": listing.id,
        "listing.last_server_restart": listing.last_server_restart,
        "listing.created_world": listing.created_world as u32,
    }
}

fn listing_expires_at_expression(seconds_remaining: i64) -> Document {
    doc! {
        "$dateAdd": {
            "startDate": "$$NOW",
            "unit": "second",
            "amount": seconds_remaining.max(0),
        }
    }
}

fn listing_upsert_update(mut listing_doc: Document, seconds_remaining: i64) -> Vec<Document> {
    strip_detail_managed_listing_fields(&mut listing_doc);
    vec![doc! {
        "$set": {
            "updated_at": "$$NOW",
            "created_at": { "$ifNull": ["$created_at", "$$NOW"] },
            "expires_at": listing_expires_at_expression(seconds_remaining),
            "listing": {
                "$mergeObjects": [
                    "$listing",
                    listing_doc,
                ]
            },
        }
    }]
}

fn current_listings_pipeline(now: chrono::DateTime<Utc>) -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "expires_at": { "$gte": mongodb::bson::DateTime::from_chrono(now) },
            }
        },
        doc! {
            "$match": {
                "listing.search_area": { "$bitsAllClear": 2 },
            }
        },
        doc! {
            "$set": {
                "time_left": {
                    "$divide": [
                        {
                            "$subtract": [
                                "$expires_at",
                                "$$NOW",
                            ]
                        },
                        1000,
                    ]
                },
                "updated_minute": {
                    "$dateTrunc": {
                        "date": "$updated_at",
                        "unit": "minute",
                        "binSize": 5,
                    },
                },
            }
        },
        doc! {
            "$match": {
                "time_left": { "$gte": 0 },
            }
        },
    ]
}

pub async fn get_current_listings(
    collection: Collection<ListingContainer>,
) -> anyhow::Result<Vec<QueriedListing>> {
    let cursor = collection
        .aggregate(current_listings_pipeline(Utc::now()), None)
        .await?;

    let listings = cursor
        .filter_map(async |result| {
            result
                .ok()
                .and_then(|raw| mongodb::bson::from_document(raw).ok())
        })
        .collect::<Vec<_>>()
        .await;

    Ok(listings)
}

pub async fn insert_listing(
    collection: Collection<ListingContainer>,
    listing: &PartyFinderListing,
) -> anyhow::Result<UpdateResult> {
    if !is_valid_world_id(listing.created_world)
        || !is_valid_world_id(listing.home_world)
        || !is_valid_world_id(listing.current_world)
    {
        anyhow::bail!("invalid listing");
    }

    let listing_doc = mongodb::bson::to_document(listing)?;
    let filter = listing_upsert_filter(listing);
    let update = listing_upsert_update(listing_doc, listing.seconds_remaining as i64);
    let options = UpdateOptions::builder().upsert(true).build();

    collection
        .update_one(filter, update, options)
        .await
        .context("could not insert record")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use mongodb::bson::Bson;

    #[test]
    fn listing_upsert_sets_expires_at_from_seconds_remaining() {
        let update = listing_upsert_update(doc! {}, 1800);
        let set = update[0]
            .get_document("$set")
            .expect("listing upsert must have a set stage");

        assert_eq!(
            set.get_document("expires_at"),
            Ok(&doc! {
                "$dateAdd": {
                    "startDate": "$$NOW",
                    "unit": "second",
                    "amount": 1800_i64,
                }
            })
        );
    }

    #[test]
    fn listing_upsert_update_preserves_false_high_end_telemetry_for_stale_clear() {
        let update = listing_upsert_update(
            doc! {
                "pf_category_high_end": false,
                "cfc_excel_high_end": false,
                "client_high_end_duty": false,
            },
            1800,
        );
        let set = update[0]
            .get_document("$set")
            .expect("listing upsert must have a set stage");
        let listing_stage = set
            .get_document("listing")
            .expect("listing upsert must merge listing data");
        let merge_objects = listing_stage
            .get_array("$mergeObjects")
            .expect("listing upsert must merge with the existing listing");
        let listing_doc = merge_objects[1]
            .as_document()
            .expect("second merge object should be the uploaded listing");

        assert_eq!(listing_doc.get_bool("pf_category_high_end"), Ok(false));
        assert_eq!(listing_doc.get_bool("cfc_excel_high_end"), Ok(false));
        assert_eq!(listing_doc.get_bool("client_high_end_duty"), Ok(false));
    }

    #[test]
    fn current_listings_pipeline_filters_by_expires_at() {
        let now = Utc::now();
        let pipeline = current_listings_pipeline(now);

        assert_eq!(
            pipeline[0].get_document("$match"),
            Ok(&doc! {
                "expires_at": { "$gte": mongodb::bson::DateTime::from_chrono(now) },
            })
        );

        let set = pipeline[2]
            .get_document("$set")
            .expect("current listings pipeline must compute fields");
        assert_eq!(
            set.get_document("time_left"),
            Ok(&doc! {
                "$divide": [
                    { "$subtract": ["$expires_at", "$$NOW"] },
                    1000,
                ],
            })
        );
    }

    #[test]
    fn current_listings_pipeline_does_not_drop_long_lived_recently_observed_listings() {
        let now = Utc::now();
        let one_hour_ago = now - TimeDelta::try_hours(1).unwrap();
        let pipeline = current_listings_pipeline(now);

        let first_match = pipeline[0]
            .get_document("$match")
            .expect("current listings pipeline must start with an expiry match");
        assert_eq!(first_match.get("updated_at"), None);
        assert_ne!(
            first_match.get("expires_at"),
            Some(&Bson::Document(doc! { "$gte": one_hour_ago }))
        );
    }
}
