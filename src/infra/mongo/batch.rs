use std::collections::{HashMap, HashSet};

use mongodb::{
    Collection,
    bson::{Bson, Document, doc},
    options::WriteConcern,
};

pub const MONGO_BATCH_CHUNK_SIZE: usize = 100;
const BATCH_DUPLICATE_KEY_MAX_RETRIES: usize = 3;

#[derive(Debug, Clone)]
pub enum BatchUpdateOp {
    Document(Document),
    Pipeline(Vec<Document>),
}

#[derive(Debug, Clone)]
pub struct BatchUpdateItem {
    pub filter: Document,
    pub update: BatchUpdateOp,
    pub upsert: bool,
    pub multi: bool,
}

impl BatchUpdateItem {
    pub fn upsert_pipeline(filter: Document, pipeline: Vec<Document>) -> Self {
        Self {
            filter,
            update: BatchUpdateOp::Pipeline(pipeline),
            upsert: true,
            multi: false,
        }
    }

    pub fn upsert_doc(filter: Document, update: Document) -> Self {
        Self {
            filter,
            update: BatchUpdateOp::Document(update),
            upsert: true,
            multi: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteError {
    pub item_index: usize,
    pub code: i32,
    pub errmsg: String,
}

#[derive(Debug, Clone, Default)]
pub struct BatchUpdateSummary {
    pub modified_count: usize,
    pub upserted_count: usize,
    pub succeeded_indices: HashSet<usize>,
    pub failures: Vec<BatchWriteError>,
    pub write_concern_error: Option<String>,
}

impl BatchUpdateSummary {
    pub fn changed_count(&self) -> usize {
        self.modified_count + self.upserted_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedChunkUpdateResponse {
    pub modified_count: usize,
    pub upserted_count: usize,
    pub write_errors: Vec<(usize, i32, String)>,
    pub write_concern_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedChunkDeleteResponse {
    pub deleted_count: usize,
    pub write_errors: Vec<(usize, i32, String)>,
    pub write_concern_error: Option<String>,
}

fn write_concern_to_bson(write_concern: Option<&WriteConcern>) -> Option<Document> {
    let wc = write_concern?;
    let doc = mongodb::bson::to_document(wc).ok()?;
    if doc.is_empty() { None } else { Some(doc) }
}

fn bson_number_as_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(v) => Some(i64::from(*v)),
        Bson::Int64(v) => Some(*v),
        Bson::Double(v) if v.is_finite() => Some(*v as i64),
        _ => None,
    }
}

fn check_command_ok(response: &Document) -> anyhow::Result<()> {
    if let Some(ok_val) = response.get("ok").and_then(bson_number_as_i64) {
        if ok_val == 0 {
            let errmsg = response
                .get_str("errmsg")
                .unwrap_or("MongoDB command returned ok=0");
            anyhow::bail!("MongoDB batch command failed: {errmsg}");
        }
    }
    Ok(())
}

fn parse_write_errors(response: &Document) -> Vec<(usize, i32, String)> {
    let Ok(errors) = response.get_array("writeErrors") else {
        return Vec::new();
    };

    errors
        .iter()
        .filter_map(|entry| entry.as_document())
        .filter_map(|doc| {
            let index = doc.get("index").and_then(bson_number_as_i64)?;
            let idx = usize::try_from(index).ok()?;
            let code = doc
                .get("code")
                .and_then(bson_number_as_i64)
                .unwrap_or(-1) as i32;
            let errmsg = doc
                .get_str("errmsg")
                .unwrap_or("unknown write error")
                .to_string();
            Some((idx, code, errmsg))
        })
        .collect()
}

fn parse_write_concern_error(response: &Document) -> Option<String> {
    let Ok(doc) = response.get_document("writeConcernError") else {
        return None;
    };
    let code = doc.get("code").and_then(bson_number_as_i64).unwrap_or(-1);
    let errmsg = doc.get_str("errmsg").unwrap_or("write concern error");
    Some(format!("code {code}: {errmsg}"))
}

pub fn build_batch_update_entry(item: &BatchUpdateItem) -> Document {
    let update_bson = match &item.update {
        BatchUpdateOp::Document(update_doc) => Bson::Document(update_doc.clone()),
        BatchUpdateOp::Pipeline(stages) => {
            Bson::Array(stages.iter().cloned().map(Bson::Document).collect())
        }
    };

    let mut entry = doc! {
        "q": item.filter.clone(),
        "u": update_bson,
        "upsert": item.upsert,
    };
    if item.multi {
        entry.insert("multi", true);
    }
    entry
}

pub fn build_batch_update_command(
    collection_name: &str,
    items: &[&BatchUpdateItem],
    write_concern: Option<&WriteConcern>,
) -> Document {
    let updates = items
        .iter()
        .map(|item| Bson::Document(build_batch_update_entry(item)))
        .collect::<Vec<_>>();

    let mut cmd = doc! {
        "update": collection_name,
        "updates": updates,
        "ordered": false,
    };
    if let Some(wc_doc) = write_concern_to_bson(write_concern) {
        cmd.insert("writeConcern", wc_doc);
    }
    cmd
}

pub fn build_batch_delete_command(
    collection_name: &str,
    filters: &[Document],
    limit: i32,
    write_concern: Option<&WriteConcern>,
) -> Document {
    let deletes = filters
        .iter()
        .map(|filter| {
            Bson::Document(doc! {
                "q": filter.clone(),
                "limit": limit,
            })
        })
        .collect::<Vec<_>>();

    let mut cmd = doc! {
        "delete": collection_name,
        "deletes": deletes,
        "ordered": false,
    };
    if let Some(wc_doc) = write_concern_to_bson(write_concern) {
        cmd.insert("writeConcern", wc_doc);
    }
    cmd
}

pub fn parse_batch_update_response(
    response: &Document,
) -> anyhow::Result<ParsedChunkUpdateResponse> {
    check_command_ok(response)?;

    let modified_count = response
        .get("nModified")
        .and_then(bson_number_as_i64)
        .unwrap_or(0)
        .max(0) as usize;
    let upserted_count = response
        .get_array("upserted")
        .map(|arr| arr.len())
        .unwrap_or(0);
    let write_errors = parse_write_errors(response);
    let write_concern_error = parse_write_concern_error(response);

    Ok(ParsedChunkUpdateResponse {
        modified_count,
        upserted_count,
        write_errors,
        write_concern_error,
    })
}

pub fn parse_batch_delete_response(
    response: &Document,
) -> anyhow::Result<ParsedChunkDeleteResponse> {
    check_command_ok(response)?;

    let deleted_count = response
        .get("n")
        .and_then(bson_number_as_i64)
        .unwrap_or(0)
        .max(0) as usize;
    let write_errors = parse_write_errors(response);
    let write_concern_error = parse_write_concern_error(response);

    Ok(ParsedChunkDeleteResponse {
        deleted_count,
        write_errors,
        write_concern_error,
    })
}

pub async fn run_batched_updates<T>(
    collection: &Collection<T>,
    items: &[BatchUpdateItem],
    retry_duplicate_keys: bool,
) -> anyhow::Result<BatchUpdateSummary> {
    let mut summary = BatchUpdateSummary::default();
    if items.is_empty() {
        return Ok(summary);
    }

    let db = collection.client().database(&collection.namespace().db);
    let collection_name = collection.name();
    let write_concern = collection.write_concern();
    let max_attempts = if retry_duplicate_keys {
        BATCH_DUPLICATE_KEY_MAX_RETRIES
    } else {
        0
    };

    let mut pending: Vec<(usize, &BatchUpdateItem)> = items.iter().enumerate().collect();

    for attempt in 0..=max_attempts {
        let mut next_wave = Vec::new();

        for chunk in pending.chunks(MONGO_BATCH_CHUNK_SIZE) {
            let chunk_items = chunk.iter().map(|(_, item)| *item).collect::<Vec<_>>();
            let cmd = build_batch_update_command(collection_name, &chunk_items, write_concern);
            let response = db.run_command(cmd, None).await?;
            let parsed = parse_batch_update_response(&response)?;

            summary.modified_count += parsed.modified_count;
            summary.upserted_count += parsed.upserted_count;
            if let Some(wc_err) = parsed.write_concern_error {
                if summary.write_concern_error.is_none() {
                    summary.write_concern_error = Some(wc_err);
                }
            }

            let mut failed_by_pos: HashMap<usize, (i32, String)> = HashMap::new();
            for (pos, code, errmsg) in parsed.write_errors {
                failed_by_pos.entry(pos).or_insert((code, errmsg));
            }

            for (pos, &(orig_idx, item)) in chunk.iter().enumerate() {
                match failed_by_pos.remove(&pos) {
                    None => {
                        summary.succeeded_indices.insert(orig_idx);
                    }
                    Some((code, errmsg)) => {
                        if retry_duplicate_keys && code == 11_000 && attempt < max_attempts {
                            next_wave.push((orig_idx, item));
                        } else {
                            summary.failures.push(BatchWriteError {
                                item_index: orig_idx,
                                code,
                                errmsg,
                            });
                        }
                    }
                }
            }
        }

        if next_wave.is_empty() {
            break;
        }
        pending = next_wave;
    }

    Ok(summary)
}

pub async fn run_batched_deletes<T>(
    collection: &Collection<T>,
    filters: &[Document],
    limit: i32,
) -> anyhow::Result<usize> {
    if filters.is_empty() {
        return Ok(0);
    }

    let db = collection.client().database(&collection.namespace().db);
    let collection_name = collection.name();
    let write_concern = collection.write_concern();
    let mut total_deleted = 0usize;

    for chunk in filters.chunks(MONGO_BATCH_CHUNK_SIZE) {
        let cmd = build_batch_delete_command(collection_name, chunk, limit, write_concern);
        let response = db.run_command(cmd, None).await?;
        let parsed = parse_batch_delete_response(&response)?;
        total_deleted += parsed.deleted_count;

        if let Some((pos, code, errmsg)) = parsed.write_errors.first() {
            anyhow::bail!(
                "MongoDB batch delete failed at chunk index {pos} (code {code}): {errmsg}"
            );
        }
        if let Some(wc_err) = parsed.write_concern_error {
            anyhow::bail!("MongoDB batch delete writeConcernError: {wc_err}");
        }
    }

    Ok(total_deleted)
}
