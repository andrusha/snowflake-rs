use std::fs::Metadata;
use std::path::Path;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::limit::LimitStore;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tokio::task;

use crate::responses::{AwsPutGetStageInfo, PutGetExecResponse, PutGetStageInfo};
use crate::SnowflakeApiError;

pub async fn put(resp: PutGetExecResponse) -> Result<(), SnowflakeApiError> {
    match resp.data.stage_info {
        PutGetStageInfo::Aws(info) => {
            put_to_s3(
                resp.data.src_locations,
                info,
                resp.data.parallel,
                resp.data.threshold,
            )
            .await
        }
        PutGetStageInfo::Azure(_) => Err(SnowflakeApiError::Unimplemented(
            "PUT local file requests for Azure".to_string(),
        )),
        PutGetStageInfo::Gcs(_) => Err(SnowflakeApiError::Unimplemented(
            "PUT local file requests for GCS".to_string(),
        )),
    }
}

async fn put_to_s3(
    src_locations: Vec<String>,
    info: AwsPutGetStageInfo,
    max_parallel_uploads: usize,
    max_file_size_threshold: i64,
) -> Result<(), SnowflakeApiError> {
    // These constants are based on the snowflake website
    let (bucket_name, bucket_path) = info
        .location
        .split_once('/')
        .ok_or(SnowflakeApiError::InvalidBucketPath(info.location.clone()))?;

    let s3 = AmazonS3Builder::new()
        .with_region(info.region)
        .with_bucket_name(bucket_name)
        .with_access_key_id(info.creds.aws_key_id)
        .with_secret_access_key(info.creds.aws_secret_key)
        .with_token(info.creds.aws_token)
        .build()?;

    let files = list_files(src_locations, max_file_size_threshold).await?;

    for src_path in files.large_files {
        put_file(&s3, &src_path, bucket_path).await?;
    }

    let limit_store = LimitStore::new(s3, max_parallel_uploads);
    put_files_par(files.small_files, bucket_path, limit_store).await?;

    Ok(())
}

/// Sorts upload files by whether they are larger or smaller than the threshold
struct SizedFiles {
    small_files: Vec<String>,
    large_files: Vec<String>,
}

// todo: security vulnerability, external system tells you which local files to upload
async fn list_files(
    src_locations: Vec<String>,
    threshold: i64,
) -> Result<SizedFiles, SnowflakeApiError> {
    let paths = task::spawn_blocking(move || traverse_globs(src_locations)).await??;
    let paths_meta = fetch_metadata(paths).await?;

    let threshold = u64::try_from(threshold).unwrap_or(0);
    let mut small_files = vec![];
    let mut large_files = vec![];
    for pm in paths_meta {
        if pm.meta.len() > threshold {
            large_files.push(pm.path);
        } else {
            small_files.push(pm.path);
        }
    }

    Ok(SizedFiles {
        small_files,
        large_files,
    })
}

fn traverse_globs(globs: Vec<String>) -> Result<Vec<String>, SnowflakeApiError> {
    let mut res = vec![];
    for g in globs {
        for path in glob::glob(&g)? {
            if let Some(p) = path?.to_str() {
                res.push(p.to_owned());
            }
        }
    }

    Ok(res)
}

struct PathMeta {
    path: String,
    meta: Metadata,
}

async fn fetch_metadata(paths: Vec<String>) -> Result<Vec<PathMeta>, SnowflakeApiError> {
    let metadata = FuturesUnordered::new();
    for path in paths {
        let task = async move {
            let meta = tokio::fs::metadata(&path).await?;
            Ok(PathMeta { path, meta })
        };
        metadata.push(task);
    }

    metadata.try_collect().await
}

async fn put_file<T: ObjectStore>(
    store: &T,
    src_path: &str,
    bucket_path: &str,
) -> Result<(), SnowflakeApiError> {
    let filename = Path::new(&src_path)
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or(SnowflakeApiError::InvalidLocalPath(src_path.to_owned()))?;

    let dest_path = format!("{bucket_path}{filename}");
    let dest_path = object_store::path::Path::parse(dest_path)?;
    let src_path = object_store::path::Path::parse(src_path)?;
    let fs = LocalFileSystem::new().get(&src_path).await?;

    store.put(&dest_path, fs.bytes().await?).await?;

    Ok::<(), SnowflakeApiError>(())
}

/// This function uploads files in parallel, useful for files below the threshold
/// One potential issue is that file size could be changed between when the file is
/// checked and when it is uploaded
async fn put_files_par<T: ObjectStore>(
    files: Vec<String>,
    bucket_path: &str,
    limit_store: LimitStore<T>,
) -> Result<(), SnowflakeApiError> {
    let limit_store = Arc::new(limit_store);
    let mut tasks = task::JoinSet::new();
    for src_path in files {
        let bucket_path = bucket_path.to_owned();
        let limit_store = Arc::clone(&limit_store);
        tasks.spawn(async move { put_file(limit_store.as_ref(), &src_path, &bucket_path).await });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
