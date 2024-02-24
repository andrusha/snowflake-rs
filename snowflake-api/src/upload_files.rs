use crate::SnowflakeApiError;
use object_store::aws::AmazonS3;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Sorts upload files by whether they are larger or smaller than the threshold
pub struct UploadFiles {
    pub small_files: Vec<String>,
    pub large_files: Vec<String>,
    threshold: u64,
}

impl UploadFiles {
    fn new(threshold: i64) -> Self {
        Self {
            small_files: Vec::new(),
            large_files: Vec::new(),
            // If the threshold is negative set it to 0, which means that all files will be considered large
            threshold: u64::try_from(threshold).unwrap_or(0),
        }
    }

    /// Pushes a file to the appropriate vector based on its size
    fn push_file(&mut self, file: String) {
        let metadata = fs::metadata(&file).unwrap();
        if metadata.len() > self.threshold {
            self.large_files.push(file);
        } else {
            self.small_files.push(file);
        }
    }
}

// todo: security vulnerability, external system tells you which local files to upload
// For right now this function ignores errors, im not sure if that is the best approach
pub fn get_files(src_locations: &[String], threshold: i64) -> UploadFiles {
    let mut upload_files = UploadFiles::new(threshold);
    let locations = src_locations
        .iter()
        .filter_map(|src_path| glob::glob(src_path).ok())
        .flat_map(|paths| paths.filter_map(Result::ok));

    for path in locations.filter_map(|path| path.to_str().map(String::from)) {
        upload_files.push_file(path);
    }
    upload_files
}

struct FileUpload {
    dest_path: object_store::path::Path,
    bytes: bytes::Bytes,
}

impl FileUpload {
    async fn from_source(src_path: &str, bucket_path: &str) -> Result<Self, SnowflakeApiError> {
        let filename = Path::new(&src_path)
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or(SnowflakeApiError::InvalidLocalPath(src_path.to_owned()))?;

        let dest_path_str = format!("{bucket_path}{filename}");
        let dest_path = object_store::path::Path::parse(dest_path_str)?;
        let src_path = object_store::path::Path::parse(src_path)?;
        let fs = LocalFileSystem::new().get(&src_path).await?;
        let bytes = fs.bytes().await?;
        Ok(Self { dest_path, bytes })
    }
}

/// This function uploads files in parallel, useful for files below the threshold
/// One potential issue is that file size could be changed between when the file is
/// checked and when it is uploaded
pub async fn upload_files_parallel(
    files: Vec<String>,
    bucket_path: &str,
    s3_arc: &Arc<AmazonS3>,
    max_parallel: usize,
) -> Result<(), SnowflakeApiError> {
    let semaphore = Arc::new(Semaphore::new(max_parallel));
    let mut set: JoinSet<Result<(), SnowflakeApiError>> = JoinSet::new();
    for src_path in files {
        let arc1 = Arc::clone(s3_arc);
        let bucket_path = bucket_path.to_owned();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        set.spawn(async move {
            let to_upload = FileUpload::from_source(&src_path, &bucket_path).await?;
            arc1.put(&to_upload.dest_path, to_upload.bytes).await?;
            // Drop the permit, so more tasks can be created.
            drop(permit);
            Ok(())
        });
    }
    while let Some(result) = set.join_next().await {
        result??;
    }
    Ok(())
}

/// This function uploads files sequentially, useful for files above the threshold
pub async fn upload_files_sequential(
    files: Vec<String>,
    bucket_path: &str,
    s3_arc: &Arc<AmazonS3>,
) -> Result<(), SnowflakeApiError> {
    let arc1 = Arc::clone(s3_arc);
    for src_path in files {
        let to_upload = FileUpload::from_source(&src_path, bucket_path).await?;
        arc1.put(&to_upload.dest_path, to_upload.bytes).await?;
    }
    Ok(())
}
