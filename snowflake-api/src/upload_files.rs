use crate::SnowflakeApiError;
use object_store::aws::AmazonS3;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Sorts upload files by whether they are larger or smaller than the threshold
pub struct UploadFiles {
    pub small_files: Vec<String>,
    pub large_files: Vec<String>,
    threshold: u64,
}

impl UploadFiles {
    fn new(threshold: i64) -> UploadFiles {
        UploadFiles {
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
pub fn get_files(
    src_locations: &[String],
    threshold: i64,
) -> Result<UploadFiles, SnowflakeApiError> {
    let mut files = UploadFiles::new(threshold);
    for src_path in src_locations {
        for entry in glob::glob(src_path).unwrap() {
            if let Ok(path) = entry {
                if let Some(item) = path.to_str() {
                    files.push_file(item.to_string());
                }
            }
        }
    }
    Ok(files)
}

/// This function uploads files in parallel, useful for files below the threshold
/// One potential issue is that file size could be changed between when the file is
/// checked and when it is uploaded
pub async fn upload_files_parallel(
    files: Vec<String>,
    bucket_path: &str,
    s3_arc: &Arc<AmazonS3>,
    max_parallel: usize
) -> Result<(), SnowflakeApiError> {
    let mut set: JoinSet<Result<(), SnowflakeApiError>> = JoinSet::new();
    for src_path in files {
        let arc1 = Arc::clone(&s3_arc);
        let bucket_path = bucket_path.to_owned();
        set.spawn(async move {
            let filename = Path::new(&src_path)
                .file_name()
                .and_then(|f| f.to_str())
                .ok_or(SnowflakeApiError::InvalidLocalPath(src_path.clone()))?;

            let dest_path_str = format!("{}{}", bucket_path.clone(), filename);
            let dest_path = object_store::path::Path::parse(dest_path_str)?;
            let src_path = object_store::path::Path::parse(src_path)?;
            let fs = LocalFileSystem::new().get(&src_path).await?;

            arc1.put(&dest_path, fs.bytes().await?).await?;
            Ok(())
        });
    }
    while let Some(res) = set.join_next().await {
        let result = res?;
        if let Err(e) = result {
            return Err(e);
        }
    }
    Ok(())
}

/// This function uploads files sequentially, useful for files above the threshold
pub async fn upload_files_sequential(
    files: Vec<String>,
    bucket_path: &str,
    s3_arc: &Arc<AmazonS3>,
) -> Result<(), SnowflakeApiError> {
    let arc1 = Arc::clone(&s3_arc);
    for src_path in files {
        let path = Path::new(&src_path);
        let filename = path
            .file_name()
            .ok_or(SnowflakeApiError::InvalidLocalPath(src_path.clone()))?;

        // fixme: unwrap
        let dest_path = format!("{}{}", bucket_path.clone(), filename.to_str().unwrap());
        let dest_path = object_store::path::Path::parse(dest_path)?;
        let src_path = object_store::path::Path::parse(src_path)?;
        let fs = LocalFileSystem::new().get(&src_path).await?;

        arc1.put(&dest_path, fs.bytes().await?).await?;
    }
    Ok(())
}
