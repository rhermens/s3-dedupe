use glob::Pattern;
use tokio::{task::JoinHandle, sync::OnceCell};
use aws_sdk_s3::{Client, operation::get_object::{GetObjectOutput, GetObjectError}, error::SdkError};
use url::Url;

static CLIENT: OnceCell<Client> = OnceCell::const_new();

pub async fn create_file_download_handles(bucket_url: &Url, pattern: &Pattern) -> Vec<JoinHandle<Result<GetObjectOutput, SdkError<GetObjectError>>>> {
    let client = CLIENT.get_or_init(|| async {
        Client::new(&aws_config::load_from_env().await)
    }).await;

    let bucket = bucket_url.host_str().expect("Invalid bucket url");
    log::info!("Listing objects in bucket url: {}", bucket_url);

    client.list_objects()
        .set_bucket(Some(bucket.to_string()))
        .set_prefix(Some(bucket_url.path().trim_start_matches("/").to_string()))
        .send()
        .await
        .expect("Failed to list contents")
        .contents()
        .into_iter()
        .filter_map(|obj| obj.key().map_or(None, |k| pattern.matches(k).then_some(k)))
        .map(|key| {
            log::debug!("Downloading object: {}", key);
            client.get_object()
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(key.to_string()))
                .send()
        })
        .map(|f| tokio::spawn(f))
        .collect::<Vec<_>>()
}

