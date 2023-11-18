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

    let objects = client.list_objects()
        .set_bucket(Some(bucket.to_string()))
        .set_prefix(Some(bucket_url.path().trim_start_matches("/").to_string()))
        .send()
        .await
        .unwrap();

    let res = objects.contents()
        .into_iter()
        .map(|obj| {
            log::info!("Found object: {}", obj.key().expect("No key"));
            obj
        })
        .filter(|obj| pattern.matches(obj.key().expect("No key")))
        .map(|object| {
            log::info!("Downloading object: {}", object.key().expect("No key"));
            client.get_object()
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(object.key().expect("No key").to_string()))
                .send()
        })
        .map(|f| tokio::spawn(f))
        .collect::<Vec<_>>();
    res
}
