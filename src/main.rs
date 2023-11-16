use std::{error::Error, collections::HashMap, io};

use aws_sdk_s3::{Client, operation::get_object::{GetObjectOutput, GetObjectError}, error::SdkError};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;
use url::Url;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Obj {
    #[serde(flatten)]
    data: HashMap<String, Value>
}

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    // S3 Bucket url s3://bucket-name/path/to/objects
    #[arg()]
    url: String,

    // Unique object key
    #[arg(short, long, default_value = "id")]
    identifier: String,

    // Filename matches substring
    #[arg(short, long)]
    substring: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    simple_logging::log_to_file("output.log", log::LevelFilter::Info).unwrap();

    let bucket_url_parsed = Url::parse(&args.url).expect("Invalid bucket url");
    if bucket_url_parsed.scheme() != "s3" {
        panic!("Invalid bucket url");
    }

    let mut results: Vec<Obj> = vec![];
    for handle in create_file_download_handles(&bucket_url_parsed, &args.substring, "json").await {
        let mut objs = serde_json::from_slice::<Vec<Obj>>(
            handle.await?
                .unwrap()
                .body
                .collect()
                .await
                .unwrap()
                .to_vec()
                .as_slice()
        ).unwrap();

        results.append(&mut objs);
    }

    let deduped = split_duplicates_by_key(&mut results, &args.identifier);
    log::info!("Found {} duplicates", results.len());
    log::info!("New length transactions: {}", deduped.len());

    serde_json::to_writer(io::stdout(), &deduped).unwrap();
    Ok(())
}

async fn create_file_download_handles(bucket_url: &Url, substring: &str, extension: &str) -> Vec<JoinHandle<Result<GetObjectOutput, SdkError<GetObjectError>>>> {
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    let bucket = bucket_url.host_str().expect("Invalid bucket url");
    log::info!("Listing objects in bucket url: {}", bucket_url);
    log::info!("Name containing: {}", substring);
    log::info!("Extension: {}", extension);

    let objects = client.list_objects()
        .set_bucket(Some(bucket.to_string()))
        .set_prefix(Some(bucket_url.path().trim_start_matches("/").to_string()))
        .send()
        .await
        .unwrap();

    let res = objects.contents()
        .into_iter()
        .filter(|obj| obj.key().is_some_and(|key| key.rsplit_once("/").unwrap().1.starts_with(substring)))
        .filter(|obj| obj.key().is_some_and(|key| key.rsplit_once("/").unwrap().1.ends_with(&format!(".{}", extension))))
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

fn split_duplicates_by_key(source: &mut Vec<Obj>, identifier: &str) -> Vec<Obj> {
    let mut dedupe = HashMap::new();
    log::info!("Deduplicating by key: {}", identifier);
    source.retain(|obj| {
        let retained = dedupe.insert(
            obj.data.get(identifier).unwrap().to_string(),
            obj.to_owned()
        );

        match retained {
            Some(retained) => {
                log::info!("Found duplicate: {:?}", retained);
                true
            },
            None => false
        }
    });

    dedupe.values().cloned().into_iter().collect::<Vec<Obj>>()
}
