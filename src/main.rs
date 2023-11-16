use std::{error::Error, env, collections::HashMap};

use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Deserialize, Serialize)]
struct Transaction {
    id: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bucket_url = env::args().nth(1).expect("No bucket url given");
    let bucket_url_parsed = Url::parse(&bucket_url).expect("Invalid bucket url");
    let bucket = bucket_url_parsed.host_str().expect("Invalid bucket url");

    println!("Bucket: {:?}", bucket_url);

    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    let objects = client.list_objects()
        .set_bucket(Some(bucket.to_string()))
        .set_prefix(Some(bucket_url_parsed.path().trim_start_matches("/").to_string()))
        .send()
        .await?;

    let handles = objects.contents()
        .into_iter()
        .filter(|obj| obj.key().is_some_and(|key| key.rsplit_once("/").unwrap().1.starts_with("transactions")))
        .map(|object| {
             client.get_object()
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(object.key().expect("No key").to_string()))
                .send()
        })
        .map(|f| tokio::spawn(f))
        .collect::<Vec<_>>();

    let mut results: Vec<Transaction> = vec![];
    for handle in handles {
        let mut transactions = serde_json::from_slice::<Vec<Transaction>>(
            handle.await?
                .unwrap()
                .body
                .collect()
                .await
                .unwrap()
                .to_vec()
                .as_slice()
        ).unwrap();

        results.append(&mut transactions);
    }

    results.sort_unstable_by_key(|t| t.id.clone());
    results.dedup_by_key(|t| t.id.clone());

    println!("Results: {:?}", results);

    Ok(())
}
