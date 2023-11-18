use std::{collections::HashMap, error::Error, io};

use clap::Parser;
use ext::Dotnotation;
use glob::Pattern;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::ext::DedupExtract;

mod ext;
mod s3;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Obj {
    #[serde(flatten)]
    data: HashMap<String, Value>,
}
impl Dotnotation for Obj {
    fn get_by_dotnotation(&self, key: &str) -> Option<&Value> {
        let keys = key.split(".").collect::<Vec<&str>>();

        match keys.len() {
            1 => self.data.get(&keys.first()?.to_string()),
            _ => self
                .data
                .get(&keys.first()?.to_string())?
                .get_by_dotnotation(&keys[1..].join(".")),
        }
    }
}

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    // S3 Bucket url s3://bucket-name/path/to/objects
    #[arg()]
    url: String,

    // Filename pattern
    #[arg(short, long, default_value = "*.json")]
    pattern: String,

    // Unique object key
    #[arg(short, long, default_value = "id")]
    identifier: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    simple_logging::log_to_file("output.log", log::LevelFilter::Info).unwrap();

    if !args.pattern.ends_with(".json") {
        panic!("Invalid pattern, must end with .json");
    }

    let pattern = Pattern::new(&args.pattern).expect("Invalid pattern");
    let bucket_url_parsed = Url::parse(&args.url).expect("Invalid bucket url");

    let mut results: Vec<Obj> = vec![];
    match bucket_url_parsed.scheme() {
        "s3" => {
            for handle in s3::create_file_download_handles(&bucket_url_parsed, &pattern).await
            {
                let mut objs = serde_json::from_slice::<Vec<Obj>>(
                    handle
                        .await?
                        .unwrap()
                        .body
                        .collect()
                        .await
                        .unwrap()
                        .to_vec()
                        .as_slice(),
                )
                .unwrap();

                results.append(&mut objs);
            }
        }
        _ => panic!("Invalid bucket url"),
    }

    log::info!("Deduplicating by key: {}", &args.identifier);
    let deduped = results.dedup_extract_by_dotnotation(&args.identifier);
    log::info!("Found {} duplicates", results.len());
    log::info!("Deduped length: {}", deduped.len());

    serde_json::to_writer(io::stdout(), &deduped).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{ext::DedupExtract, Obj};

    #[test]
    fn split_duplicates_can_split_by_root_property() {
        simple_logging::log_to_file("output.log", log::LevelFilter::Info).unwrap();
        let file_content = std::fs::read_to_string("./test/transactions_1.json").unwrap();

        let mut objects = serde_json::from_str::<Vec<Obj>>(&file_content).unwrap();

        let result = objects.dedup_extract_by_dotnotation("id");

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn split_duplicates_can_split_by_nested_property() {
        simple_logging::log_to_file("output.log", log::LevelFilter::Info).unwrap();
        let file_content = std::fs::read_to_string("./test/transactions_1.json").unwrap();

        let mut objects = serde_json::from_str::<Vec<Obj>>(&file_content).unwrap();

        let result = objects.dedup_extract_by_dotnotation("data.somekey");

        assert_eq!(result.len(), 1);
    }
}
