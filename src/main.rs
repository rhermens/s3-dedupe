use std::{error::Error, fs, io};

use clap::Parser;
use glob::{glob, Pattern};
use serde_json::Value;
use url::Url;

use crate::ext::DedupExtract;

mod ext;
mod s3;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    // S3 Bucket url s3://bucket-name/path/to/objects or file path file:///path/to/objects
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
    let url = Url::parse(&args.url).expect("Invalid url");

    let mut results: Vec<Value> = vec![];
    match url.scheme() {
        "s3" => {
            for handle in s3::create_file_download_handles(&url, &pattern).await {
                let parsed = serde_json::from_slice::<Value>(
                    handle
                        .await?
                        .unwrap()
                        .body
                        .collect()
                        .await
                        .unwrap()
                        .to_vec()
                        .as_slice(),
                );

                match parsed {
                    Ok(Value::Array(objs)) => results.append(objs.to_owned().as_mut()),
                    Ok(Value::Object(_)) => results.push(parsed.unwrap()),
                    _ => log::warn!("Invalid json object")
                };

                log::info!("Downloaded file..");
            }
        }
        "file" => {
            let pattern = &format!(
                "{}/{}",
                url.path().to_string().trim_end_matches("/"),
                &args.pattern
            );
            log::info!("Reading files from path: {}", &pattern);

            for file in glob(&pattern).expect("Failed to read glob pattern") {
                let file_content = fs::read_to_string(file.unwrap()).unwrap();
                let parsed = serde_json::from_str::<Value>(&file_content);

                match parsed {
                    Ok(Value::Array(objs)) => results.append(objs.to_owned().as_mut()),
                    Ok(Value::Object(_)) => results.push(parsed.unwrap()),
                    _ => log::warn!("Invalid json object")
                };
            }
        }
        _ => panic!("Invalid url"),
    }

    log::info!("Testing {} objects", results.len());
    log::info!("Deduplicating by key: {}", args.identifier);
    let deduped = results.dedup_extract_by_dotnotation(&args.identifier);
    log::info!("Found {} duplicates", results.len());
    log::info!("Deduped length: {}", deduped.len());

    serde_json::to_writer(io::stdout(), &deduped).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use crate::ext::{DedupExtract, Dotnotation};

    #[test]
    fn split_duplicates_can_split_by_root_property() {
        let file_content = std::fs::read_to_string("./test/nested.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("id");

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn split_large_nested() {
        let file_content = std::fs::read_to_string("./test/bitcoin-unconfirmed.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("hash");

        assert_eq!(result.len(), 100);
    }

    #[test]
    fn split_duplicates_can_split_by_nested_property() {
        let file_content = std::fs::read_to_string("./test/nested.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("data.somekey");

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn keeps_data_structure() {
        let file_content = std::fs::read_to_string("./test/nested.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = serde_json::from_str::<Vec<Value>>(
            &serde_json::to_string(&objects.dedup_extract_by_dotnotation("id")).unwrap(),
        )
        .unwrap();

        result.iter().for_each(|obj| {
            assert!(obj.get("id").is_some());
            assert!(obj.get("amount").is_some());
            assert!(obj.get("data").is_some());
            assert!(obj.get_by_dotnotation("data.somekey").is_some());
        })
    }

    #[test]
    fn keeps_casing() {
        let file_content = std::fs::read_to_string("./test/casing.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = serde_json::from_str::<Vec<Value>>(
            &serde_json::to_string(&objects.dedup_extract_by_dotnotation("id")).unwrap(),
        )
        .unwrap();

        result.iter().for_each(|obj| {
            assert!(obj.get("camelCase").is_some());
            assert!(obj.get("snake_case").is_some());
        })
    }
}
