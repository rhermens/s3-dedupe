use std::{error::Error, fs, io};

use chrono::DateTime;
use clap::Parser;
use glob::{glob, Pattern};
use serde_json::Value;
use url::Url;

use ext::DedupExtract;

use crate::ext::Dotnotation;

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

    // Log file
    #[arg(short, long, default_value = "output.log")]
    log: String,

    #[arg(short, long)]
    sort_by: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    simple_logging::log_to_file(args.log, log::LevelFilter::Info).unwrap();

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
                    _ => log::warn!("Invalid json object"),
                };

                log::debug!("Downloaded file..");
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
                log::debug!("Reading: {:?}", file);
                let file_content = fs::read_to_string(file.unwrap()).unwrap();
                let parsed = serde_json::from_str::<Value>(&file_content);

                match parsed {
                    Ok(Value::Array(objs)) => results.append(objs.to_owned().as_mut()),
                    Ok(Value::Object(_)) => results.push(parsed.unwrap()),
                    _ => log::warn!("Invalid json object"),
                };
            }
        }
        _ => panic!("Invalid url"),
    }

    log::info!("Testing {} objects", results.len());
    log::info!("Deduplicating by key: {}", args.identifier);
    let mut deduped = results.dedup_extract_by_dotnotation(&args.identifier);
    log::info!("Found {} duplicates", results.len());
    log::info!("Deduped length: {}", deduped.len());

    if args.sort_by.is_some() {
        let sort_key = args.sort_by.as_ref().expect("Invalid sort key").as_str();

        deduped.sort_by(|a, b| {
            let a_value = a.get_by_dotnotation(&sort_key).expect("Invalid sort by").as_str().expect("Invalid sort by");
            let b_value = b.get_by_dotnotation(&sort_key).expect("Invalid sort by").as_str().expect("Invalid sort by");
            let a_date = DateTime::parse_from_rfc3339(a_value);
            let b_date = DateTime::parse_from_rfc3339(b_value);

            match a_date {
                Ok(date) => date.cmp(&b_date.unwrap()),
                Err(_) => a_value.cmp(b_value)
            }
        });
    }

    serde_json::to_writer(io::stdout(), &deduped).unwrap();
    Ok(())
}
