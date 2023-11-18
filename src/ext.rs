use std::collections::HashMap;

use serde_json::Value;

pub trait Dotnotation: Clone {
    fn get_by_dotnotation(&self, key: &str) -> Option<&Value>;
}

impl Dotnotation for Value {
    fn get_by_dotnotation(&self, key: &str) -> Option<&Value> {
        log::info!("Getting by dotnotation: {}", key);
        let keys = key.split(".").collect::<Vec<&str>>();

        let result = keys.iter()
            .try_fold(self, |acc, key| match acc.get(key) {
                Some(value) => Ok(value),
                None => Err(())
            });

        match result {
            Ok(value) => Some(value),
            Err(_) => None
        }
    }
}

pub trait DedupExtract<T>
where T: Dotnotation {
    fn dedup_extract_by_key(&mut self, key: &str) -> Vec<T>;
}

impl<T> DedupExtract<T> for Vec<T>
where T: Dotnotation {
    fn dedup_extract_by_key(&mut self, key: &str) -> Vec<T> {
        log::info!("Deduplicating by key: {}", key);

        let mut dedupe = HashMap::new();
        self.retain(|obj| {
            let value = match obj.get_by_dotnotation(key) {
                Some(value) => value,
                None => return false
            };

            let retained = dedupe.insert(
                value.to_string(),
                obj.to_owned()
            );

            match retained {
                Some(_) => {
                    log::info!("Found duplicate: {}", value.to_string());
                    true
                },
                None => false
            }
        });

        dedupe.values().cloned().into_iter().collect::<Vec<_>>()
    }
}
