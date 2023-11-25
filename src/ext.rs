use std::{collections::HashMap, fmt::Display};

use serde_json::Value;

pub trait Dotnotation
where
    Self: Clone + Display,
{
    fn get_by_dotnotation(&self, key: &str) -> Option<&Self>;
}
impl Dotnotation for Value {
    fn get_by_dotnotation(&self, key: &str) -> Option<&Value> {
        match key
            .split(".")
            .try_fold(self, |acc, key| match acc.get(key) {
                Some(value) => Ok(value),
                None => Err(()),
            }) {
            Ok(value) => Some(value),
            Err(_) => None,
        }
    }
}

pub trait DedupExtract<T>
where
    T: Dotnotation,
{
    fn dedup_extract_by_dotnotation(&mut self, key: &str) -> Vec<T>;
}

impl<T> DedupExtract<T> for Vec<T>
where
    T: Dotnotation,
{
    fn dedup_extract_by_dotnotation(&mut self, key: &str) -> Vec<T> {
        let mut dedupe = HashMap::new();
        self.retain(|obj| match obj.get_by_dotnotation(key) {
            Some(value) => dedupe.insert(value.to_string(), obj.to_owned()).is_some(),
            None => false,
        });

        dedupe.values().cloned().into_iter().collect::<Vec<_>>()
    }
}

pub trait SortByDotnotation<T>
where
    T: Dotnotation,
{
    fn sort_by_dotnotation(&mut self, key: &str);
}

impl<T> SortByDotnotation<T> for Vec<T>
where 
    T: Dotnotation,
{
    fn sort_by_dotnotation(&mut self, key: &str) {
        self.sort_unstable_by(|a, b| {
            let a_value = a.get_by_dotnotation(&key).expect("Invalid sort by").to_string();
            let b_value = b.get_by_dotnotation(&key).expect("Invalid sort by").to_string();

            a_value.cmp(&b_value)
        });
    }
}

