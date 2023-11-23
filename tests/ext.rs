#[cfg(test)]
mod tests {
    use serde_json::Value;
    use s3_dedupe::ext::*;

    #[test]
    fn split_duplicates_can_split_by_root_property() {
        let file_content = std::fs::read_to_string("./tests/fixtures/nested.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("id");

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn split_large_nested() {
        let file_content = std::fs::read_to_string("./tests/fixtures/bitcoin-unconfirmed.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("hash");

        assert_eq!(result.len(), 100);
    }

    #[test]
    fn split_duplicates_can_split_by_nested_property() {
        let file_content = std::fs::read_to_string("./tests/fixtures/nested.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("data.somekey");

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn keeps_data_structure() {
        let file_content = std::fs::read_to_string("./tests/fixtures/nested.json").unwrap();
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
        let file_content = std::fs::read_to_string("./tests/fixtures/casing.json").unwrap();
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
