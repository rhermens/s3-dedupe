#[cfg(test)]
mod tests {
    use s3_dedupe::ext::*;
    use serde_json::Value;

    #[test]
    fn split_duplicates_can_split_by_root_property() {
        let file_content = std::fs::read_to_string("./tests/fixtures/nested.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content).unwrap();
        let result = objects.dedup_extract_by_dotnotation("id");

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn split_large_nested() {
        let file_content =
            std::fs::read_to_string("./tests/fixtures/bitcoin-unconfirmed.json").unwrap();
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

    #[test]
    fn sorts_by_dotnotation_int() {
        let file_content_1 = std::fs::read_to_string("./tests/fixtures/sort_1_int.json").unwrap();
        let file_content_2 = std::fs::read_to_string("./tests/fixtures/sort_2_int.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content_1).unwrap();
        objects.append(&mut serde_json::from_str::<Vec<Value>>(&file_content_2).unwrap());

        let mut result = objects.dedup_extract_by_dotnotation("id");
        result.sort_by_dotnotation("data.sort");

        assert_eq!(result.len(), 5);

        result.iter().enumerate().for_each(|(i, obj)| {
            assert_eq!(
                obj.get_by_dotnotation("data.sort")
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                (i + 1) as u64
            );
        })
    }

    #[test]
    fn sorts_by_dotnotation_string() {
        let file_content_1 = std::fs::read_to_string("./tests/fixtures/sort_1_string.json").unwrap();
        let file_content_2 = std::fs::read_to_string("./tests/fixtures/sort_2_string.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content_1).unwrap();
        objects.append(&mut serde_json::from_str::<Vec<Value>>(&file_content_2).unwrap());

        let mut result = objects.dedup_extract_by_dotnotation("id");
        result.sort_by_dotnotation("data.sort");

        assert_eq!(result.len(), 5);

        result.iter().enumerate().for_each(|(i, obj)| {
            assert_eq!(
                obj.get_by_dotnotation("data.sort")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                char::from_u32((i + 97) as u32).unwrap().to_string()
            );
        })
    }

    #[test]
    fn sorts_by_dotnotation_date() {
        let file_content_1 = std::fs::read_to_string("./tests/fixtures/sort_1_date.json").unwrap();
        let file_content_2 = std::fs::read_to_string("./tests/fixtures/sort_2_date.json").unwrap();
        let mut objects = serde_json::from_str::<Vec<Value>>(&file_content_1).unwrap();
        objects.append(&mut serde_json::from_str::<Vec<Value>>(&file_content_2).unwrap());

        let mut result = objects.dedup_extract_by_dotnotation("id");
        result.sort_by_dotnotation("data.sort");

        assert_eq!(result.len(), 5);

        assert_eq!(result[0].get("id").unwrap(), "someid5");
        assert_eq!(result[1].get("id").unwrap(), "someid1");
        assert_eq!(result[2].get("id").unwrap(), "someid3");
        assert_eq!(result[3].get("id").unwrap(), "someid2");
        assert_eq!(result[4].get("id").unwrap(), "someid4");
    }
}
