use serde_derive::{Deserialize, Serialize};
use snafu::{Location, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} encode log {:?} failed: {}", location, item, source))]
    EncodeLog {
        source: serde_json::Error,
        location: Location,
        item: LogItem,
    },

    #[snafu(display("{} decode log {:?} failed: {}", location, json_str, source))]
    DecodeLog {
        source: serde_json::Error,
        location: Location,
        json_str: String,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

// log //////////////////////////////////////////////////
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct LogItem {
    pub cmd: String,
    pub key: String,
    #[serde(default)]
    pub value: Option<String>,
}

#[allow(unused)]
impl LogItem {
    pub fn new(cmd: String, key: String, value: Option<String>) -> LogItem {
        LogItem { cmd, key, value }
    }
}

#[allow(unused)]
pub struct LogEncoder;

#[allow(unused)]
impl LogEncoder {
    pub fn encode(item: &LogItem) -> Result<String> {
        serde_json::to_string(item).context(EncodeLogSnafu { item: item.clone() })
    }

    pub fn decode(json_str: &str) -> Result<LogItem> {
        serde_json::from_str(json_str).context(DecodeLogSnafu { json_str })
    }
}

#[cfg(test)]
mod tests {
    // use assert_cmd::assert;
    use super::{LogEncoder, LogItem};
    #[test]
    fn test_log_item_serde() {
        // decode valid
        let test_json1 = r#"
        {
            "cmd": "get",
            "key": "key1",
            "value": null
        }
        "#;
        let test_log1 = LogEncoder::decode(test_json1).unwrap();
        assert_eq!(test_log1.cmd, "get");
        assert_eq!(test_log1.key, "key1");
        assert!(test_log1.value.is_none());

        // decode invalid
        let test_json2 = r#"
        {
            "cmd": "get",
            "inv_key": "key1",
            "inv_value": null,
        }
        "#;
        let test_log2 = LogEncoder::decode(test_json2);
        assert!(test_log2.is_err());

        // encode
        let test_log3 = LogItem::new(
            "set".to_owned(),
            "key2".to_owned(),
            Some("value2".to_owned()),
        );

        let test_log4 = LogItem::new("get".to_owned(), "key3".to_owned(), None);
        let res3 = LogEncoder::encode(&test_log3);
        let res4 = LogEncoder::encode(&test_log4);
        assert!(res3.is_ok());
        assert!(res4.is_ok());
        let test_json3 = res3.unwrap();
        let test_json4 = res4.unwrap();
        assert!(test_json3.contains("set"));
        assert!(test_json3.contains("key2"));
        assert!(test_json3.contains("value2"));
        assert!(test_json4.contains("get"));
        assert!(test_json4.contains("key3"));
        assert!(test_json4.contains("null"));
    }
}
