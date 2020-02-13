use ieql::{Query};

pub fn get_query() -> Query {
    ron::de::from_str("Query ( response: ( kind: Full, include: [ Url, Excerpt ], ), scope: ( pattern: ( content: \".+\", kind: RegEx, ), content: Text, ), threshold: ( considers: [ Trigger(\"0\"), Trigger(\"1\"), ], requires: 1, inverse: false, ), triggers: [ ( pattern: ( content: \"[Ii]cy[Bb]ounce\", kind: RegEx, ), id: \"0\", ), ( pattern: ( content: \"Icy Bounce\", kind: Raw, ), id: \"1\", ) ], id: Some(\"Igor\"),)").unwrap()
}