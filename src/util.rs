use ieql::common::pattern::{Pattern, PatternKind};
use ieql::query::response::{Response, ResponseItem, ResponseKind};
use ieql::query::scope::{Scope, ScopeContent};
use ieql::query::threshold::{Threshold, ThresholdConsideration};
use ieql::query::trigger::Trigger;
use ieql::Query;

pub fn get_query() -> Query {
    Query {
        response: Response {
            kind: ResponseKind::Full,
            include: vec![ResponseItem::Excerpt, ResponseItem::Url],
        },
        scope: Scope {
            pattern: Pattern {
                content: String::from(".+"),
                kind: PatternKind::RegEx,
            },
            content: ScopeContent::Raw,
        },
        threshold: Threshold {
            considers: vec![
                ThresholdConsideration::Trigger(String::from("A")),
                ThresholdConsideration::NestedThreshold(Threshold {
                    considers: vec![
                        ThresholdConsideration::Trigger(String::from("B")),
                        ThresholdConsideration::Trigger(String::from("C")),
                    ],
                    inverse: true,
                    requires: 1,
                }),
            ],
            inverse: false,
            requires: 2,
        },
        triggers: vec![
            Trigger {
                pattern: Pattern {
                    content: String::from("hello"),
                    kind: PatternKind::RegEx,
                },
                id: String::from("A"),
            },
            Trigger {
                pattern: Pattern {
                    content: String::from("everyone"),
                    kind: PatternKind::RegEx,
                },
                id: String::from("B"),
            },
            Trigger {
                pattern: Pattern {
                    content: String::from("around"),
                    kind: PatternKind::RegEx,
                },
                id: String::from("C"),
            },
        ],
        id: Some(String::from("Test Trigger #2 (inverse)")),
    }
}