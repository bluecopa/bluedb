use thiserror::Error;

#[derive(Debug, Error)]
pub enum MqlError {
    #[error("unsupported operator {0}")]
    UnsupportedOperator(String),
    #[error("unsupported aggregation stage {0}")]
    UnsupportedStage(String),
    #[error("malformed query: {0}")]
    Malformed(String),
}

impl MqlError {
    /// MongoDB error code (subset) for the `{ok:0, code, ...}` response shape.
    pub fn mongo_code(&self) -> i32 {
        match self {
            MqlError::UnsupportedOperator(_) | MqlError::Malformed(_) => 2,   // BadValue
            MqlError::UnsupportedStage(_) => 115,                              // CommandNotSupported
        }
    }
    pub fn mongo_code_name(&self) -> &'static str {
        match self {
            MqlError::UnsupportedOperator(_) | MqlError::Malformed(_) => "BadValue",
            MqlError::UnsupportedStage(_) => "CommandNotSupported",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unsupported_operator_names_the_operator() {
        let e = MqlError::UnsupportedOperator("$where".into());
        assert!(e.to_string().contains("$where"));
        assert_eq!(e.mongo_code(), 2); // BadValue
    }
}
