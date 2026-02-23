/// Helper: quote identifier (double-quote, escaping inner double-quotes).
pub fn qi(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
