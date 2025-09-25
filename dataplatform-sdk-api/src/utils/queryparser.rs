use crate::utils::constants::prod::*;

use sqlparser::ast::{Expr, LimitClause, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum QueryParserError {
    #[error("SQL parse error")]
    SqlParseError(#[from] ParserError),

    #[error("Invalid query: must contain 'object_store'")]
    InvalidTableName,

    #[error("Invalid query: must contain 'object_store_catalog'")]
    InvalidCatalogTableName,

    #[error("Select query type not found")]
    SelectQueryNotFound,

    #[error("Unsupported query type")]
    UnsupportedQueryType,
}

#[derive(Debug)]
pub enum QueryKind {
    SelectDownload,
    Catalog,
}

/// validate the query
pub fn prepare_query(query: &str, query_kind: QueryKind) -> Result<String, QueryParserError> {
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, query)?;
    let query_stmt = ast
        .get_mut(0)
        .ok_or(QueryParserError::UnsupportedQueryType)?;
    let Statement::Query(_query) = query_stmt else {
        return Err(QueryParserError::UnsupportedQueryType);
    };

    match query_kind {
        QueryKind::SelectDownload => prepare_query_select_download(&mut ast),
        QueryKind::Catalog => prepare_query_catalog(&mut ast),
    }
}

/// validate the query,
/// prepare the query by adding limit if not exists,
/// checkening table name,
fn prepare_query_select_download(ast: &mut [Statement]) -> Result<String, QueryParserError> {
    if let Some(Statement::Query(query)) = ast.get_mut(0) {
        // check query contains object_store table
        let valid_table = match &*query.body {
            SetExpr::Select(select) => {
                if let Some(from_table) = select.from.get(0) {
                    if let TableFactor::Table { name, .. } = &from_table.relation {
                        name.0
                            .last()
                            .map(|ident| ident.to_string() == TABLE_NAME)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        };
        if !valid_table {
            return Err(QueryParserError::InvalidTableName);
        }

        if let SetExpr::Select(_select) = &mut *query.body {
            // query contains limit
            if query.limit_clause.is_none() {
                query.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number(MAX_ROWS.to_string(), false).into(),
                    )),
                    offset: None,
                    limit_by: vec![],
                })
            };

            Ok(ast[0].to_string())
        } else {
            Err(QueryParserError::SelectQueryNotFound)
        }
    } else {
        Err(QueryParserError::UnsupportedQueryType)
    }
}

/// validate the query,
/// prepare the query by adding limit if not exists,
/// checkening table name,
fn prepare_query_catalog(ast: &mut [Statement]) -> Result<String, QueryParserError> {
    if let Some(Statement::Query(query)) = ast.get_mut(0) {
        // check query contains object_store table
        let valid_table = match &*query.body {
            SetExpr::Select(select) => {
                if let Some(from_table) = select.from.get(0) {
                    if let TableFactor::Table { name, .. } = &from_table.relation {
                        name.0
                            .last()
                            .map(|ident| ident.to_string() == CATALOG_NAME)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        };
        if !valid_table {
            return Err(QueryParserError::InvalidCatalogTableName);
        }

        if let SetExpr::Select(_select) = &mut *query.body {
            // query contains limit
            if query.limit_clause.is_none() {
                query.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number(MAX_ROWS_CATALOG.to_string(), false).into(),
                    )),
                    offset: None,
                    limit_by: vec![],
                })
            };

            Ok(ast[0].to_string())
        } else {
            Err(QueryParserError::SelectQueryNotFound)
        }
    } else {
        Err(QueryParserError::UnsupportedQueryType)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("select * from object_store", Ok("SELECT * FROM object_store LIMIT 10".to_string()))]
    #[case("select * from object_store limit 10", Ok("SELECT * FROM object_store LIMIT 10".to_string()))]
    #[case("select * from object_store where file_name = 'foo'", Ok("SELECT * FROM object_store WHERE file_name = 'foo' LIMIT 10".to_string()))]
    #[case("select * from object_store where file_name = 'foo' limit 10", Ok("SELECT * FROM object_store WHERE file_name = 'foo' LIMIT 10".to_string()))]
    #[case("select * from foo", Err(QueryParserError::InvalidTableName))]
    #[case(
        "delete from object_store",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "update object_store set file_name = 'foo' where file_name = 'bar'",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "insert into object_store(file_name) values('foo')",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case("foo bar baz", Err(QueryParserError::SqlParseError(ParserError::ParserError("Expected: an SQL statement, found: foo at Line: 1, Column: 1".to_string()))))]
    fn prepare_query_select_download_test(
        #[case] input: &str,
        #[case] expected: Result<String, QueryParserError>,
    ) {
        assert_eq!(expected, prepare_query(input, QueryKind::SelectDownload));
    }

    #[rstest]
    #[case("select * from object_store_catalog", Ok("SELECT * FROM object_store_catalog LIMIT 1000".to_string()))]
    #[case("select * from object_store_catalog limit 10", Ok("SELECT * FROM object_store_catalog LIMIT 10".to_string()))]
    #[case("select * from object_store_catalog where file_type = 'foo'", Ok("SELECT * FROM object_store_catalog WHERE file_type = 'foo' LIMIT 1000".to_string()))]
    #[case("select * from object_store_catalog where file_type = 'foo' limit 10", Ok("SELECT * FROM object_store_catalog WHERE file_type = 'foo' LIMIT 10".to_string()))]
    #[case("select * from object_store_catalog where file_type = 'foo' limit 10", Ok("SELECT * FROM object_store_catalog WHERE file_type = 'foo' LIMIT 10".to_string()))]
    #[case("select * from object_store_catalog where file_type = 'foo' or year = '2020'", Ok("SELECT * FROM object_store_catalog WHERE file_type = 'foo' OR year = '2020' LIMIT 1000".to_string()))]
    #[case("select * from foo", Err(QueryParserError::InvalidCatalogTableName))]
    #[case(
        "delete from object_store_catalog",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "update object_store_catalog set data_type = 'foo' where data_type = 'rnd'",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "insert into object_store_catalog(file_name) values('foo')",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case("foo bar baz", Err(QueryParserError::SqlParseError(ParserError::ParserError("Expected: an SQL statement, found: foo at Line: 1, Column: 1".to_string()))))]
    fn prepare_query_catalog_test(
        #[case] input: &str,
        #[case] expected: Result<String, QueryParserError>,
    ) {
        assert_eq!(expected, prepare_query(input, QueryKind::Catalog));
    }
}
