#[derive(Debug, PartialEq)]
pub enum ApiRoute {
    AliveGet,
    SelectPost,
    DownloadPost,
    CatalogPost,
}

impl TryFrom<(&str, &str)> for ApiRoute {
    type Error = String;

    fn try_from((method, path): (&str, &str)) -> Result<Self, Self::Error> {
        match (method, path) {
            ("GET", "/alive") => Ok(ApiRoute::AliveGet),
            ("POST", "/select") => Ok(ApiRoute::SelectPost),
            ("POST", "/download") => Ok(ApiRoute::DownloadPost),
            ("POST", "/catalog") => Ok(ApiRoute::CatalogPost),
            _ => Err(format!(
                "unsupported resource method: {method}, path: {path}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[test]
    #[case(("GET", "/alive"), Ok(ApiRoute::AliveGet))]
    #[case(("POST", "/select"), Ok(ApiRoute::SelectPost))]
    #[case(("POST", "/download"), Ok(ApiRoute::DownloadPost))]
    #[case(("POST", "/catalog"), Ok(ApiRoute::CatalogPost))]
    #[case(("foo", "/foo"), Err(format!("unsupported resource method: foo, path: /foo")))]
    #[case(("", "/"), Err(format!("unsupported resource method: , path: /")))]
    fn test_api_route(#[case] input: (&str, &str), #[case] expected: Result<ApiRoute, String>) {
        let res = input.try_into();
        assert_eq!(res, expected);
    }
}
