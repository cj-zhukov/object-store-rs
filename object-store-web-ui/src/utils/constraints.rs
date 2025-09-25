pub const URL: &str = "https://path-to-api"; // #TODO setup as env var
pub const ZIP_NAME: &str = "download.zip";
pub const QUERY_EXAMPLES: &[(&str, &str)] = &[
    ("Basic Usage #1", 
    r#"
    select * from object_store"#),
    ("Basic Usage #2", 
    r#"
    select * from object_store 
    where file_name = 'foo' 
    limit 10"#),
    ("Files from 2021 (by prefix)", 
    r#"
    select * from object_store 
    where dt like '2021%' 
    limit 10"#),
    ("Files from 2021 (by date range)", 
    r#"
    select * from object_store 
    where cast(dt AS date) between '2021-01-01' and '2021-12-01' 
    limit 10"#),
    ("Filter by file_type", 
    r#"
    select * from object_store 
    where file_type in ('foo', 'bar', 'baz') 
    limit 10"#),
    ("Exclude by file name", 
    r#"
    select * from object_store 
    where file_name not in ('foo', 'bar', 'baz') 
    limit 10"#),
    ("Biggest foo files from 2022", 
    r#"
    select * from object_store 
    where dt like '2022%' 
    and file_type = 'foo'
    order by file_size desc 
    limit 10"#),
];
