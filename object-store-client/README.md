# First, export the backend URL
export OBJECT_STORE_URL="https://api-path/"

# Run SELECT query and print a table of results
```bash
python3 -m object_store_client -q "select * from object_store limit 10" -p select
```

# Download results as zip
```bash
python3 -m object_store_client -q "select * from object_store where file_type = 'foo' limit 10" -p download -o download.zip
```