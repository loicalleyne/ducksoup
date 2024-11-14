# ducksoup
 Experiments with DuckDB


## ArrowDuck
POC using Bodkin to detect an Arrow schema, generate mock data with gofakeit and insert the Arrow records into DuckDB using the Arrow API.
Performance on a 13th Gen Intel i7 13700H - inserted ~74K/rows per second.