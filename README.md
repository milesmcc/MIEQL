# MIEQL

This is a connector implementation for IEQL that allows streaming between [IEQL](https://github.com/milesmcc/ieql) and Common Crawl.

Notably, it provides:

* On-the-fly gzip decoding and processing
* Fully distributed and parallelized architecture
* Master/client functionality via a CLI
* Full integration with AWS S3 for data retrieval

## Database

The database must have the following tables: `queries`, `outputs`, and `inputs`. Create them according to the following SQL commands:

```sql
CREATE TABLE queries (
    ron TEXT
);

CREATE TABLE outputs (
    json JSON
);

CREATE TABLE inputs (
    url TEXT
);
```

---

This is a proof of concept, and is not meant to be used as a library.