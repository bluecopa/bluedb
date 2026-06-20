# Collections search

bluedb exposes an **Elasticsearch-shaped search API** over `/collections` documents.
Documents are indexed with BM25 relevance (powered by bluedb's embedded tantivy
engine), and the request/response shapes intentionally mirror the Elasticsearch
`_search` API, so tooling, queries, and mental models transfer. The API speaks
**plain HTTP/JSON**, not the Elasticsearch wire or REST protocol: native ES clients
(`elasticsearch-py`, `@elastic/elasticsearch`) cannot connect directly. Any HTTP
client works.

!!! note "Not the Elasticsearch wire protocol"
    This surface is **HTTP-compatible** with Elasticsearch's shape, but it does not
    speak the Elasticsearch REST API protocol. Kibana, native ES client libraries,
    and `_cluster`/`_cat` endpoints are not supported. Use any HTTP client.

## Declaring a search mapping

Before a collection can be searched, you declare a **search mapping** that tells
bluedb which fields to index and how to analyze them.

```bash
curl -s -X POST localhost:8081/collections/articles/searchIndex \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{
        "fields": {
          "title":    {"analyzer": "english"},
          "body":     {"analyzer": "english"},
          "author":   {"type": "keyword"},
          "year":     {"type": "integer"},
          "tags":     {"analyzer": "standard"}
        }
      }'
# {"acknowledged":true,"backfilled":142}
```

`backfilled` is the number of documents already in the collection that were
indexed in the background during this call. Re-declaring the mapping replaces the
existing one and re-indexes all existing documents.

**Required scope:** `schema:admin`. Active writer only.

### Field types

| Field spec | Meaning |
|------------|---------|
| `{"analyzer": "english"}` | **text** field; language-aware stemming (Porter). Enables `match`/`match_phrase`/BM25 scoring. |
| `{"analyzer": "standard"}` | **text** field; Unicode whitespace + lowercase tokenization. No stemming. |
| `{"analyzer": "whitespace"}` | **text** field; whitespace tokenization, case-preserved. |
| `{"type": "keyword"}` | **keyword** field; stored untokenized. Supports exact `term`/`range` queries. |
| `{"type": "integer"}` | **integer** field (i64). Supports `term`/`range` queries and numeric sort. |

!!! note "No boolean or float field types in v1"
    Boolean and float field types are not available in this release. Store booleans
    or floats as `keyword` or `integer` as appropriate, or omit them from the
    mapping.

### Describing a mapping

```bash
curl -s localhost:8081/collections/articles/searchIndex \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme'
```

```json
{
  "articles": {
    "mappings": {
      "title":  {"analyzer": "english"},
      "body":   {"analyzer": "english"},
      "author": {"type": "keyword"},
      "year":   {"type": "integer"},
      "tags":   {"analyzer": "standard"}
    }
  }
}
```

Returns `404` if no mapping has been declared for the collection.

**Required scope:** `data:read`.

## Searching

```bash
curl -s -X POST localhost:8081/collections/articles/search \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{
        "query": {
          "bool": {
            "must":   [{"match": {"body": "database storage"}}],
            "filter": [{"range": {"year": {"gte": 2020}}}]
          }
        },
        "from": 0,
        "size": 5,
        "sort": [{"year": "desc"}],
        "_source": ["title", "author", "year"],
        "highlight": {"fields": {"body": {}}}
      }'
```

Response (the Elasticsearch hits envelope):

```json
{
  "took": 12,
  "timed_out": false,
  "hits": {
    "total":     {"value": 23, "relation": "eq"},
    "max_score": 1.842,
    "hits": [
      {
        "_index":  "articles",
        "_id":     "d4e5f6a1b2c3",
        "_score":  1.842,
        "_source": {"title": "Object-storage databases", "author": "ada", "year": 2023},
        "highlight": {
          "body": ["…a <em>database</em> built directly on object <em>storage</em>…"]
        }
      }
    ]
  }
}
```

`hits.total.value` is the number of matching documents and `hits.total.relation`
is `"eq"` for an exact count. Match counts above roughly 100,000 are reported with
`"relation": "gte"`: the value is a lower bound, not an exact total.

**Required scope:** `data:read`.

### Request fields

| Field | Default | Description |
|-------|---------|-------------|
| `query` | `match_all` | Query DSL expression (see below) |
| `from` | `0` | Offset into the ranked result set (pagination) |
| `size` | `10` | Number of hits to return |
| `sort` | `[{"_score": "desc"}]` | Sort specification (see [Sort](#sort)) |
| `_source` | `true` | Source inclusion: `true` (full doc), `false` (omit), or `["field", …]` (include-list) |
| `highlight` | — | Highlighting configuration: `{"fields": {"<field>": {}}}` |

## Query DSL compatibility

### Supported query types

| Query type | Example | Notes |
|------------|---------|-------|
| `match_all` | `{"match_all": {}}` | Matches every document |
| `match` | `{"match": {"body": "database storage"}}` | Full-text analysis + BM25 scoring. Default operator: `OR`. |
| `match` with options | `{"match": {"body": {"query": "database storage", "operator": "and"}}}` | Force `AND`: all terms must appear |
| `match_phrase` | `{"match_phrase": {"body": "object storage"}}` | Phrase match; terms must appear in order |
| `term` | `{"term": {"author": "ada"}}` | Exact match. String value for `keyword` fields; integer for `integer` fields |
| `range` | `{"range": {"year": {"gte": 2020, "lte": 2023}}}` | Supported on `integer` and `keyword` fields. Operators: `gt`, `gte`, `lt`, `lte` |
| `exists` | `{"exists": {"field": "author"}}` | Matches documents where the field is present (non-null) |
| `bool` | `{"bool": {"must": […], "should": […], "must_not": […], "filter": […]}}` | Compound queries. `filter` behaves as `must` for matching in v1 (no score contribution) |

!!! note "`range` on analyzed text is not supported"
    `range` queries on `text` fields (those declared with an `analyzer`) are
    rejected with a 400 error. Use `range` on `keyword` or `integer` fields only.

### Not supported in v1

The following query types are not implemented in this release and return a
`400` error if used:

- `fuzzy`, `wildcard`, `prefix`
- `multi_match`, `query_string`, `simple_query_string`
- `nested`, `geo_*`
- `percolate`, `more_like_this`
- `span_*`

## Results features

### Pagination

Use `from` + `size` to page through results:

```json
{"query": {"match_all": {}}, "from": 20, "size": 10}
```

`from` is the zero-based offset; `size` is the page size. Large offsets are
supported but become slower as `from` grows, because BM25 ranking must evaluate
the full result set up to `from + size`.

### Sort

Results default to descending `_score` (BM25 relevance). To sort by a field:

```json
{"sort": [{"year": "asc"}]}
```

| Sort expression | Supported | Notes |
|-----------------|-----------|-------|
| `{"_score": "desc"}` | yes | Default; descending relevance score |
| `{"<integer-field>": "asc"\|"desc"}` | yes | Numeric sort on a mapped `integer` field |
| `"<integer-field>"` (bare string) | yes | Ascending numeric sort (matches Elasticsearch's bare-field default) |
| `{"<text-field>": …}` | **no** | Sorting on an analyzed `text` or `keyword` field returns a 400 error in v1 |

Documents missing the sort field sort **last** regardless of direction (the
Elasticsearch `missing: _last` default).

### `_source`

Control which fields appear in each hit's `_source`:

| Value | Behavior |
|-------|----------|
| `true` (default) | Full document returned |
| `false` | `_source` omitted from hits (only `_id` and `_score`) |
| `["field", "field2"]` | Only the listed top-level fields are included |

### Highlight

Highlighting marks the matched query terms in the source text. Declare fields to
highlight in the request:

```json
{"highlight": {"fields": {"body": {}, "title": {}}}}
```

Each hit that matches a highlighted field gains a `highlight` object:

```json
"highlight": {
  "body": ["…retrieval in a <em>database</em> backed by <em>storage</em>…"]
}
```

!!! warning "Best-effort highlighting"
    Highlighting is **best-effort** whole-token `<em>` wrapping over the raw
    `_source`, not fragment extraction. It matches the analyzed query terms against
    the raw source words:

    - With the **`english`** (stemming) analyzer, a stemmed query term may not wrap
      the original source word. For example, the query `"dogs"` is stemmed to
      `"dog"` during indexing and matching, but the source word `"dogs"` will not
      receive an `<em>` tag because the highlight pass matches the stem `"dog"`,
      not the surface form `"dogs"`. Use `standard` or `whitespace` analyzers when
      exact-token highlighting matters.
    - With **`keyword`** or **`standard`** analyzers, highlighting works as
      expected for exact terms.

    tantivy fragment snippets (offset-based highlighting) are not used in v1.

## Freshness

Search reflects **read-your-writes on the active writer**: a document you just
inserted, updated, or deleted is immediately visible to a search on the same node
that accepted the write. No seal cycle is needed.

Reader replicas are **eventually consistent**: they see documents only after the
underlying data has sealed and propagated. If you need search to reflect a recent
write on a reader replica, route the request to the active writer.

`searchIndex` declarations and per-document index maintenance are **active-writer
only**.

If search-index maintenance fails during a write, the document is still stored
durably and the write reports success; the search index may lag until that
document is next written or the mapping is re-declared (which backfills it).

## Authorization and tenancy

Each request must carry a bearer token:

```
Authorization: Bearer <token>
X-Bluedb-Tenant: acme
```

Omitting `X-Bluedb-Tenant` uses the default tenant `_`. Tenants are fully
isolated: a mapping declared in one tenant is invisible to another.

| Operation | Required scope |
|-----------|----------------|
| `POST /collections/{c}/searchIndex` (declare/replace mapping) | `schema:admin` |
| `GET /collections/{c}/searchIndex` (describe mapping) | `data:read` |
| `POST /collections/{c}/search` | `data:read` |

See [REST API: Authorization](../api/rest.md#authorization) for token and scope
configuration.

## Errors

Error responses use bluedb's **standard error body** (a JSON object with an
`"error"` string field), not the Elasticsearch `error.type`/`reason` envelope:

```json
{"error": "no search mapping for collection 'articles'"}
```

| Situation | HTTP status |
|-----------|-------------|
| No mapping declared for the collection | `404` |
| Malformed query DSL / unsupported query type | `400` |
| Query on an unmapped field | `400` |
| Range on an analyzed text field | `400` |
| Sort on a text or keyword field | `400` |
| Request sent to a passive (non-writer) node for a write op | `503` |

## See also

- [Collections: MongoDB-style API](README.md): `find`, `aggregate`, `createIndex`, and more.
- [Full-text search](../sql/full-text-search.md): BM25 and trigram search via SQL.
- [REST API](../api/rest.md): the SQL and REST data planes.
