# graphQL OpenEBench test queries

Ways to launch these graphql queries using curl

```bash
curl -H "Content-type: application/json" https://dev-openebench.bsc.es/sciapi/graphql -d @aggregation_query.json
curl -u user:password -H "Content-type: application/json" https://dev-openebench.bsc.es/api/scientific/graphql -d @aggregation_query.json
curl -H 'Authorization: Bearer token' -H "Content-type: application/json" https://dev-openebench.bsc.es/api/scientific/graphql -d @aggregation_query.json
```
