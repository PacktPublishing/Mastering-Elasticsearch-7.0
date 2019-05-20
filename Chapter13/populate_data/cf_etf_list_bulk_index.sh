#!/bin/bash
curl -XPOST localhost:9200/cf_etf/_bulk?pretty -H "Content-Type:application/json" --data-binary @cf_etf_list_bulk.json
