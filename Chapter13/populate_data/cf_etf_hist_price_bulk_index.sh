#!/bin/bash
curl -XDELETE localhost:9200/cf_etf_hist_price
curl -XPUT localhost:9200/cf_etf_hist_price -H "Content-Type:application/json" --data-binary @define_custom_analyzer.json
curl -XPOST localhost:9200/cf_etf_hist_price/_bulk?pretty -H "Content-Type:application/json" --data-binary @cf_etf_hist_price_bulk.json
