package com.example.eshadoop.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EsSparkSQL {
	public static final Logger logger = LoggerFactory.getLogger(EsSparkSQL.class);
	
	@Autowired
	private SparkSession sparkSession;

	public Map<String,Dataset<Row>> readValues(String indexName, String[] fieldNames) {
		String statement = String.format("select %s from view1", StringUtils.join(fieldNames, ","));
		Dataset<Row> dataSetAD = null;
		Dataset<Row> dataSet = null;
		try {
			dataSet = sparkSession.read().format("org.elasticsearch.spark.sql").load(indexName);
			dataSet.createOrReplaceTempView("view1");
			dataSetAD = sparkSession.sql(statement);
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
		Map<String, Dataset<Row>> dataSetMap = new HashMap<String, Dataset<Row>>();
		dataSetMap.put("dataSet", dataSet);
		dataSetMap.put("dataSetAD", dataSetAD);
		return dataSetMap;
	}
}
