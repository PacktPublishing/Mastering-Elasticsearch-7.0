package com.example.eshadoop.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.spark.sql.functions;
import com.example.eshadoop.common.RestClientResponse;
import com.example.eshadoop.spark.EsSparkSQL;
import com.example.eshadoop.spark.mllib.AnomalyDetection;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class EsHadoopSparkServiceImpl implements EsHadoopSparkService {
	@Autowired
	EsSparkSQL esSparkIO;
	
	@Autowired
	AnomalyDetection anomalyDetection;

	public static final Logger logger = LoggerFactory.getLogger(EsHadoopSparkServiceImpl.class);

	@Override
	public Map<String, Object> buildAnomalyDetectionModel(String indexName, String[] fieldNames, int numOfClass) {
		Map<String, Object> convertValue=null;
		
		Map<String, Dataset<Row>> dataSetMap = esSparkIO.readValues(indexName, fieldNames);
		if (dataSetMap.isEmpty())
			return null;
		Dataset<Row> dataSet = dataSetMap.get("dataSet");
		Dataset<Row> dataSetAD = dataSetMap.get("dataSetAD");
  		Dataset<Row> prediction = anomalyDetection.buildKmeansModel(dataSetAD, indexName, fieldNames, numOfClass);

		if (prediction != null) {
			String predictionString = prediction.collectAsList().toString();
			convertValue = new HashMap<String, Object>();
			convertValue.put("prediction", predictionString);
			Dataset<Row> data_id = dataSet.selectExpr("_metadata._id as id");
			Dataset<Row> data_row_index = data_id.withColumn("row_index", functions.monotonically_increasing_id());
			Dataset<Row> prediction_row_index = prediction.withColumn("row_index", functions.monotonically_increasing_id());
			Dataset<Row> update = data_row_index.join(prediction_row_index, "row_index").drop("row_index");
			update.write().format("org.elasticsearch.spark.sql").option("es.mapping.id", "id").option("es.mapping.exclude", "id")
			.option("es.write.operation", "update").mode("append").save("cf_rfem_hist_price");
			
		} else { 
			RestClientResponse clientResponse = new RestClientResponse();
			clientResponse.setStatusCode(500);
			convertValue = 
					(Map<String, Object>) (new ObjectMapper()).convertValue(clientResponse, RestClientResponse.class);
		}
	
		return convertValue;
	}

}
