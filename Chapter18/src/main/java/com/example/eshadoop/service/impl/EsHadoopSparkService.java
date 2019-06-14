package com.example.eshadoop.service.impl;

import java.util.Map;


public interface EsHadoopSparkService {
	Map<String, Object> buildAnomalyDetectionModel(String indexName, String[] fieldName, int numOfClass);

}
