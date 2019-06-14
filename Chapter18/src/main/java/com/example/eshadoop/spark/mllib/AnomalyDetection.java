package com.example.eshadoop.spark.mllib;

import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.eshadoop.service.impl.EsHadoopSparkServiceImpl;

import java.io.File;
import java.io.IOException;
import org.apache.spark.ml.clustering.KMeans;

@Component
public class AnomalyDetection {
	public static final Logger logger = LoggerFactory.getLogger(EsHadoopSparkServiceImpl.class);
	
	public Dataset<Row> buildKmeansModel(Dataset<Row> dataset, String indexName, String[] fieldNames, int numOfClass) {
		
		VectorAssembler assembler = new VectorAssembler().setInputCols(fieldNames).setOutputCol("features");
		Dataset<Row> features = assembler.transform(dataset);
		KMeansModel model = null;

		try {
			File file = new File("./kmeansModel");
			if (file.exists() && file.isFile())
				model = KMeansModel.load("./kmeansModel");
			else {
				KMeans kmeans = new KMeans().setFeaturesCol("features").setK(numOfClass).setSeed(1L);
				model = kmeans.fit(features);
				model.write().overwrite().save("./kmeansModel");
			}
		} catch (IOException ex) {
			logger.error(ex.getMessage());
		}

		Dataset<Row> prediction = null;
		if (model != null)
			prediction = model.transform(features).select("prediction");

		return prediction;
	}
}
