package com.example.client.restclient.service.impl;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.client.restclient.configuration.SqlJdbcClientConfig;

@Service
public class SqlJdbcClientServiceImpl implements SqlJdbcClientService {
	public static final Logger logger = LoggerFactory.getLogger(SqlJdbcClientServiceImpl.class);
	
	@Autowired
	Connection connection;

	@Override
	public Map<String, Object> executeQuery(String url, String sqlStatement) {
		List<Map<String, Object>> hitList = new ArrayList<Map<String, Object>>();
		Map<String, Object> result = new HashMap<String, Object>();
		Statement statement;
		int count=0;
		Connection new_connection=null;
		
		try {
			if (url != null && !url.isEmpty())
				statement = connection.createStatement();
			else {
				new_connection = SqlJdbcClientConfig.AdhocSqlJdbcClient(url);
				statement = new_connection.createStatement();
			}
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			ResultSetMetaData rsmd = resultSet.getMetaData();
			count = rsmd.getColumnCount();
			while (resultSet.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i=0; i<count; i++) {
					String columnName = rsmd.getColumnName(i);
					map.put(columnName, resultSet.getObject(columnName));
				}
				hitList.add(map);
			}
			if (new_connection != null) {
				new_connection.close();
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			result.put("error", e.getMessage());
		}


		result.put("total", count);
		result.put("hits", hitList.toArray(new HashMap[hitList.size()]));
		return result;
	}

}
