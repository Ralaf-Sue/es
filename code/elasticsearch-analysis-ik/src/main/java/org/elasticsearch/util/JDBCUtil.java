package org.elasticsearch.util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.CollectionUtils;
import org.wltea.analyzer.dic.Monitor;

/**
 * @author SuZuQi
 * @title: JDBCUtil
 * @projectName elasticsearch-analysis-ik
 * @description: TODO
 * @date 2021/4/15
 */
public class JDBCUtil {

    private static final Logger logger = ESLoggerFactory.getLogger(Monitor.class.getName());

    private static String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static Properties properties = null;
    private static String MYSQL_URL = null;
    private static String MYSQL_USERNAME = null;
    private static String MYSQL_PASSWORD = null;

    static {
        try {
            Class.forName(MYSQL_DRIVER);
            String userRoot = System.getProperty("user.dir");
            File file = new File(userRoot);
            String parent = file.getParent();
            String currentPath = parent + File.separator + "plugins" + File.separator + "ik";
            String jdbcPath = currentPath + File.separator + "config" + File.separator + "jdbc.properties";
            logger.info(jdbcPath);
            properties = new Properties();
            properties.load(new FileInputStream(jdbcPath));
            MYSQL_URL = properties.getProperty("mysql.url");
            MYSQL_USERNAME = properties.getProperty("mysql.username");
            MYSQL_PASSWORD = properties.getProperty("mysql.password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据sql获取数据
     *
     * @param sql
     * @return
     */
    public static List<Map<String, Object>> getResult(String sql, String[] columns, Object... args) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
            preparedStatement = connection.prepareStatement(sql);
            if (Objects.nonNull(args)) {
                for (int i = 0; i < args.length; i++) {
                    preparedStatement.setObject(i, args[i]);
                }
            }
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> resultMap = new HashMap<>();

                if (!CollectionUtils.isEmpty(columns)) {
                    for (String column : columns) {
                        resultMap.put(column, resultSet.getObject(column));
                    }
                    list.add(resultMap);

                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet);
            close(preparedStatement);
            close(connection);
        }
        return list;
    }

    public static void update(String sql, List<Integer> ids) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ids.size() - 1; i++) {
                sb.append(ids.get(i)).append(",");
            }
            sb.append(ids.get(ids.size() - 1));
            String updateIn = sb.toString().trim();
            preparedStatement = connection.prepareStatement(String.format(sql, updateIn));

            int i = preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet);
            close(preparedStatement);
            close(connection);
        }

    }

    public static void close(AutoCloseable closeable) {
        if (!Objects.isNull(closeable)) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                closeable = null;
            }
        }
    }

}
