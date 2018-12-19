package com.test.cache;

import javax.sql.DataSource;
import java.sql.SQLException;

public interface DataSourceProvider {
    int maximumPoolSize(DataSource var1, JsonObject var2) throws SQLException;

    DataSource getDataSource(JsonObject var1) throws SQLException;

    void close(DataSource var1) throws SQLException;
}
