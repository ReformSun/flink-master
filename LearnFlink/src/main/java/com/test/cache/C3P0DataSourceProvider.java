package com.test.cache;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import com.mchange.v2.c3p0.cfg.C3P0Config;
import com.mchange.v2.c3p0.impl.C3P0Defaults;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.SQLException;

public class C3P0DataSourceProvider implements DataSourceProvider {
    public C3P0DataSourceProvider() {
    }
    @Override
    public DataSource getDataSource(JsonObject config) throws SQLException {
        String url = config.getString("url");
        if (url == null) {
            throw new NullPointerException("url cannot be null");
        } else {
            String driverClass = config.getString("driver_class");
            String user = config.getString("user");
            String password = config.getString("password");
            Integer maxPoolSize = config.getInteger("max_pool_size");
            Integer initialPoolSize = config.getInteger("initial_pool_size");
            Integer minPoolSize = config.getInteger("min_pool_size");
            Integer maxStatements = config.getInteger("max_statements");
            Integer maxStatementsPerConnection = config.getInteger("max_statements_per_connection");
            Integer maxIdleTime = config.getInteger("max_idle_time");
            Integer acquireRetryAttempts = config.getInteger("acquire_retry_attempts");
            Integer acquireRetryDelay = config.getInteger("acquire_retry_delay");
            Boolean breakAfterAcquireFailure = config.getBoolean("break_after_acquire_failure");
            ComboPooledDataSource cpds = new ComboPooledDataSource();
            cpds.setJdbcUrl(url);
            if (driverClass != null) {
                try {
                    cpds.setDriverClass(driverClass);
                } catch (PropertyVetoException var17) {
                    throw new IllegalArgumentException(var17);
                }
            }
            if (user != null) {
                cpds.setUser(user);
            }

            if (password != null) {
                cpds.setPassword(password);
            }

            if (maxPoolSize != null) {
                cpds.setMaxPoolSize(maxPoolSize.intValue());
            }

            if (minPoolSize != null) {
                cpds.setMinPoolSize(minPoolSize.intValue());
            }

            if (initialPoolSize != null) {
                cpds.setInitialPoolSize(initialPoolSize.intValue());
            }

            if (maxStatements != null) {
                cpds.setMaxStatements(maxStatements.intValue());
            }

            if (maxStatementsPerConnection != null) {
                cpds.setMaxStatementsPerConnection(maxStatementsPerConnection.intValue());
            }

            if (maxIdleTime != null) {
                cpds.setMaxIdleTime(maxIdleTime.intValue());
            }

            if (acquireRetryAttempts != null) {
                cpds.setAcquireRetryAttempts(acquireRetryAttempts.intValue());
            }

            if (acquireRetryDelay != null) {
                cpds.setAcquireRetryDelay(acquireRetryDelay.intValue());
            }

            if (breakAfterAcquireFailure != null) {
                cpds.setBreakAfterAcquireFailure(breakAfterAcquireFailure.booleanValue());
            }

            return cpds;
        }
    }

    @Override
    public void close(DataSource dataSource) throws SQLException {
        if (dataSource instanceof PooledDataSource) {
            ((PooledDataSource)dataSource).close();
        }

    }
    @Override
    public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
        if (dataSource instanceof PooledDataSource) {
            Integer val = config.getInteger("max_pool_size");
            if (val == null) {
                val = C3P0Config.initializeIntPropertyVar("maxPoolSize", C3P0Defaults.maxPoolSize());
            }

            return val.intValue();
        } else {
            return -1;
        }
    }
}
