package net.rzaw.solar.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class Transaction<T>
    implements Callable<T>
{
    protected static final Logger LOG = Logger.getLogger( Transaction.class );

    private final ComboPooledDataSource pooledDatasource;

    private final boolean autoCommit;

    public Transaction( ComboPooledDataSource pooledDatasource, boolean autoCommit )
        throws SQLException
    {
        this.pooledDatasource = pooledDatasource;
        this.autoCommit = autoCommit;
    }

    private Connection getConnection()
        throws SQLException
    {
        Connection connection = pooledDatasource.getConnection();
        connection.setAutoCommit( autoCommit );
        return connection;
    }

    @Override
    public T call()
        throws SQLException, IOException
    {
        LOG.info( "Starting transaction." );
        Connection connection = getConnection();
        T result;
        try
        {
            result = action( connection );
        }
        finally
        {
            LOG.info( "Closing connection." );
            connection.close();
        }
        return result;
    }

    public abstract T action( Connection connection )
        throws SQLException, IOException;
}
