package net.rzaw.solar.db;

import static net.rzaw.solar.StringFormatter.formatString;

import java.beans.PropertyVetoException;
import java.io.Closeable;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import net.rzaw.solar.csvimport.CSVImporter;
import net.rzaw.solar.csvimport.InstallationEntry;
import net.rzaw.solar.csvimport.SumEntry;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DatabaseProcessor
    implements Closeable
{
    private static final Logger LOG = Logger.getLogger( CSVImporter.class );

    public static final String POWERS_TABLE = "leistung";

    public static final String POWERS_CONSUMPTION_TABLE = "leistung_verbrauch";

    public static final String YIELD_TABLE = "ertrag";

    public static final String FILES_TABLE = "dateien";

    public static final String CONSUMPTION_TABLE = "verbrauch";

    public static final String INSTALLATIONS_TABLE = "anlage";

    public static final String MYSQL_DATABASE = "mysql.database";

    public static final String MYSQL_USER = "mysql.user";

    public static final String MYSQL_PASSWORD = "mysql.password";

    public static final String MYSQL_SERVER = "mysql.server";

    public static final String DATABASE = "mysql.database";

    public static final String USERNAME = "mysql.user";

    public static final String PASSWORD = "mysql.password";

    public static final String SERVER = "mysql.server";

    public static final String PORT = "mysql.port";

    private final ComboPooledDataSource pooledDatasource;

    private final boolean autoCommit;

    public static final DateTimeFormatter MYSQL_DATETIME_FORMAT = DateTimeFormat.forPattern( "dd.MM.YY HH:mm:ss" );

    public static final DateTimeFormatter MYSQL_DATE_FORMAT = DateTimeFormat.forPattern( "dd.MM.YY" );

    public static final DateTimeFormatter MYSQL_INTERNAL_DATE_FORMAT = DateTimeFormat.forPattern( "YYYY-MM-dd" );

    public static final DateTimeFormatter MYSQL_INTERNAL_DATETIME_FORMAT = DateTimeFormat
        .forPattern( "YYYY-MM-dd HH:mm:ss" );

    public DatabaseProcessor( Properties properties, boolean autoCommit )
        throws SQLException
    {
        // create database connection
        String userName = properties.getProperty( USERNAME );
        String password = properties.getProperty( PASSWORD );
        String database = properties.getProperty( DATABASE );
        String server = properties.getProperty( SERVER );
        String port = properties.getProperty( PORT );

        try
        {
            pooledDatasource = new ComboPooledDataSource();
            pooledDatasource.setDriverClass( "com.mysql.jdbc.Driver" ); // loads the jdbc driver
            String connectionString = formatString( "jdbc:mysql://{}:{}/{}", server, port, database ).toString();

            LOG.debug( formatString( "Use connection string: {}", connectionString ) );
            pooledDatasource.setJdbcUrl( connectionString );
            pooledDatasource.setUser( userName );
            pooledDatasource.setPassword( password );

            // the settings below are optional -- c3p0 can work with defaults
            pooledDatasource.setMinPoolSize( 10 );
            pooledDatasource.setAcquireIncrement( 5 );
            pooledDatasource.setMaxPoolSize( 50 );
            this.autoCommit = autoCommit;
        }
        catch ( PropertyVetoException e )
        {
            close();
            throw new SQLException( e );
        }
    }

    public DatabaseProcessor( Properties properties )
        throws SQLException
    {
        this( properties, true );
    }

    public Connection getConnection()
        throws SQLException
    {
        Connection connection = pooledDatasource.getConnection();
        connection.setAutoCommit( autoCommit );
        return connection;
    }

    @Override
    public void close()
    {
        pooledDatasource.close();
    }

    public int getInstallationId( String installation )
        throws SQLException
    {

        String sqlQuery = formatString( "select id from {} where verzeichnis = ?", INSTALLATIONS_TABLE ).toString();

        ResultSet rs = null;
        int id = -1;
        PreparedStatement statement = null;
        try
        {
            statement = getConnection().prepareStatement( sqlQuery );
            statement.setString( 1, installation );
            rs = statement.executeQuery();
            if ( rs.next() )
            {
                id = rs.getInt( 1 );
            }
        }
        finally
        {
            if ( rs != null )
            {
                rs.close();
            }
            if ( statement != null )
            {
                statement.close();
            }
            Preconditions.checkState( id != -1,
                formatString( "The id for supplied directory {} could not be found in the database.", installation ) );
        }
        return id;
    }

    @SuppressWarnings( "unused" )
    public void updateDaySumColumns( String installationDir, String tableName, Map<String, SumEntry> daySums )
        throws SQLException
    {
        LOG.info( formatString( "Writing to DB table {} day sum, installationEntry: {}", tableName, installationDir ) );

        String placeholder = "(?, ?, ?, ?)";
        String insertQueryStub =
            formatString( "insert into {} (anlage, datum, typ, daysum) values ", tableName ).toString();
        StringBuilder builder = new StringBuilder( insertQueryStub );

        LOG.info( formatString( "Statement for {}: {}", installationDir, builder.toString() ) );
        // don't proceed if the map is empty
        if ( daySums.isEmpty() )
        {
            // there were no rows to add
            return;
        }

        // iterate over every entry in the map we have aka the maximum entry for today to add the ? fields for the
        // statement
        for ( Entry<String, SumEntry> sumColumnsEntry : daySums.entrySet() )
        {
            builder.append( placeholder ).append( "," );
        }

        if ( builder.length() == insertQueryStub.length() )
        {
            // there were no rows to add
            return;
        }

        // delete the last ","
        builder.deleteCharAt( builder.length() - 1 );

        // query id for installation in directory "installationDir"
        int id = getInstallationId( installationDir );

        PreparedStatement statement = null;
        try
        {
            statement = getConnection().prepareStatement( builder.toString() );
            int i = 0;

            // iterate again over every entry in the map we have aka every date line with a sum
            for ( Entry<String, SumEntry> sumColumnsEntry : daySums.entrySet() )
            {
                statement.setInt( ++i, id );
                statement.setTimestamp( ++i, new java.sql.Timestamp( sumColumnsEntry.getValue().getDate().getTime() ) );
                statement.setString( ++i, sumColumnsEntry.getKey() );
                statement.setDouble( ++i, sumColumnsEntry.getValue().getDaySum() );
            }
            int count = statement.executeUpdate();
            LOG.info( formatString( "Rows affected: {}", count ) );
        }
        finally
        {
            if ( statement != null )
            {
                statement.close();
            }
        }
    }

    @SuppressWarnings( "unused" )
    public void updateSumSameColumns( String installationDir, String tableName, Map<String, List<SumEntry>> sumsColumns )
        throws SQLException
    {
        LOG.info( formatString( "Writing to DB table {} sum for same columns, installationEntry: {}", tableName,
            installationDir ) );

        String placeholder = "(?, ?, ?, ?)";
        String insertQueryStub =
            formatString( "insert into {} (anlage, datum, typ, leistung) values ", tableName ).toString();
        StringBuilder builder = new StringBuilder( insertQueryStub );
        LOG.info( formatString( "Statement for {}: {}", installationDir, builder.toString() ) );

        // don't proceed if the map is empty
        if ( sumsColumns.isEmpty() )
        {
            return;
        }

        // iterate over every entry in the map we have aka every date line with a sum to add the ? fields for the
        // statement
        for ( Entry<String, List<SumEntry>> sumColumnsEntry : sumsColumns.entrySet() )
        {
            for ( SumEntry sumEntry : sumColumnsEntry.getValue() )
            {
                builder.append( placeholder ).append( "," );
            }
        }

        if ( builder.length() == insertQueryStub.length() )
        {
            // there were no rows to add
            return;
        }

        // delete the last ","
        builder.deleteCharAt( builder.length() - 1 );

        // query id for installation in directory "installationDir"
        int id = getInstallationId( installationDir );

        PreparedStatement statement = null;
        try
        {
            statement = getConnection().prepareStatement( builder.toString() );
            int i = 0;

            // iterate again over every entry in the map we have aka every date line with a sum
            for ( Entry<String, List<SumEntry>> sumColumnsEntry : sumsColumns.entrySet() )
            {
                for ( SumEntry sumEntry : sumColumnsEntry.getValue() )
                {
                    statement.setInt( ++i, id );
                    statement.setTimestamp( ++i, new java.sql.Timestamp( sumEntry.getDate().getTime() ) );
                    statement.setString( ++i, sumColumnsEntry.getKey() );
                    statement.setDouble( ++i, sumEntry.getDaySum() );
                }
            }
            int count = statement.executeUpdate();
            LOG.info( formatString( "Rows affected: {}", count ) );
        }
        finally
        {
            if ( statement != null )
            {
                statement.close();
            }
        }
    }

    public void updateMd5Sum( File csvFile, String newMd5Sum )
        throws SQLException
    {
        LOG.info( formatString( "Writing to DB MD5 sum for {}, installationEntry:  {}", csvFile, newMd5Sum ) );

        String insertQuery = "insert into dateien (dateiname, md5sum, fertig) values (?, ?, ?)";
        String updateQuery = "update dateien set md5sum = ? where dateiname = ?";

        PreparedStatement statement = null;
        try
        {
            // query table if we already have a md5sum saved for this filename
            String oldMd5Sum = getMd5Sum( csvFile );
            if ( oldMd5Sum == null )
            {
                // there is no md5sum saved for this filename, due to null not being allowed for md5sum by the db, this
                // will be an insert
                statement = getConnection().prepareStatement( insertQuery );
                statement.setString( 1, csvFile.toString() );
                statement.setString( 2, newMd5Sum );
                statement.setBoolean( 3, false );
            }
            else
            {
                // there already is a record with this filename, we got a valid md5sum, so statement will be an update
                statement = getConnection().prepareStatement( updateQuery );
                statement.setString( 1, newMd5Sum );
                statement.setString( 2, csvFile.toString() );
            }
            statement.executeUpdate();
        }
        finally
        {
            if ( statement != null )
            {
                statement.close();
            }
        }
    }

    public void markAsDone( File csvFile )
        throws SQLException
    {
        LOG.info( formatString( "Writing to DB table {}, csvFile: {}", FILES_TABLE, csvFile ) );

        String query = formatString( "update {} set fertig = ? where dateiname = ?", FILES_TABLE ).toString();
        PreparedStatement stat = null;
        try
        {
            stat = getConnection().prepareStatement( query );
            stat.setBoolean( 1, true );
            stat.setString( 2, csvFile.toString() );
            stat.executeUpdate();
        }
        finally
        {
            stat.close();
        }
    }

    /**
     * Check if a file is marked as done.
     * 
     * @param directory
     * @param csvFile
     * @return
     * @throws SQLException
     */
    public boolean isDone( File csvFile )
        throws SQLException
    {
        String selectQuery = formatString( "select fertig from {} where dateiname = ?", FILES_TABLE ).toString();

        ResultSet rs = null;
        boolean isDone = false;
        PreparedStatement statement = null;
        try
        {
            statement = getConnection().prepareStatement( selectQuery );
            statement.setString( 1, csvFile.toString() );
            rs = statement.executeQuery();
            // we have an entry in the db for this file, now lets look whether it is done or not
            if ( rs.next() )
            {
                isDone = rs.getBoolean( 1 );
            }
        }
        finally
        {
            if ( rs != null )
            {
                rs.close();
            }
            if ( statement != null )
            {
                statement.close();
            }
        }
        return isDone;
    }

    /**
     * Reads md5sum from for a csvFile in a directory from db
     * 
     * @param directory
     * @param csvFile
     * @return
     * @throws SQLException
     */
    public String getMd5Sum( File csvFile )
        throws SQLException
    {

        String sqlQuery = formatString( "select md5sum from {} where dateiname = ?", FILES_TABLE ).toString();

        ResultSet rs = null;
        String md5Sum = null;
        PreparedStatement statement = null;
        try
        {
            statement = getConnection().prepareStatement( sqlQuery );
            statement.setString( 1, csvFile.toString() );
            rs = statement.executeQuery();
            if ( rs.next() )
            {
                md5Sum = rs.getString( 1 );
            }
        }
        finally
        {
            if ( rs != null )
            {
                rs.close();
            }
            if ( statement != null )
            {
                statement.close();
            }
        }
        return md5Sum;
    }

    public ArrayList<InstallationEntry> getInstallationEntries()
        throws SQLException
    {
        String sqlQuery =
            formatString( "select id, verzeichnis, verbrauch_daysum_cols, verbrauch_sum_cols from {} where aktiv = 1",
                INSTALLATIONS_TABLE ).toString();

        ArrayList<InstallationEntry> installations = new ArrayList<InstallationEntry>();
        PreparedStatement statement = null;
        ResultSet rs = null;
        try
        {
            statement = getConnection().prepareStatement( sqlQuery );
            rs = statement.executeQuery();
            while ( rs.next() )
            {
                installations.add( new InstallationEntry( rs.getInt( 1 ), rs.getString( 2 ), rs.getString( 3 ), rs
                    .getString( 4 ) ) );
            }
        }
        finally
        {
            if ( rs != null )
            {
                rs.close();
            }
            if ( statement != null )
            {
                statement.close();
            }
        }
        return installations;
    }

    // ==========================================================================

    public String getPowerQueryDay( int installionId, DateTime date )
    {
        return formatString(
            "SELECT datum, leistung * 0.001 " + //
                "FROM {} " + //
                "WHERE anlage = {} " + //
                "and leistung > 0 " + //
                "AND datum BETWEEN '{} 05:30:00' AND '{} 21:30:00'" + //
                "AND typ = 'PAC' " + //
                "order by datum",//
            POWERS_TABLE, installionId, MYSQL_INTERNAL_DATE_FORMAT.print( date ),
            MYSQL_INTERNAL_DATE_FORMAT.print( date ) ).toString();
    }

    public String getYieldQueryYear( int installionId, DateTime date )
    {
        return formatString(
            "SELECT DATE_FORMAT(datum,'%M') as tag, daysum * 0.001 " + //
                "FROM {} " + //
                "WHERE anlage = {} " + //
                "AND datum BETWEEN DATE_SUB('{}', INTERVAL 365 DAY) AND '{}' AND typ = 'DaySum'",// +
            YIELD_TABLE, installionId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ),
            MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) ).toString();
    }

    public String getYieldQueryWeek( int anlageId, DateTime date )
    {
        return formatString(
            "SELECT DATE_FORMAT(datum,'%W') as tag, daysum * 0.001 " + //
                "FROM {} " + //
                "WHERE anlage = {} " + //
                "AND datum BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND '{}' AND typ = 'DaySum'",// +
            YIELD_TABLE, anlageId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ),
            MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) ).toString();
    }

    public String getYieldQueryMonth( int installationId, DateTime date )
    {
        return formatString(
            "SELECT DATE_FORMAT(datum,'%e') as tag, daysum * 0.001 " + //
                "FROM {} " + //
                "WHERE anlage = {} " + //
                "AND datum BETWEEN DATE_SUB('{}', INTERVAL 31 DAY) AND '{}' AND typ = 'DaySum'",//
            YIELD_TABLE, installationId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ),
            MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) ).toString();
    }
}
