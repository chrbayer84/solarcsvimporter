package net.rzaw.solar.db;

import static net.rzaw.solar.StringFormatter.formatString;

import java.beans.PropertyVetoException;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

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
    private static final Logger LOG = Logger.getLogger( DatabaseProcessor.class );

    public static final String POWERS_TABLE = "leistung";

    public static final String POWERS_CONSUMPTION_TABLE = "leistung_verbrauch";

    public static final String YIELD_TABLE = "ertrag";

    public static final String FILES_TABLE = "dateien";

    public static final String CONSUMPTION_TABLE = "verbrauch";

    public static final String INSTALLATIONS_TABLE = "anlage";

    private static final String DATABASE = "mysql.database";

    private static final String USERNAME = "mysql.user";

    private static final String PASSWORD = "mysql.password";

    private static final String SERVER = "mysql.server";

    private static final String PORT = "mysql.port";

    public static final DateTimeFormatter MYSQL_DATETIME_FORMAT = DateTimeFormat.forPattern( "dd.MM.YY HH:mm:ss" );

    public static final DateTimeFormatter MYSQL_DATE_FORMAT = DateTimeFormat.forPattern( "dd.MM.YY" );

    public static final DateTimeFormatter MYSQL_INTERNAL_DATE_FORMAT = DateTimeFormat.forPattern( "YYYY-MM-dd" );

    public static final DateTimeFormatter MYSQL_INTERNAL_DATETIME_FORMAT = DateTimeFormat
        .forPattern( "YYYY-MM-dd HH:mm:ss" );

    private static final String FILES_DATE_COLUMN = "datum";

    private final ComboPooledDataSource pooledDatasource;

    private final boolean autoCommit;

    public DatabaseProcessor( Properties properties, boolean autoCommit )
        throws SQLException, IOException
    {
        // create database connection
        String userName = properties.getProperty( USERNAME );
        String password = properties.getProperty( PASSWORD );
        String database = properties.getProperty( DATABASE );
        String server = properties.getProperty( SERVER );
        String port = properties.getProperty( PORT );

        this.autoCommit = autoCommit;

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
        }
        catch ( PropertyVetoException e )
        {
            close();
            throw new SQLException( e );
        }
        // if date column does not exist, create it
        if ( !isNewSchema() )
        {
            addDateColumnFiles();
        }
        Preconditions.checkState( isNewSchema(), "Could not create new date column to files table." );
    }

    private boolean isNewSchema()
        throws SQLException, IOException
    {
        return new Transaction<Boolean>( pooledDatasource, autoCommit )
        {
            @Override
            public Boolean action( Connection connection )
                throws SQLException
            {
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet rs = metaData.getColumns( null, null, FILES_TABLE, FILES_DATE_COLUMN );
                return rs.next();
            }
        }.call();
    }

    public ComboPooledDataSource getPooledDatasource()
    {
        return pooledDatasource;
    }

    private void addDateColumnFiles()
        throws SQLException, IOException
    {
        final String sqlAdd = formatString( "ALTER TABLE {} ADD COLUMN {} DATETIME",//
            FILES_TABLE, FILES_DATE_COLUMN ).toString();

        new Transaction<Boolean>( pooledDatasource, autoCommit )
        {
            @Override
            public Boolean action( Connection connection )
                throws SQLException
            {
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( sqlAdd );
                    statement.execute();
                }
                catch ( SQLException e )
                {
                    // re-trow
                    throw e;
                }
                finally
                {
                    if ( statement != null )
                    {
                        statement.close();
                    }
                }
                return null;
            }
        }.call();
    }

    public DatabaseProcessor( Properties properties )
        throws SQLException, IOException
    {
        this( properties, true );
    }

    @Override
    public void close()
    {
        pooledDatasource.close();
    }

    public int getInstallationId( final String installation )
        throws SQLException, IOException
    {
        final String sqlQuery =
            formatString( "select id from {} where verzeichnis = ?", INSTALLATIONS_TABLE ).toString();
        return new Transaction<Integer>( pooledDatasource, autoCommit )
        {
            @Override
            public Integer action( Connection connection )
                throws SQLException
            {
                ResultSet rs = null;
                int id = -1;
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( sqlQuery );
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
                    Preconditions.checkState(
                        id != -1,
                        formatString( "The id for supplied directory {} could not be found in the database.",
                            installation ) );
                }
                return id;
            }
        }.call();
    }

    public void updateDaySumColumns( final String installationDir, final String tableName,
                                     final Map<String, SumEntry> daySums )
        throws SQLException, IOException
    {
        LOG.info( formatString( "Writing to DB table {} day sum, installationEntry: {}", tableName, installationDir ) );

        final String replaceQuery =
            formatString( "replace into {} (anlage, datum, typ, daysum) value (?, ?, ?, ?)", tableName ).toString();

        // don't proceed if the map is empty
        if ( daySums.isEmpty() )
        {
            // there were no rows to add
            return;
        }

        LOG.info( formatString( "Statements for {}: {}", installationDir, replaceQuery ) );

        // query id for installation in directory "installationDir"
        final int id = getInstallationId( installationDir );

        new Transaction<Void>( pooledDatasource, false )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException
            {
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( replaceQuery.toString() );

                    // iterate again over every entry in the map we have aka every date line with a sum
                    for ( Entry<String, SumEntry> sumColumnsEntry : daySums.entrySet() )
                    {
                        int i = 0;
                        statement.setInt( ++i, id );
                        // set date for daysum to midnight
                        statement.setTimestamp( ++i, new Timestamp( new DateTime( sumColumnsEntry.getValue().getDate() )
                            .toDateMidnight().toDate().getTime() ) );
                        statement.setString( ++i, sumColumnsEntry.getKey() );
                        statement.setDouble( ++i, sumColumnsEntry.getValue().getDaySum() );
                        statement.addBatch();
                    }
                    int[] count = statement.executeBatch();
                    LOG.info( formatString( "REPLACE: Rows affected: {}", addArray( count ) ) );
                }
                finally
                {
                    if ( statement != null )
                    {
                        statement.close();
                    }
                }
                return null;
            }
        }.call();
    }

    private int addArray( int[] count )
    {
        int sum = 0;
        for ( int i : count )
        {
            sum += i;
        }
        return sum;
    }

    public void updateSumSameColumns( final String installationDir, final String tableName,
                                      final Map<String, List<SumEntry>> sumsColumns )
        throws SQLException, IOException
    {
        LOG.info( formatString( "Writing to DB table {} sum for same columns, installationEntry: {}", tableName,
            installationDir ) );

        final String replaceQuery =
            formatString( "replace into {} (anlage, datum, typ, leistung) value (?, ?, ?, ?)", tableName ).toString();

        // don't proceed if the map is empty
        if ( sumsColumns.isEmpty() )
        {
            return;
        }

        LOG.info( formatString( "Statements for {}: {}", installationDir, replaceQuery ) );
        // query id for installation in directory "installationDir"
        final int id = getInstallationId( installationDir );
        new Transaction<Void>( pooledDatasource, false )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException
            {
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( replaceQuery );

                    // iterate again over every entry in the map we have aka every date line with a sum
                    for ( Entry<String, List<SumEntry>> sumColumnsEntry : sumsColumns.entrySet() )
                    {
                        for ( SumEntry sumEntry : sumColumnsEntry.getValue() )
                        {
                            int i = 0;
                            statement.setInt( ++i, id );
                            statement.setTimestamp( ++i, new java.sql.Timestamp( sumEntry.getDate().getTime() ) );
                            statement.setString( ++i, sumColumnsEntry.getKey() );
                            statement.setDouble( ++i, sumEntry.getDaySum() );
                            statement.addBatch();
                        }
                    }
                    int[] count = statement.executeBatch();
                    LOG.info( formatString( "INSERT: Rows affected: {}", addArray( count ) ) );
                }
                finally
                {
                    if ( statement != null )
                    {
                        statement.close();
                    }
                }
                return null;
            }
        }.call();
    }

    public void updateFileAge( final File csvFile, final DateTime dateTime )
        throws SQLException, IOException
    {
        LOG.info( formatString( "Writing to DB file age for {}, installationEntry:  {}", csvFile, dateTime ) );

        final String updateQuery = "update dateien set datum = ? where dateiname = ?";
        final String insertQuery = "insert into dateien (datum) values (?) where dateiname = ?";
        new Transaction<Void>( pooledDatasource, autoCommit )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException
            {
                PreparedStatement statement = null;
                try
                {
                    try
                    {
                        // update
                        statement = connection.prepareStatement( updateQuery );
                        statement.setTimestamp( 1, new Timestamp( dateTime.toDate().getTime() ) );
                        statement.setString( 2, csvFile.toString() );
                        statement.executeUpdate();
                    }
                    catch ( SQLException e )
                    {
                        // insert
                        statement = connection.prepareStatement( insertQuery );
                        statement.setTimestamp( 1, new Timestamp( dateTime.toDate().getTime() ) );
                        statement.setString( 2, csvFile.toString() );
                        statement.executeUpdate();
                    }
                }
                finally
                {
                    if ( statement != null )
                    {
                        statement.close();
                    }
                }
                return null;
            }
        }.call();
    }

    public void updateMd5Sum( final File csvFile, final String newMd5Sum )
        throws SQLException, IOException
    {
        LOG.info( formatString( "Writing to DB MD5 sum for {}, installationEntry:  {}", csvFile, newMd5Sum ) );

        final String insertQuery = "insert into dateien (dateiname, md5sum, fertig) values (?, ?, ?)";
        final String updateQuery = "update dateien set md5sum = ? where dateiname = ?";
        new Transaction<Void>( pooledDatasource, autoCommit )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException, IOException
            {
                PreparedStatement statement = null;
                try
                {
                    // query table if we already have a md5sum saved for this filename
                    String oldMd5Sum = getMd5Sum( csvFile );
                    if ( oldMd5Sum == null )
                    {
                        // there is no md5sum saved for this filename, due to null not being allowed for md5sum by the
                        // db, this
                        // will be an insert
                        statement = connection.prepareStatement( insertQuery );
                        statement.setString( 1, csvFile.toString() );
                        statement.setString( 2, newMd5Sum );
                        statement.setBoolean( 3, false );
                    }
                    else
                    {
                        // there already is a record with this filename, we got a valid md5sum, so statement will be an
                        // update
                        statement = connection.prepareStatement( updateQuery );
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
                return null;
            }
        }.call();
    }

    public void markAsDone( final File csvFile )
        throws SQLException, IOException
    {
        LOG.info( formatString( "Writing to DB table {}, csvFile: {}", FILES_TABLE, csvFile ) );

        final String query = formatString( "update {} set fertig = ? where dateiname = ?", FILES_TABLE ).toString();
        new Transaction<Void>( pooledDatasource, autoCommit )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException
            {
                PreparedStatement stat = null;
                try
                {
                    stat = connection.prepareStatement( query );
                    stat.setBoolean( 1, true );
                    stat.setString( 2, csvFile.toString() );
                    stat.executeUpdate();
                }
                finally
                {
                    stat.close();
                }
                return null;
            }
        }.call();
    }

    /**
     * Check if a file is marked as done.
     * 
     * @param directory
     * @param csvFile
     * @return
     * @throws SQLException
     */
    public boolean isDone( final File csvFile )
        throws SQLException, IOException
    {
        final String selectQuery = formatString( "select fertig from {} where dateiname = ?", FILES_TABLE ).toString();
        return new Transaction<Boolean>( pooledDatasource, autoCommit )
        {
            @Override
            public Boolean action( Connection connection )
                throws SQLException
            {
                ResultSet rs = null;
                boolean isDone = false;
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( selectQuery );
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
        }.call();

    }

    /**
     * Reads md5sum from for a csvFile in a directory from db
     * 
     * @param directory
     * @param csvFile
     * @return
     * @throws SQLException
     */
    public String getMd5Sum( final File csvFile )
        throws SQLException, IOException
    {
        final String sqlQuery = formatString( "select md5sum from {} where dateiname = ?", FILES_TABLE ).toString();
        return new Transaction<String>( pooledDatasource, autoCommit )
        {
            @Override
            public String action( Connection connection )
                throws SQLException
            {
                ResultSet rs = null;
                String md5Sum = null;
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( sqlQuery );
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
        }.call();
    }

    public DateTime getFileAge( final File csvFile )
        throws SQLException, IOException
    {
        final String sqlQuery = formatString( "select datum from {} where dateiname = ?", FILES_TABLE ).toString();
        return new Transaction<DateTime>( pooledDatasource, autoCommit )
        {
            @Override
            public DateTime action( Connection connection )
                throws SQLException
            {
                ResultSet rs = null;
                Date fileAge = null;
                PreparedStatement statement = null;
                try
                {
                    statement = connection.prepareStatement( sqlQuery );
                    statement.setString( 1, csvFile.toString() );
                    rs = statement.executeQuery();
                    if ( rs.next() )
                    {
                        fileAge = rs.getDate( 1 );
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
                // new DateTime( null) == new DateTime() --> gt -36h
                return new DateTime( fileAge );
            }
        }.call();
    }

    public List<InstallationEntry> getInstallationEntries()
        throws SQLException, IOException
    {
        final String sqlQuery =
            formatString( "select id, verzeichnis, verbrauch_daysum_cols, verbrauch_sum_cols from {} where aktiv = 1",
                INSTALLATIONS_TABLE ).toString();
        return new Transaction<List<InstallationEntry>>( pooledDatasource, autoCommit )
        {
            @Override
            public List<InstallationEntry> action( Connection connection )
                throws SQLException
            {
                ArrayList<InstallationEntry> installations = new ArrayList<InstallationEntry>();
                PreparedStatement statement = null;
                ResultSet rs = null;
                try
                {
                    statement = connection.prepareStatement( sqlQuery );
                    rs = statement.executeQuery();
                    while ( rs.next() )
                    {
                        installations.add( new InstallationEntry( rs.getInt( 1 ), rs.getString( 2 ), rs.getString( 3 ),
                            rs.getString( 4 ) ) );
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
        }.call();
    }

    // ==========================================================================

    public String powerQueryDay( int installionId, DateTime date )
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

    public String yieldQueryYear( int installionId, DateTime date )
    {
        return formatString(
            "SELECT DATE_FORMAT(datum,'%M') as tag, daysum * 0.001 " + //
                "FROM {} " + //
                "WHERE anlage = {} " + //
                "AND datum BETWEEN DATE_SUB('{}', INTERVAL 365 DAY) AND '{}' AND typ = 'DaySum'",// +
            YIELD_TABLE, installionId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ),
            MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) ).toString();
    }

    public String yieldQueryWeek( int anlageId, DateTime date )
    {
        return formatString(
            "SELECT DATE_FORMAT(datum,'%W') as tag, daysum * 0.001 " + //
                "FROM {} " + //
                "WHERE anlage = {} " + //
                "AND datum BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND '{}' AND typ = 'DaySum'",// +
            YIELD_TABLE, anlageId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ),
            MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) ).toString();
    }

    public String yieldQueryMonth( int installationId, DateTime date )
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
