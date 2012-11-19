package net.rzaw.solar.db;

import static net.rzaw.solar.StringFormatter.formatString;

import java.io.Closeable;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import net.rzaw.solar.StringFormatter;
import net.rzaw.solar.csvimport.CSVImporter;
import net.rzaw.solar.csvimport.InstallationEntry;
import net.rzaw.solar.csvimport.SumEntry;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;

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

    private final Connection connection;

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
            Class.forName( "com.mysql.jdbc.Driver" );
            String connectionString =
                formatString( "jdbc:mysql://{}:{}/{}?user={}&password={}", server, port, database, userName, password )
                    .toString();
            LOG.debug( formatString( "Use connection string: {}", connectionString ) );
            connection = DriverManager.getConnection( connectionString );
            connection.setAutoCommit( autoCommit );
        }
        catch ( ClassNotFoundException e )
        {
            close();
            throw new SQLException( e );
        }
        catch ( SQLException e )
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
    {
        return connection;
    }

    @Override
    public void close()
    {
        if ( connection != null )
        {
            try
            {
                connection.close();
            }
            catch ( SQLException e )
            {
                LOG.error( "Error on closing: ", e );
            }
        }
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
            statement = connection.prepareStatement( builder.toString() );
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
            statement = connection.prepareStatement( builder.toString() );
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
                statement = connection.prepareStatement( insertQuery );
                statement.setString( 1, csvFile.toString() );
                statement.setString( 2, newMd5Sum );
                statement.setBoolean( 3, false );
            }
            else
            {
                // there already is a record with this filename, we got a valid md5sum, so statement will be an update
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
    }

    public void markAsDone( File csvFile )
        throws SQLException
    {
        LOG.info( formatString( "Writing to DB table {}, csvFile: {}", FILES_TABLE, csvFile ) );

        String query = formatString( "update {} set fertig = ? where dateiname = ?", FILES_TABLE ).toString();
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
            statement = connection.prepareStatement( sqlQuery );
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
    private static final String YIELD_CLAUSE = "select {}(datum), sum(daysum) from ";

    private static final String POWERS_CLAUSE = "select datum, leistung * 0.001 from ";

    private static final String CONSUMPTION_CLAUSE = "select MONTH(datum), sum(daysum) ";

    private static final String DATE_CLAUSE = "where datum > (SELECT TIMESTAMP(DATE_SUB(NOW(), INTERVAL {} day)))";

    // private static final String DATE_CLAUSE = "where datum > (SELECT TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1000 day)))";

    private static final String WHERE_YEAR_CLAUSE = "where YEAR(datum) > YEAR(NOW())-2 group by MONTH(datum)";

    private static final String WHERE_TYPE_CLAUSE = "and type = '{}'";

    private static final String MONTH_SUM_CLAUSE = "select sum(daysum) from ";

    // select MONTH(datum), sum(leistung) from leistung where YEAR(datum) > YEAR(NOW())-1 group by MONTH(datum);

    // select sum(daysum) from ertrag where datum > (SELECT TIMESTAMP('2011-04-26')) and datum < (SELECT
    // TIMESTAMP('2011-05-26'));
    private static final String MONTH_DATE_CLAUSE =
        "where datum > (SELECT TIMESTAMP(DATE_SUB(NOW(), INTERVAL {} day))) ";

    private String getYearClause( String clause )
    {
        return formatString( clause, "MONTH" ).toString();
    }

    private String getMonthClause( String clause )
    {
        return formatString( clause, "DATE" ).toString();
    }

    private String getWeekClause( String clause )
    {
        return formatString( clause, "DAYNAME" ).toString();
    }

    private String getDateClause( int day )
    {
        return formatString( DATE_CLAUSE, day ).toString();
    }

    public String getYieldQueryDay( int days )
    {
        return formatString( " {} {} {}", getYearClause( YIELD_CLAUSE ), YIELD_TABLE, getDateClause( days ) )
            .toString();
    }

    public String getPowerQueryDay( int days )
    {
        return formatString( "{} {} {}", POWERS_CLAUSE, POWERS_TABLE, getDateClause( days ) ).toString();
    }

    public String getPowersConsumptionQueryDay( int days )
    {
        return StringFormatter
            .formatString( "{} {} {}", POWERS_CLAUSE, POWERS_CONSUMPTION_TABLE, getDateClause( days ) ).toString();
    }

    public String getConsumptionQueryDay( int days )
    {
        return formatString( "{} {} {}", CONSUMPTION_CLAUSE, CONSUMPTION_TABLE, getDateClause( days ) ).toString();
    }

    // ==========================================================================

    public String getYieldQueryYear( int anlageId, DateTime date )
    {
        return formatString( "SELECT DATE_FORMAT(datum,'%M') as tag, daysum * 0.001 " +
        // ", DATE_FORMAT(datum,'%W') as tag " + //
            "FROM `ertrag` " + //
            "WHERE `anlage` = {} " + //
            "AND `datum` BETWEEN DATE_SUB('{}', INTERVAL 365 DAY) AND '{}' AND `typ` = 'DaySum'",// +
            anlageId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ), MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) )
            .toString();
    }

    public String getYieldQueryWeek( int anlageId, DateTime date )
    {
        return formatString( "SELECT DATE_FORMAT(datum,'%W') as tag, daysum * 0.001 " +
        // ", DATE_FORMAT(datum,'%W') as tag " + //
            "FROM `ertrag` " + //
            "WHERE `anlage` = {} " + //
            "AND `datum` BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND '{}' AND `typ` = 'DaySum'",// +
            anlageId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ), MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) )
            .toString();
    }

    public String getYieldQueryMonth( int anlageId, DateTime date )
    {
        return formatString( "SELECT DATE_FORMAT(datum,'%e') as tag, daysum * 0.001 " +
        // ", DATE_FORMAT(datum,'%e') as tag " + //
            "FROM `ertrag` " + //
            "WHERE `anlage` = {} " + //
            "AND `datum` BETWEEN DATE_SUB('{}', INTERVAL 31 DAY) AND '{}' AND `typ` = 'DaySum'",//
            anlageId, MYSQL_INTERNAL_DATETIME_FORMAT.print( date ), MYSQL_INTERNAL_DATETIME_FORMAT.print( date ) )
            .toString();
    }

    public String getPowerQueryDay( int anlageId, DateTime date )
    {
        // return formatString( "SELECT datum, leistung " +
        return formatString( "SELECT DATE_FORMAT(datum,'%T' as tag, leistung * 0.001 " +
        // ", DATE_FORMAT(datum,'%T') as uhrzeit " + //
            "FROM `leistung` WHERE `anlage` = {} " + //
            "AND `datum` BETWEEN '{} 05:30:00' AND '{} 21:30:00' " + //
            "AND `typ` = 'PAC' " + //
            "ORDER BY `leistung`.`datum` ASC",//
            anlageId, MYSQL_INTERNAL_DATE_FORMAT.print( date ), MYSQL_INTERNAL_DATE_FORMAT.print( date ) ).toString();
    }
}
