package net.rzaw.solar.csvimport;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import static net.rzaw.solar.StringFormatter.formatString;
import net.rzaw.solar.db.DatabaseProcessor;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Logger;
import org.h2.tools.Csv;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class CSVImporter
    implements Closeable
{
    public static final String FIELD_DATE = "field.date";

    public static final String FIELD_TIME = "field.time";

    public static final String FIELDS_DAYSUM = "fields.daysum";

    public static final String FIELDS_SUM = "fields.sum";

    protected static final Pattern COMMA_SEPARATOR_PARSER = Pattern.compile( "," );

    private static final Logger LOG = Logger.getLogger( CSVImporter.class );

    /*
     * date and time fields
     */
    private final String dateField;

    private final String timeField;

    /*
     * Fields to be summed up
     */
    protected final String[] sumFields;

    protected final String[] daysumFields;

    private final FilenameFilter csvFilter = new AndFileFilter( new WildcardFileFilter( "min*.csv" ),
        FileFileFilter.FILE );

    private final DatabaseProcessor databaseProcessor;

    public CSVImporter( Properties properties )
        throws SQLException
    {
        // populate fields
        dateField = properties.getProperty( FIELD_DATE );
        timeField = properties.getProperty( FIELD_TIME );

        String daysumFieldsString = properties.getProperty( FIELDS_DAYSUM );
        Preconditions.checkNotNull( daysumFieldsString );
        daysumFields = COMMA_SEPARATOR_PARSER.split( daysumFieldsString );

        String sumFieldsString = properties.getProperty( FIELDS_SUM );
        Preconditions.checkNotNull( sumFieldsString );
        sumFields = COMMA_SEPARATOR_PARSER.split( sumFieldsString );

        databaseProcessor = new DatabaseProcessor( properties );
    }

    /**
     * Process a directory of csv files that were uploaded.
     * 
     * @param installationEntry
     * @throws IOException
     * @throws SQLException
     */
    public void processDirectory( InstallationEntry installationEntry )
        throws IOException, SQLException
    {
        LOG.info( "Processing directory: " + installationEntry );
        // process all csv files in this directory
        for ( String csvFileName : installationEntry.getDirectory().list( csvFilter ) )
        {
            LOG.info( "File to be processed: " + csvFileName );

            File csvFile = new File( installationEntry.getDirectory(), csvFileName );
            if ( !databaseProcessor.isDone( csvFile ) )
            {
                // get old md5sum from database
                String oldMd5Sum = databaseProcessor.getMd5Sum( csvFile );
                // calculate current md5sum from file content
                String newMd5Sum = DigestUtils.md5Hex( Files.toString( csvFile, Charsets.UTF_8 ) );
                // TODO get from file or db.
                // DateTime age;
                if ( newMd5Sum.equals( oldMd5Sum )
                // && age.isAfter( new DateTime().minusHours( 36 ) )
                )
                {
                    // the file did not change from the last iteration, this was
                    // the last data set for today
                    // mark this file as stable/done
                    databaseProcessor.markAsDone( csvFile );
                    LOG.info( formatString( "Marked file {} as done, not processing again.",
                        csvFileName ) );
                }
                else
                {
                    // we don't have it, create a new record or file has changed
                    // @formatter:off
					/*
					 * -------------------------
					 * PAC | 1.2.2011 23:55 | 0
					 * ...
					 * PAC | 1.2.2011 12:00 | 25
					 * ...
					 * PAC | 1.2.2011 00:00 | 0
					 * --------------------------
					 * SolIrr | 1.2.2011 23:55 | 0
					 * ...
					 * SolIrr | 1.2.2011 12:00 | 25
					 * ...
					 * SolIrr | 1.2.2011 00:00 | 0
					 * --------------------------
					 */
					// @formatter:on
                    // process sums over all fields of the same name
                    Map<String, List<SumEntry>> sumsColumns =
                        calculateSumSameColumns( csvFile, sumFields, installationEntry.getConsumptionSumCols() );
                    databaseProcessor.updateSumSameColumns( installationEntry.getDirectory().toString(),
                        DatabaseProcessor.POWERS_TABLE, sumsColumns );

                    // calculate the greatest daySum
                    Map<String, SumEntry> daySums =
                        calculateDaySum( csvFile, daysumFields, installationEntry.getConsumptionDaySumCols() );
                    databaseProcessor.updateDaySumColumns( installationEntry.getDirectory().toString(),
                        DatabaseProcessor.YIELD_TABLE, daySums );

                    // process consumption
                    Map<String, List<SumEntry>> consumptionSumsColumns =
                        calculateConsumptionSumSameColumns( csvFile, installationEntry.getConsumptionSumCols() );
                    databaseProcessor.updateSumSameColumns( installationEntry.getDirectory().toString(),
                        DatabaseProcessor.POWERS_CONSUMPTION_TABLE, consumptionSumsColumns );

                    Map<String, SumEntry> consumptionDaySums =
                        calculateConsumptionDaySum( csvFile, installationEntry.getConsumptionDaySumCols() );
                    databaseProcessor.updateDaySumColumns( installationEntry.getDirectory().toString(),
                        DatabaseProcessor.CONSUMPTION_TABLE, consumptionDaySums );

                    // or file has changed, update md5sum
                    databaseProcessor.updateMd5Sum( csvFile, newMd5Sum );
                }
            }
            else
            {
                LOG.info( formatString( "file {} is already done, not processing again.", csvFileName ) );
            }
        }
    }

    private Map<String, SumEntry> calculateConsumptionDaySum( File csvFile, List<Integer> consumptionDaySumCols )
        throws SQLException
    {
        LOG.debug( "Calculating consumption day sum." );
        // csv reader
        Csv csvReader = Csv.getInstance();
        csvReader.setFieldSeparatorRead( ';' );

        ResultSet csvData = csvReader.read( csvFile.getPath(), null, null );
        ResultSetMetaData csvFileHeader = csvData.getMetaData();

        return calcSumFirstLine( csvData, processHeaders( csvFileHeader, consumptionDaySumCols ) );
    }

    private Map<String, List<SumEntry>> calculateConsumptionSumSameColumns( File csvFile,
                                                                            List<Integer> consumptionSumCols )
        throws SQLException
    {
        LOG.debug( "Calculating consumption sum same columns." );
        // csv reader
        Csv csvReader = Csv.getInstance();
        csvReader.setFieldSeparatorRead( ';' );

        ResultSet csvData = csvReader.read( csvFile.getPath(), null, null );
        ResultSetMetaData csvFileHeader = csvData.getMetaData();

        // powers/Pac/SolIrr
        return calcSumOverall( csvData, processHeaders( csvFileHeader, consumptionSumCols ) );
    }

    private Map<String, List<Integer>> processHeaders( ResultSetMetaData csvFileHeader, List<Integer> consumptionCols )
        throws SQLException
    {
        LOG.info( "Processing headers. consumptionCols:" + consumptionCols );
        // process CSV header line (1st line)
        Map<String, List<Integer>> columns = Maps.newHashMap();
        for ( int consumptionCol : consumptionCols )
        {
            String headerColumnName =
                csvFileHeader.getColumnName( consumptionCol ).toLowerCase().replaceAll( "[0-9]", "" );
            // get all field indexes with the same name
            List<Integer> fieldsSameNameList = columns.get( headerColumnName );
            if ( fieldsSameNameList == null )
            {
                fieldsSameNameList = Lists.newArrayList();
                columns.put( headerColumnName, fieldsSameNameList );
            }
            fieldsSameNameList.add( consumptionCol );
        }
        return columns;
    }

    /**
     * Process header line of CSV file, identify columns and find the columns we are looking for (like PAC, daysum,
     * etc.)
     * 
     * @param csvFileHeader
     * @param fields
     * @return
     * @throws SQLException
     */
    private Map<String, List<Integer>> processHeaders( ResultSetMetaData csvFileHeader, String[] fields,
                                                       List<Integer> consumptionCols )
        throws SQLException
    {
        LOG.info( formatString( "Processing headers. consumptionCols: {}, fields: {}", consumptionCols,
            Arrays.toString( fields ) ) );
        // process CSV header line (1st line)
        Map<String, List<Integer>> columns = Maps.newHashMap();
        for ( int i = 1; i < csvFileHeader.getColumnCount() + 1; i++ )
        {
            // process all fields where the greatest value should be found
            for ( String field : fields )
            {
                String headerColumnName = csvFileHeader.getColumnName( i ).toLowerCase().replaceAll( "[0-9]", "" );
                if ( headerColumnName.equals( field.toLowerCase() ) )
                {
                    // get all field indexes with the same name
                    List<Integer> fieldsSameNameList = columns.get( field );
                    if ( fieldsSameNameList == null )
                    {
                        fieldsSameNameList = Lists.newArrayList();
                        columns.put( field, fieldsSameNameList );
                    }
                    if ( !consumptionCols.contains( i ) )
                    {
                        fieldsSameNameList.add( i );
                    }
                }
            }
        }
        return columns;
    }

    /**
     * Process fields whose maximum value should be determined. So this processes only the top line of DaySum and
     * DaySumIrr fields
     * 
     * @param csvFile
     * @return
     * @throws SQLException
     */
    protected Map<String, SumEntry> calculateDaySum( File csvFile, String[] daysumFields,
                                                     List<Integer> consumptionDaySumCols )
        throws SQLException
    {
        LOG.debug( "Calculating day sum." );

        // csv reader
        Csv csvReader = Csv.getInstance();
        csvReader.setFieldSeparatorRead( ';' );

        ResultSet csvData = csvReader.read( csvFile.getPath(), null, null );
        ResultSetMetaData csvFileHeader = csvData.getMetaData();

        return calcSumFirstLine( csvData, processHeaders( csvFileHeader, daysumFields, consumptionDaySumCols ) );
    }

    private Map<String, SumEntry> calcSumFirstLine( ResultSet csvData, Map<String, List<Integer>> daySumColumns )
        throws SQLException
    {
        LOG.debug( "Calculating first line." );
        Map<String, SumEntry> daySums = Maps.newHashMap();
        // just process the first line
        if ( csvData.next() )
        {
            String dateString = csvData.getString( dateField );
            String timeString = csvData.getString( timeField );

            // calculate date from both dates
            Date dateTime =
                DatabaseProcessor.MYSQL_DATETIME_FORMAT.parseLocalDateTime( dateString + " " + timeString ).toDate();

            // process all columns for which we would like to find the maximum value
            // example: DaySum [3, 5, 7]
            for ( Entry<String, List<Integer>> columnSameNamesEntry : daySumColumns.entrySet() )
            {
                double sum = 0;
                // finally the calculation: add up all fields with the same name using the indexes
                for ( int fieldIndex : columnSameNamesEntry.getValue() )
                {
                    // get single value
                    double singleValue = csvData.getDouble( fieldIndex );
                    // add it to sum
                    sum += singleValue;
                }
                // put it into map
                SumEntry value = new SumEntry( dateTime, sum );
                daySums.put( columnSameNamesEntry.getKey(), value );
            }
        }
        return daySums;
    }

    /**
     * Sums up all columns with same names into one sum per line. So for every timestamp we have one PAC value that is
     * the sum of all PAC columns that were in that line. With this you can create a chart for the per-timestamp
     * performance of a device.
     * 
     * @param csvFile
     * @return
     * @throws SQLException
     */
    protected Map<String, List<SumEntry>> calculateSumSameColumns( File csvFile, String[] sumFields,
                                                                   List<Integer> consumptionSumCols )
        throws SQLException
    {
        LOG.debug( "Calculating day sum same columns." );

        // csv reader
        Csv csvReader = Csv.getInstance();
        csvReader.setFieldSeparatorRead( ';' );

        ResultSet csvData = csvReader.read( csvFile.getPath(), null, null );
        ResultSetMetaData csvFileHeader = csvData.getMetaData();

        // powers/Pac/SolIrr
        return calcSumOverall( csvData, processHeaders( csvFileHeader, sumFields, consumptionSumCols ) );
    }

    private Map<String, List<SumEntry>> calcSumOverall( ResultSet csvData, Map<String, List<Integer>> sumColumns )
        throws SQLException
    {
        LOG.debug( "Calculating overall sum." );
        Map<String, List<SumEntry>> datesSumSameColumn = Maps.newHashMap();

        // process all lines
        while ( csvData.next() )
        {
            String dateString = csvData.getString( dateField );
            String timeString = csvData.getString( timeField );

            // calculate date from both dates
            Date dateTime =
                DatabaseProcessor.MYSQL_DATETIME_FORMAT.parseLocalDateTime( dateString + " " + timeString ).toDate();

            // sumField == Pac || sumField == SolIrr || ...
            for ( Entry<String, List<Integer>> columnSameNamesEntry : sumColumns.entrySet() )
            {
                double sum = 0;
                // finally the calculation: add up all fields with the same name using the indexes
                // go horizontally through a CSV line and add up all fields with the same name
                for ( int fieldIndex : columnSameNamesEntry.getValue() )
                {
                    // get single value
                    double singleValue = csvData.getDouble( fieldIndex );
                    // add it to sum
                    sum += singleValue;
                }
                // put it into map
                SumEntry lineValue = new SumEntry( dateTime, sum );

                List<SumEntry> sumSameColumn = datesSumSameColumn.get( columnSameNamesEntry.getKey() );
                if ( sumSameColumn == null )
                {
                    sumSameColumn = Lists.newArrayList();
                }
                sumSameColumn.add( lineValue );
                datesSumSameColumn.put( columnSameNamesEntry.getKey(), sumSameColumn );
            }
        }
        return datesSumSameColumn;
    }

    @Override
    public void close()
    {
        databaseProcessor.close();
    }

    public ArrayList<InstallationEntry> getInstallationEntries()
        throws SQLException
    {
        return databaseProcessor.getInstallationEntries();
    }
}
