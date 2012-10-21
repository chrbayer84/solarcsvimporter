package net.rzaw.solar.csvimport;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

public class _CSVImporter
{
    private static String[] testFiles = { "min110527.csv", "min110901.csv", "minEmpty.csv", "ignore.csv",
        "min120209.csv" };

    private CSVImporter csvImporter;

    private Map<String, File> filesFromClassPath;

    @Before
    public void setup()
        throws SQLException, IOException, ClassNotFoundException
    {
        Properties properties = new Properties();
        String filename = "config.properties";
        final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream( filename );
        Preconditions.checkNotNull( stream );
        InputSupplier<InputStream> configFileInputStreamSupplier = new InputSupplier<InputStream>()
        {
            @Override
            public InputStream getInput()
                throws IOException
            {
                return stream;
            }
        };

        properties.load( configFileInputStreamSupplier.getInput() );
        csvImporter = new CSVImporter( properties );

        File path = new File( "./target/" );
        filesFromClassPath = loadFilesFromClassPath( testFiles, path );
    }

    private Map<String, File> loadFilesFromClassPath( String expectedFileNames[], File path )
        throws IOException
    {
        Map<String, File> expectedFiles = new HashMap<String, File>();
        for ( String filename : expectedFileNames )
        {
            // pulls data from core.reporting package
            path.mkdirs();
            File to = new File( path, filename );
            final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream( filename );
            Preconditions.checkNotNull( stream );
            Files.copy( new InputSupplier<InputStream>()
            {
                @Override
                public InputStream getInput()
                    throws IOException
                {
                    return stream;
                }
            }, to );
            expectedFiles.put( filename, to );
        }
        return expectedFiles;
    }

    @Test
    public void _testCalculateDaySumSol()
        throws SQLException, IOException
    {
        DateTimeFormatter fmt = DateTimeFormat.forPattern( "dd.MM.YYYY HH:mm:ss" );
        Date dateTime = fmt.parseDateTime( "27.05.2011 22:55:00" ).toDate();

        Map<String, SumEntry> expectedSol = Maps.newHashMap();
        expectedSol.put( "DaySum", new SumEntry( dateTime, 1228000.0 ) );
        expectedSol.put( "DaySumIrr", new SumEntry( dateTime, 3587.0 ) );

        Map<String, SumEntry> calculateDaySumSol =
            csvImporter.calculateDaySum( filesFromClassPath.get( testFiles[0] ), csvImporter.daysumFields,
                new ArrayList<Integer>() );
        Assert.assertTrue( expectedSol.equals( calculateDaySumSol ) );
    }

    @Test
    public void _testCalculateDaySumWind()
        throws SQLException, IOException
    {
        DateTimeFormatter fmt = DateTimeFormat.forPattern( "dd.MM.YYYY HH:mm:ss" );
        Date dateTime = fmt.parseDateTime( "01.09.2011 20:10:00" ).toDate();
        Map<String, SumEntry> expectedWind = Maps.newHashMap();
        expectedWind.put( "DaySum", new SumEntry( dateTime, 1719175.0 ) );
        expectedWind.put( "DaySumIrr", new SumEntry( dateTime, 0.0 ) );

        Map<String, SumEntry> calculateDaySumWind =
            csvImporter.calculateDaySum( filesFromClassPath.get( testFiles[1] ), csvImporter.daysumFields,
                new ArrayList<Integer>() );
        Assert.assertTrue( expectedWind.equals( calculateDaySumWind ) );
    }

    @Test
    public void _testCalculateDaySumSameColumnsWind()
        throws SQLException, IOException
    {
        DateTimeFormatter fmt = DateTimeFormat.forPattern( "dd.MM.YYYY HH:mm:ss" );

        Map<String, List<SumEntry>> calculateDaySumSol =
            csvImporter.calculateSumSameColumns( filesFromClassPath.get( testFiles[1] ), csvImporter.sumFields,
                new ArrayList<Integer>() );

        // probe 3 examples
        List<SumEntry> expectedList = Lists.newArrayList();
        // Pac
        Date dateTime = fmt.parseDateTime( "01.09.2011 16:00:00" ).toDate();
        SumEntry expected = new SumEntry( dateTime, 178303.0 );
        expectedList.add( expected );

        dateTime = fmt.parseDateTime( "01.09.2011 16:10:00" ).toDate();
        expected = new SumEntry( dateTime, 169650.0 );
        expectedList.add( expected );

        dateTime = fmt.parseDateTime( "01.09.2011 16:20:00" ).toDate();
        expected = new SumEntry( dateTime, 190395.0 );
        expectedList.add( expected );

        Assert.assertTrue( calculateDaySumSol.get( "Pac" ).containsAll( expectedList ) );
        // SolIrr
        dateTime = fmt.parseDateTime( "01.09.2011 16:20:00" ).toDate();
        expected = new SumEntry( dateTime, 0.0 );
        expectedList.add( expected );
        Assert.assertTrue( calculateDaySumSol.get( "SolIrr" ).contains( expected ) );
    }

    @Test
    public void _testCalculateDaySumSameColumnsIgnore()
        throws SQLException, IOException
    {
        DateTimeFormatter fmt = DateTimeFormat.forPattern( "dd.MM.YYYY HH:mm:ss" );
        // sum
        List<Integer> sumConsumptionCols = Lists.newArrayList();
        sumConsumptionCols.add( 4 );

        Map<String, List<SumEntry>> calculateSum =
            csvImporter.calculateSumSameColumns( filesFromClassPath.get( testFiles[4] ), csvImporter.sumFields,
                sumConsumptionCols );

        // probe examples
        // Pac
        Date dateTime = fmt.parseDateTime( "09.02.2012 14:30:00" ).toDate();
        SumEntry expected = new SumEntry( dateTime, 151.0 );

        Assert.assertTrue( calculateSum.get( "Pac" ).contains( expected ) );

        // daySum
        List<Integer> daysumConsumptionCols = Lists.newArrayList();
        daysumConsumptionCols.add( 5 );

        Map<String, SumEntry> expectedDaySum = Maps.newHashMap();
        dateTime = fmt.parseDateTime( "09.02.2012 23:55:00" ).toDate();
        expectedDaySum.put( "DaySum", new SumEntry( dateTime, 340.0 ) );
        expectedDaySum.put( "DaySumIrr", new SumEntry( dateTime, 2686.0 ) );

        Map<String, SumEntry> calculateDaySum =
            csvImporter.calculateDaySum( filesFromClassPath.get( testFiles[4] ), csvImporter.daysumFields,
                daysumConsumptionCols );
        Assert.assertTrue( expectedDaySum.equals( calculateDaySum ) );
    }

    @Test
    public void _testEmptyFiles()
        throws SQLException, IOException
    {
        // 1st file
        Map<String, SumEntry> calculateDaySumWind =
            csvImporter.calculateDaySum( filesFromClassPath.get( testFiles[2] ), csvImporter.daysumFields,
                new ArrayList<Integer>() );
        Assert.assertTrue( calculateDaySumWind.isEmpty() );

        Map<String, List<SumEntry>> calculateSumWind =
            csvImporter.calculateSumSameColumns( filesFromClassPath.get( testFiles[2] ), csvImporter.daysumFields,
                new ArrayList<Integer>() );
        Assert.assertTrue( calculateSumWind.isEmpty() );

        // 2nd file
        calculateDaySumWind =
            csvImporter.calculateDaySum( filesFromClassPath.get( testFiles[3] ), csvImporter.daysumFields,
                new ArrayList<Integer>() );
        Assert.assertTrue( calculateDaySumWind.isEmpty() );

        calculateSumWind =
            csvImporter.calculateSumSameColumns( filesFromClassPath.get( testFiles[3] ), csvImporter.daysumFields,
                new ArrayList<Integer>() );
        Assert.assertTrue( calculateSumWind.isEmpty() );
    }
}
