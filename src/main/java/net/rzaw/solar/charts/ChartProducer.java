package net.rzaw.solar.charts;

import static net.rzaw.solar.StringFormatter.formatString;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import javax.imageio.ImageIO;

import net.rzaw.solar.csvimport.InstallationEntry;
import net.rzaw.solar.db.DatabaseProcessor;
import net.rzaw.solar.db.Transaction;

import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.xy.XYBarDataset;
import org.joda.time.DateTime;

public class ChartProducer
    implements Closeable
{
    private static final String FILE_FORMAT = "png";

    // grey, 50% transparency
    private static final Color PLOT_BACKGROUND_COLOR = new Color( 169, 169, 169, 127 );

    // white, 50% transparency
    private static final Color BACKGROUND_COLOR = new Color( 255, 255, 255, 127 );

    // hsl(211, 92%, 36%)
    // #075BB2
    // rgb(7, 91, 178)
    // neon-green
    private static final Color SERIES_COLOR = new Color( 188, 224, 46 );

    private static final int IMAGE_LENGTH = 670;

    private static final int IMAGE_HEIGHT = 380;

    private static final Logger LOG = Logger.getLogger( ChartProducer.class );

    private static final String CHARTSSUB_DIRECTORY = "display";

    private final DatabaseProcessor databaseProcessor;

    private File getChartsSubdirectory( File parentDir )
    {
        File subdir = new File( parentDir, CHARTSSUB_DIRECTORY );
        subdir.mkdirs();
        return subdir;
    }

    public void render( InstallationEntry installationEntry, DateTime dateTime )
        throws SQLException, IOException
    {
        LOG.info( formatString( "Creating charts in directory {}", installationEntry.getDirectory() ) );

        createBarChartTimeDataset( databaseProcessor.powerQueryDay( installationEntry.getId(), dateTime ), "Leistung",
            "Tag", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );

        createCategoryDataset( databaseProcessor.yieldQueryMonth( installationEntry.getId(), dateTime ), "Ertrag",
            "Monat", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );

        createCategoryDataset( databaseProcessor.yieldQueryWeek( installationEntry.getId(), dateTime ), "Ertrag",
            "Woche", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );

        createCategoryDataset( databaseProcessor.yieldQueryYear( installationEntry.getId(), dateTime ), "Ertrag",
            "Jahr", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );
    }

    private void createCategoryDataset( final String query, final String name, final String timeFrame,
                                        final File parentDir, final DateTime dateTime )
        throws SQLException, IOException
    {
        new Transaction<Void>( databaseProcessor.getPooledDatasource(), true )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException, IOException
            {
                RzawJDBCCategoryDataset dataset = new RzawJDBCCategoryDataset( connection );
                LOG.debug( "Chart query: " + query );
                dataset.executeQuery( query );
                // create the chart...
                JFreeChart chart =
                    ChartFactory.createBarChart( "", "", "kWh", dataset, PlotOrientation.VERTICAL, false, false, false );

                // set the background color for the chart
                chart.setBackgroundPaint( BACKGROUND_COLOR );

                // get a reference to the plot for further customisation
                CategoryPlot plot = (CategoryPlot) chart.getPlot();
                plot.setBackgroundPaint( PLOT_BACKGROUND_COLOR );

                // rotate labels for jahr, otherwise won't fit
                if ( timeFrame.equals( "Jahr" ) )
                {
                    NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
                    rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );
                    CategoryAxis xAxis = plot.getDomainAxis();
                    xAxis.setCategoryLabelPositions( CategoryLabelPositions.UP_45 );
                }

                // disable bar outlines
                BarRenderer renderer = (BarRenderer) plot.getRenderer();
                renderer.setDrawBarOutline( false );

                renderer.setSeriesPaint( 0, SERIES_COLOR );
                renderer.setBaseSeriesVisible( true );
                renderer.setSeriesFillPaint( 0, SERIES_COLOR );

                // retrieve image
                BufferedImage bi = chart.createBufferedImage( IMAGE_LENGTH, IMAGE_HEIGHT );
                File outputFile = new File( parentDir, fileName( name, timeFrame, dateTime ) );
                ImageIO.write( bi, FILE_FORMAT, outputFile );
                return null;
            }
        }.call();
    }

    private String fileName( String name, String timeFrame, DateTime dateTime )
    {
        return formatString( "{}_{}_{}.{}", name, timeFrame,
            DatabaseProcessor.MYSQL_INTERNAL_DATE_FORMAT.print( dateTime ), FILE_FORMAT ).toString();
    }

    private void createBarChartTimeDataset( final String query, final String name, final String timeFrame,
                                            final File parentDir, final DateTime dateTime )
        throws SQLException, IOException
    {
        new Transaction<Void>( databaseProcessor.getPooledDatasource(), true )
        {
            @Override
            public Void action( Connection connection )
                throws SQLException, IOException
            {
                RzawJDBCXYDataset dataset = new RzawJDBCXYDataset( connection );
                LOG.debug( "Chart query: " + query );
                dataset.executeQuery( query );
                XYBarDataset xyBarDataSet = new XYBarDataset( dataset, 100 );
                // create the chart...
                JFreeChart chart =
                    ChartFactory.createXYBarChart( "", "", true, "kW", xyBarDataSet, PlotOrientation.VERTICAL, false,
                        false, false );

                // set the background color for the chart
                chart.setBackgroundPaint( BACKGROUND_COLOR );

                // get a reference to the plot for further customisation
                XYPlot plot = (XYPlot) chart.getPlot();
                plot.setBackgroundPaint( PLOT_BACKGROUND_COLOR );

                // set the range axis to display dates
                DateAxis axis = (DateAxis) plot.getDomainAxis();
                axis.setDateFormatOverride( new SimpleDateFormat( "HH:mm" ) );

                // set the range axis to display integers only...
                // NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
                // rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

                // disable bar outlines...
                XYBarRenderer renderer = (XYBarRenderer) plot.getRenderer();
                renderer.setDrawBarOutline( false );

                // // set up gradient paints for series...
                renderer.setSeriesPaint( 0, SERIES_COLOR );

                // retrieve image
                BufferedImage bi = chart.createBufferedImage( IMAGE_LENGTH, IMAGE_HEIGHT );
                File outputFile = new File( parentDir, fileName( name, timeFrame, dateTime ) );
                ImageIO.write( bi, FILE_FORMAT, outputFile );
                return null;
            }
        }.call();
    }

    public ChartProducer( Properties properties )
        throws SQLException, IOException
    {
        databaseProcessor = new DatabaseProcessor( properties, false );
    }

    @Override
    public void close()
    {
        databaseProcessor.close();
    }
}
