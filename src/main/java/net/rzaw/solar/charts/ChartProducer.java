package net.rzaw.solar.charts;

import static net.rzaw.solar.StringFormatter.formatString;

import java.awt.Color;
import java.awt.GradientPaint;
import java.awt.image.BufferedImage;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import javax.imageio.ImageIO;

import net.rzaw.solar.csvimport.InstallationEntry;
import net.rzaw.solar.db.DatabaseProcessor;

import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
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

    public void render( DateTime dateTime )
        throws SQLException, IOException
    {
        // iterate over active installations
        for ( InstallationEntry installationEntry : databaseProcessor.getInstallationEntries() )
        {
            createBarChartTimeDataset( databaseProcessor.getPowerQueryDay( 1 ), "Leistung", "Tag",
                getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );

            createCategoryDataset( databaseProcessor.getYieldQueryMonth( installationEntry.getId(), dateTime ),
                "Ertrag", "Monat", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );

            createCategoryDataset( databaseProcessor.getYieldQueryWeek( installationEntry.getId(), dateTime ),
                "Ertrag", "Woche", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );

            createCategoryDataset( databaseProcessor.getYieldQueryYear( installationEntry.getId(), dateTime ),
                "Ertrag", "Jahr", getChartsSubdirectory( installationEntry.getDirectory() ), dateTime );
        }
    }

    private void createCategoryDataset( String query, String name, String timeFrame, File parentDir, DateTime dateTime )
        throws SQLException, IOException
    {
        RzawJDBCCategoryDataset dataset = new RzawJDBCCategoryDataset( databaseProcessor.getConnection() );
        LOG.debug( "Chart query: " + query );
        dataset.executeQuery( query );
        // create the chart...
        JFreeChart chart =
            ChartFactory.createBarChart( name, "", "Leistung in kWh", dataset, PlotOrientation.VERTICAL, false, false,
                false );

        // set the background color for the chart
        chart.setBackgroundPaint( BACKGROUND_COLOR );

        // get a reference to the plot for further customisation
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setBackgroundPaint( PLOT_BACKGROUND_COLOR );

        // set the range axis to display integers only
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

        // disable bar outlines
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setDrawBarOutline( false );

        renderer.setSeriesPaint( 0, SERIES_COLOR );
        // renderer.setBasePaint( PLOT_BACKGROUND_COLOR );
        renderer.setBaseSeriesVisible( true );
        renderer.setSeriesFillPaint( 0, SERIES_COLOR );
        // renderer.setBaseFillPaint( PLOT_BACKGROUND_COLOR );

        // retrieve image
        BufferedImage bi = chart.createBufferedImage( IMAGE_LENGTH, IMAGE_HEIGHT );
        File outputFile = new File( parentDir, fileName( name, timeFrame, dateTime ) );
        ImageIO.write( bi, FILE_FORMAT, outputFile );
    }

    private String fileName( String name, String timeFrame, DateTime dateTime )
    {
        return formatString( "{}_{}_{}.{}", name, timeFrame, DatabaseProcessor.MYSQL_DATE_FORMAT.print( dateTime ),
            FILE_FORMAT ).toString();
    }

    @Deprecated
    private void createBarChartDataset( String query, String name, File parentDir )
        throws SQLException, IOException
    {
        RzawJDBCXYDataset dataset = new RzawJDBCXYDataset( databaseProcessor.getConnection() );
        LOG.debug( "Chart query: " + query );
        dataset.executeQuery( query );
        XYBarDataset xyBarDataSet = new XYBarDataset( dataset, 1 );
        // create the chart...
        JFreeChart chart =
            ChartFactory.createXYBarChart( name, "Monat", false, "Leistung", xyBarDataSet, PlotOrientation.VERTICAL,
                false, false, false );

        // set the background color for the chart...
        // hsl(211, 92%, 36%)
        // #075BB2
        // rgb(7, 91, 178)
        // Paint p = new GradientPaint( 0, 480, new Color( 7, 91, 178 ), 0, 0, Color.white );
        chart.setBackgroundPaint( new Color( 255, 255, 255, 0 ) );
        chart.setBackgroundImageAlpha( 0.0f );

        // get a reference to the plot for fselect MONTH(datum), sum(daysum) from ertrag where datum > (SELECT
        // TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1000 day)))urther customisation...
        XYPlot plot = (XYPlot) chart.getPlot();

        // NumberAxis axis = (NumberAxis) plot.getDomainAxis();
        // axis.setDateFormatOverride( new SimpleDateFormat( "MMM-yyyy" ) );

        // set the range axis to display integers only...
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

        // disable bar outlines...
        XYBarRenderer renderer = (XYBarRenderer) plot.getRenderer();
        // renderer.setDrawBarOutline( false );

        // // set up gradient paints for series...
        GradientPaint gp0 =
            new GradientPaint( 0.0f, 0.0f, new Color( 188, 224, 46 ), 0.0f, 0.0f, new Color( 0, 0, 64 ) );
        renderer.setSeriesPaint( 0, new Color( 188, 224, 46 ) );
        // CategoryAxis domainAxis = plot.getDomainAxis();
        // domainAxis.setCategoryLabelPositions( CategoryLabelPositions.createUpRotationLabelPositions( Math.PI / 6.0 )
        // );
        // OPTIONAL CUSTOMISATION COMPLETED.
        // retrieve image
        BufferedImage bi = chart.createBufferedImage( 670, 380 );

        File outputFile = new File( parentDir, name + ".png" );
        ImageIO.write( bi, FILE_FORMAT, outputFile );
    }

    private void createBarChartTimeDataset( String query, String name, String timeFrame, File parentDir,
                                            DateTime dateTime )
        throws SQLException, IOException
    {
        RzawJDBCXYDataset dataset = new RzawJDBCXYDataset( databaseProcessor.getConnection() );
        LOG.debug( "Chart query: " + query );
        dataset.executeQuery( query );
        XYBarDataset xyBarDataSet = new XYBarDataset( dataset, 100 );
        // create the chart...
        JFreeChart chart =
            ChartFactory.createXYBarChart( name, "", true, "Leistung in kWh", xyBarDataSet, PlotOrientation.VERTICAL,
                false, false, false );

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
    }

    @Deprecated
    private void createLineChart( String query, String name, File parentDir )
        throws SQLException, IOException
    {
        RzawJDBCXYDataset dataset = new RzawJDBCXYDataset( databaseProcessor.getConnection() );
        LOG.debug( "Chart query: " + query );
        dataset.executeQuery( query );
        // create the chart...
        JFreeChart chart =
            ChartFactory.createTimeSeriesChart( name, "Uhrzeit", "Leistung in kWh", dataset, false, false, false );

        // set the background color for the chart...
        // hsl(211, 92%, 36%)
        // #075BB2
        // rgb(7, 91, 178)
        // Paint p = new GradientPaint( 0, 480, new Color( 7, 91, 178 ), 0, 0, Color.white );

        chart.setBackgroundPaint( new Color( 255, 255, 255, 128 ) );
        chart.setBackgroundImageAlpha( 0.5f );

        XYPlot plot = (XYPlot) chart.getPlot();

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride( new SimpleDateFormat( "HH:mm:ss" ) );

        // set the range axis to display integers only...
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

        // disable bar outlines...
        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();

        // // set up gradient paints for series...
        Color seriesColor = new Color( 188, 224, 46 );
        // GradientPaint gp0 =
        // new GradientPaint( 0.0f, 0.0f, new Color( 188, 224, 46 ), 0.0f, 0.0f, new Color( 0, 0, 64 ) );
        renderer.setSeriesPaint( 0, seriesColor );
        renderer.setBasePaint( PLOT_BACKGROUND_COLOR );
        renderer.setBaseSeriesVisible( true );
        renderer.setSeriesFillPaint( 0, seriesColor );

        renderer.setBaseFillPaint( PLOT_BACKGROUND_COLOR );
        // retrieve image
        BufferedImage bi = chart.createBufferedImage( 670, 380 );

        File outputFile = new File( parentDir, name + ".png" );
        ImageIO.write( bi, FILE_FORMAT, outputFile );
    }

    public ChartProducer( Properties properties )
        throws SQLException
    {
        databaseProcessor = new DatabaseProcessor( properties, false );
    }

    @Override
    public void close()
    {
        databaseProcessor.close();
    }
}
