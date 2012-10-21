package net.rzaw.solar.charts;

import java.awt.Color;
import java.awt.GradientPaint;
import java.awt.Paint;
import java.awt.image.BufferedImage;
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
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.joda.time.DateTime;

public class ChartProducer
{
    private static final Logger LOG = Logger.getLogger( ChartProducer.class );

    private static final String CHARTSSUB_DIRECTORY = "display";

    private final DatabaseProcessor databaseProcessor;

    private File getChartsSubdirectory( File parentDir )
    {
        return new File( parentDir, CHARTSSUB_DIRECTORY );
    }

    public void render( DateTime dateTime )
        throws SQLException, IOException
    {
        // iterate over active installations
        for ( InstallationEntry installationEntry : databaseProcessor.getInstallationEntries() )
        {
            String name = "Ertrag_1000_2";
            createXYTimeDataset( databaseProcessor.getPowerQueryDay( 1000 ), name,
                getChartsSubdirectory( installationEntry.getDirectory() ) );

            name = "Ertrag_woche_" + DatabaseProcessor.MYSQL_DATE_FORMAT.print( dateTime );
            createXYTimeDataset( databaseProcessor.getYieldQueryWeek( installationEntry.getId(), dateTime ), name,
                getChartsSubdirectory( installationEntry.getDirectory() ) );

            name = "Monat_" + DatabaseProcessor.MYSQL_DATE_FORMAT.print( dateTime );
            createXYTimeDataset( databaseProcessor.getYieldQueryMonth( installationEntry.getId(), dateTime ), name,
                getChartsSubdirectory( installationEntry.getDirectory() ) );

            name = "Leistung_" + DatabaseProcessor.MYSQL_DATE_FORMAT.print( dateTime );
            createLineChart( databaseProcessor.getPowerQueryDay( installationEntry.getId(), dateTime ), name,
                getChartsSubdirectory( installationEntry.getDirectory() ) );
        }
    }

    protected void createCategoryDataset( String query, String name, File parentDir )
        throws SQLException, IOException
    {
        RzawJDBCCategoryDataset dataset = new RzawJDBCCategoryDataset( databaseProcessor.getConnection() );
        LOG.debug( "Chart query: " + query );
        dataset.executeQuery( query );
        // create the chart...
        JFreeChart chart =
        // ChartFactory.createTimeSeriesChart( name, "Monat", "Leistung", dataset, false, false, false );
            ChartFactory.createBarChart( name, "Monat", "Leistung", dataset, PlotOrientation.VERTICAL, true, false,
                false );

        // set the background color for the chart...
        chart.setBackgroundImageAlpha( 1.0f );

        // get a reference to the plot for further customisation...
        CategoryPlot plot = (CategoryPlot) chart.getPlot();

        // set the range axis to display integers only...
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

        // disable bar outlines...
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setDrawBarOutline( false );

        // // set up gradient paints for series...
        // GradientPaint gp0 = new GradientPaint( 0.0f, 0.0f, Color.blue, 0.0f, 0.0f, new Color( 0, 0, 64 ) );
        // GradientPaint gp1 = new GradientPaint( 0.0f, 0.0f, Color.green, 0.0f, 0.0f, new Color( 0, 64, 0 ) );
        // // GradientPaint gp2 = new GradientPaint( 0.0f, 0.0f, Color.red, 0.0f, 0.0f, new Color( 64, 0, 0 ) );
        // renderer.setSeriesPaint( 0, gp0 );
        // renderer.setSeriesPaint( 1, gp1 );
        // // renderer.setSeriesPaint( 2, gp2 );

        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions( CategoryLabelPositions.createUpRotationLabelPositions( Math.PI / 6.0 ) );
        // OPTIONAL CUSTOMISATION COMPLETED.
        // retrieve image
        BufferedImage bi = chart.createBufferedImage( 640, 480 );

        File outputFile = new File( parentDir, name + ".png" );
        ImageIO.write( bi, "png", outputFile );
    }

    protected void createXYTimeDataset( String query, String name, File parentDir )
        throws SQLException, IOException
    {
        RzawJDBCXYDataset dataset = new RzawJDBCXYDataset( databaseProcessor.getConnection() );
        LOG.debug( "Chart query: " + query );
        dataset.executeQuery( query );
        // create the chart...
        JFreeChart chart = ChartFactory.createTimeSeriesChart( name, "Monat", "Leistung", dataset, false, false, false );

        // set the background color for the chart...
        // hsl(211, 92%, 36%)
        // #075BB2
        // rgb(7, 91, 178)
        Paint p = new GradientPaint( 0, 480, new Color( 7, 91, 178 ), 0, 0, Color.white );
        chart.setBackgroundPaint( p );

        // get a reference to the plot for fselect MONTH(datum), sum(daysum) from ertrag where datum > (SELECT
        // TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1000 day)))urther customisation...
        XYPlot plot = (XYPlot) chart.getPlot();

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride( new SimpleDateFormat( "MMM-yyyy" ) );

        // set the range axis to display integers only...
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

        // disable bar outlines...
        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        // renderer.setDrawBarOutline( false );

        // // set up gradient paints for series...
        GradientPaint gp0 =
            new GradientPaint( 0.0f, 0.0f, new Color( 188, 224, 46 ), 0.0f, 0.0f, new Color( 0, 0, 64 ) );
        renderer.setSeriesPaint( 0, gp0 );
        // CategoryAxis domainAxis = plot.getDomainAxis();
        // domainAxis.setCategoryLabelPositions( CategoryLabelPositions.createUpRotationLabelPositions( Math.PI / 6.0 )
        // );
        // OPTIONAL CUSTOMISATION COMPLETED.
        // retrieve image
        BufferedImage bi = chart.createBufferedImage( 670, 380 );

        File outputFile = new File( parentDir, name + ".png" );
        ImageIO.write( bi, "png", outputFile );
    }

    protected void createLineChart( String query, String name, File parentDir )
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
        Paint p = new GradientPaint( 0, 480, new Color( 7, 91, 178 ), 0, 0, Color.white );
        chart.setBackgroundPaint( p );

        XYPlot plot = (XYPlot) chart.getPlot();

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride( new SimpleDateFormat( "HH:mm:ss" ) );

        // set the range axis to display integers only...
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );

        // disable bar outlines...
        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();

        // // set up gradient paints for series...
        GradientPaint gp0 =
            new GradientPaint( 0.0f, 0.0f, new Color( 188, 224, 46 ), 0.0f, 0.0f, new Color( 0, 0, 64 ) );
        renderer.setSeriesPaint( 0, gp0 );
        renderer.setBasePaint( gp0 );
        renderer.setBaseSeriesVisible( true );
        renderer.setSeriesFillPaint( 0, gp0 );

        // retrieve image
        BufferedImage bi = chart.createBufferedImage( 670, 380 );

        File outputFile = new File( parentDir, name + ".png" );
        ImageIO.write( bi, "png", outputFile );
    }

    public ChartProducer( Properties properties )
        throws SQLException
    {
        databaseProcessor = new DatabaseProcessor( properties, false );
    }

    public void shutdown()
    {
        databaseProcessor.shutdown();
    }
}
