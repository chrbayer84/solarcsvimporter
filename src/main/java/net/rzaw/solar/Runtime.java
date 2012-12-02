package net.rzaw.solar;

import static net.rzaw.solar.StringFormatter.formatString;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.rzaw.solar.charts.ChartProducer;
import net.rzaw.solar.csvimport.CSVImporter;
import net.rzaw.solar.csvimport.InstallationEntry;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class Runtime
{
    private static final Logger LOG = Logger.getLogger( Runtime.class );

    public static void main( String[] args )
    {
        try
        {
            // check command line
            Preconditions.checkArgument( args.length == 1,
                "Please specify exactly only one argument to the configfile to use." );
            // process config file
            String configFile = args[0];
            Properties properties = new Properties();
            InputSupplier<FileInputStream> configFileInputStreamSupplier =
                Files.newInputStreamSupplier( new File( configFile ) );
            properties.load( configFileInputStreamSupplier.getInput() );
            // create new CSV importer instance
            final CSVImporter csvImporter = new CSVImporter( properties );
            final ChartProducer chartProducer = new ChartProducer( properties );
            final ExecutorService importAndChartsExecutor =
                Executors.newFixedThreadPool( java.lang.Runtime.getRuntime().availableProcessors() * 2,
                    new ThreadFactoryBuilder().setDaemon( true ).setNameFormat( "ImportAndChartsProducer #(%s)" )
                        .build() );
            try
            {
                // get all file paths from the database
                List<InstallationEntry> installationEntries = csvImporter.getInstallationEntries();
                LOG.info( formatString( "Starting up CSVImporter, processing the following directories: {}",
                    Arrays.toString( installationEntries.toArray() ) ).toString() );
                List<Callable<ImportChartResult>> tasks = Lists.newArrayList();

                for ( final InstallationEntry installationEntry : installationEntries )
                {
                    tasks.add( new Callable<ImportChartResult>()
                    {
                        @Override
                        public ImportChartResult call()
                        {
                            try
                            {
                                csvImporter.processDirectory( installationEntry );
                                // now render the chart images
                                chartProducer.render( installationEntry, new DateTime() );
                            }
                            catch ( SQLException e )
                            {
                                return new ImportChartResult( false, installationEntry, e );
                            }
                            catch ( IOException e )
                            {
                                return new ImportChartResult( false, installationEntry, e );
                            }
                            return new ImportChartResult( true, null, null );
                        }
                    } );
                }
                List<Future<ImportChartResult>> results = importAndChartsExecutor.invokeAll( tasks );
                importAndChartsExecutor.shutdown();
                importAndChartsExecutor.awaitTermination( 1, TimeUnit.DAYS );
                LOG.info( "Finished processing directories." );

                for ( Future<ImportChartResult> result : results )
                {
                    if ( !result.get().isSuccess() )
                    {
                        LOG.error( "Failed processing directory " + result.get().getInstallationEntry().getDirectory(),
                            result.get().getException() );
                        throw result.get().getException();
                    }
                }
            }
            finally
            {
                csvImporter.close();
                chartProducer.close();
                // all tasks have been completed
                importAndChartsExecutor.shutdownNow();
            }
        }
        catch ( Throwable e )
        {
            // Explicitly catch throwable to be able to log OOM to logfile
            LOG.fatal( "Terminated unexpectly:", e );
        }
    }
}
