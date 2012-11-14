package net.rzaw.solar;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import net.rzaw.solar.csvimport.CSVImporter;
import net.rzaw.solar.csvimport.InstallationEntry;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

public class Runtime
{
    private static final Logger LOG = Logger.getLogger( Runtime.class );

    public static void main( String[] args )
        throws IOException, SQLException
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
        CSVImporter csvImporter = new CSVImporter( properties );

        // get all file paths from the database
        ArrayList<InstallationEntry> installationEntries = csvImporter.getInstallationEntries();
        LOG.info( StringFormatter.formatString( "Starting up CSVImporter, processing the following directories: {}",
            Arrays.toString( installationEntries.toArray() ) ).toString() );
        for ( InstallationEntry installationEntry : installationEntries )
        {
            csvImporter.processDirectory( installationEntry );
        }
        csvImporter.shutdown();

        LOG.info( "Finished processing directories." );
        // now render the chart images
        // LOG.info( "Creating charts.");
        // ChartProducer chartProducer = new ChartProducer( properties );
        // chartProducer.render( new DateTime( 2011, 5, 27, 0, 0 ) );
        // chartProducer.shutdown();
        // LOG.info( "Finished creating charts.");
    }
}
