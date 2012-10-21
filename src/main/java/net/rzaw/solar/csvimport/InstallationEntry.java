package net.rzaw.solar.csvimport;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;

public class InstallationEntry
{
    private final int id;

    private final File directory;

    private final List<Integer> consumptionDaySumCols;

    private final List<Integer> consumptionSumCols;

    public InstallationEntry( int id, String directory, String consumptionDaySumCols, String consumptionSumCols )
    {
        this.id = id;
        this.directory = new File( directory );
        this.consumptionDaySumCols = Lists.newArrayList();
        this.consumptionSumCols = Lists.newArrayList();

        if ( consumptionDaySumCols != null && !consumptionDaySumCols.isEmpty() )
        {
            String[] consumptionDaySumColsString = CSVImporter.COMMA_SEPARATOR_PARSER.split( consumptionDaySumCols );
            for ( String string : consumptionDaySumColsString )
            {
                this.getConsumptionDaySumCols().add( Integer.parseInt( string ) );
            }
        }

        if ( consumptionSumCols != null && !consumptionSumCols.isEmpty() )
        {
            String[] consumptionSumColsString = CSVImporter.COMMA_SEPARATOR_PARSER.split( consumptionSumCols );
            for ( String string : consumptionSumColsString )
            {
                this.getConsumptionSumCols().add( Integer.parseInt( string ) );
            }
        }
    }

    public File getDirectory()
    {
        return directory;
    }

    @Override
    public String toString()
    {
        return "InstallationEntry [directory=" + directory + ", consumptionDaySumCols=" + getConsumptionDaySumCols()
            + ", consumptionSumCols=" + getConsumptionSumCols() + "]";
    }

    public List<Integer> getConsumptionDaySumCols()
    {
        return consumptionDaySumCols;
    }

    public List<Integer> getConsumptionSumCols()
    {
        return consumptionSumCols;
    }

    public int getId()
    {
        return id;
    }
}
