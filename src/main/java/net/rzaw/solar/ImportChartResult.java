package net.rzaw.solar;

import net.rzaw.solar.csvimport.InstallationEntry;

class ImportChartResult
{
    private final boolean success;

    private final InstallationEntry installationEntry;

    private final Throwable exception;

    public ImportChartResult( boolean success, InstallationEntry installationEntry, Throwable exception )
    {
        this.success = success;
        this.installationEntry = installationEntry;
        this.exception = exception;
    }

    public boolean isSuccess()
    {
        return success;
    }

    public InstallationEntry getInstallationEntry()
    {
        return installationEntry;
    }

    public Throwable getException()
    {
        return exception;
    }
}
