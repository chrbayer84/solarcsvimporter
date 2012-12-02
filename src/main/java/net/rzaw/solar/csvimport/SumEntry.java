package net.rzaw.solar.csvimport;

import java.util.Date;

import com.google.common.base.Objects;

public class SumEntry
{
    private final Date date;

    private final Double daySum;

    public SumEntry( Date date, Double daySum )
    {
        this.date = date;
        this.daySum = daySum;
    }

    @Override
    public String toString()
    {
        return "SumEntry [date=" + getDate() + ", daySum=" + getDaySum() + "]";
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( getDaySum(), getDate() );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof SumEntry )
            return Objects.equal( getDate(), ( (SumEntry) obj ).getDate() )
                && Objects.equal( getDaySum(), ( (SumEntry) obj ).getDaySum() );
        else
            return false;
    }

    public Double getDaySum()
    {
        return daySum;
    }

    public Date getDate()
    {
        return date;
    }
}
