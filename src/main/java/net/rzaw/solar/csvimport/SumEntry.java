package net.rzaw.solar.csvimport;

import java.util.Date;

import com.google.common.base.Objects;

public class SumEntry
{
    private Date date;

    private Double daySum;

    public SumEntry( Date date, Double daySum )
    {
        this.setDate( date );
        this.setDaySum( daySum );
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

    public void setDaySum( Double daySum )
    {
        this.daySum = daySum;
    }

    public Date getDate()
    {
        return date;
    }

    public void setDate( Date date )
    {
        this.date = date;
    }
}