package net.rzaw.solar;

public class StringFormatter
{
    private static final String DELIMITER = "{}";

    private final Object[] arguments;

    private final String string;

    private StringFormatter( String message, Object[] arguments )
    {
        this.string = message;
        this.arguments = arguments;
    }

    public static StringFormatter formatString( String message, Object... args )
    {
        return new StringFormatter( message, args );
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder( string.length() + 512 );
        int argIndex = 0;
        int cursor = 0;

        for ( int currentFound = string.indexOf( DELIMITER, cursor ); argIndex < arguments.length && currentFound >= 0; currentFound =
            string.indexOf( DELIMITER, cursor ) )
        {
            if ( currentFound > cursor )
            {
                stringBuilder.append( string.substring( cursor, currentFound ) );
            }
            cursor = currentFound + DELIMITER.length();

            String argument = arguments[argIndex] != null ? arguments[argIndex].toString() : "null";
            stringBuilder.append( argument != null ? argument.toString() : "null" );
            argIndex++;
        }

        if ( cursor < string.length() )
        {
            stringBuilder.append( string.substring( cursor ) );
        }

        return stringBuilder.toString();
    }
}
