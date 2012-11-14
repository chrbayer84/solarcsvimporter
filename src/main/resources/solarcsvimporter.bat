REM set this to the path where java is installed
set path=%path%;c:\Programme\JRE-1.6.27\bin

java -XX:+HeapDumpOnOutOfMemoryError -XX:NewSize=64m -Xms512m -Xmx1024m -jar lib\solarcsvimporter-1.0.jar config.properties
