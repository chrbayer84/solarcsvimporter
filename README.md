<html>
<body>

<h1>SolarImporter v1.2</h1>
<h2>Changelog</h2>
<p>2012-12-28  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * .../java/net/rzaw/solar/db/DatabaseProcessor.java
<br>  * Changed timestamp for daysums to be always on 0:00am to avoid duplicate daysum entries per day when using file expiry > 24h
<p>2012-12-10  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * .../java/net/rzaw/solar/db/DatabaseProcessor.java,
<br>  src/main/java/net/rzaw/solar/db/Transaction.java,
<br>  src/main/resources/create_tables.sql:
<br>* Fixed duplicate adding of records when files change, now uses mysql 'replace into' to avoid duplicates
<p>2012-12-02  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * .../java/net/rzaw/solar/ImportChartResult.java,
<br>  src/main/java/net/rzaw/solar/Runtime.java,
<br>  .../java/net/rzaw/solar/charts/ChartProducer.java,
<br>  .../rzaw/solar/charts/RzawJDBCCategoryDataset.java,
<br>  .../net/rzaw/solar/charts/RzawJDBCXYDataset.java,
<br>  .../java/net/rzaw/solar/csvimport/CSVImporter.java,
<br>  .../java/net/rzaw/solar/csvimport/SumEntry.java,
<br>  .../java/net/rzaw/solar/db/DatabaseProcessor.java,
<br>  src/main/java/net/rzaw/solar/db/Transaction.java: 
<br>* Support for 36h delay before marking a file as "done" - proper transaction isolation and closing using transaction callable
<p>2012-11-21  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * pom.xml, src/main/java/net/rzaw/solar/Runtime.java,
<br>  .../java/net/rzaw/solar/db/DatabaseProcessor.java,
<br>  src/main/resources/log4j.properties:
<br>* Use ThreadPoolExecutor to execute tasks: Use c3p0 pooled jdbc connections
<p>2012-11-20  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * src/main/java/net/rzaw/solar/Runtime.java,
<br>  .../java/net/rzaw/solar/charts/ChartProducer.java,
<br>  .../java/net/rzaw/solar/db/DatabaseProcessor.java: * Charts refined, removed
<br>  captions everywhere, correct legend 
<br>* split importing CSV data and producing charts up into seperate tasks that depend from the installation and can be executed in parallel per installation 
<br>* Use ExecutorService for executing all tasks in parallel
<br>* cleanup, removed lots of obsolete db queries and disabled code
<p>2012-11-18  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * src/main/java/net/rzaw/solar/Runtime.java,
<br>  .../java/net/rzaw/solar/charts/ChartProducer.java,
<br>  .../rzaw/solar/charts/RzawJDBCCategoryDataset.java,
<br>  .../net/rzaw/solar/charts/RzawJDBCXYDataset.java,
<br>  .../java/net/rzaw/solar/csvimport/CSVImporter.java,
<br>  .../java/net/rzaw/solar/db/DatabaseProcessor.java,
<br>  src/main/resources/log4j.properties:
<br>* Charting support for 4 base queries
<br>* Better logging added all over the place, now also writes Errors to log ;) 
<br>* usual cleanup
<p>2012-11-14  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * pom.xml, src/main/java/net/rzaw/solar/Runtime.java,
<br>  src/main/java/net/rzaw/solar/csvimport/CSVImporter.java,
<br>  src/main/resources/solarcsvimporter.bat:
<br>* increased default memory footprint for JVM
<br>* setting correct version 1.2-SNAPSHOT in pom
<br>* disabled charting for now
<br>* solarcsvimporter-1: Joda date parsing fails when importing
<br>* always use  local time
<p>2012-10-21  Christian Bayer <chrbayer84@googlemail.com>
<br>
<br>  * README.md, pom.xml, src/main/assembly/assembly.xml,
<br>  src/main/java/net/rzaw/solar/Runtime.java,
<br>  src/main/java/net/rzaw/solar/StringFormatter.java,
<br>  .../java/net/rzaw/solar/charts/ChartProducer.java,
<br>  .../rzaw/solar/charts/RzawJDBCCategoryDataset.java,
<br>  .../net/rzaw/solar/charts/RzawJDBCXYDataset.java,
<br>  .../java/net/rzaw/solar/csvimport/CSVImporter.java,
<br>  .../rzaw/solar/csvimport/InstallationEntry.java,
<br>  .../java/net/rzaw/solar/csvimport/SumEntry.java,
<br>  .../java/net/rzaw/solar/db/DatabaseProcessor.java,
<br>  src/main/resources/config.properties, src/main/resources/create_tables.sql,
<br>  src/main/resources/log4j.properties, src/main/resources/solarcsvimporter.bat,
<br>  .../net/rzaw/solar/csvimport/_CSVImporter.java,
<br>  src/test/resources/min110527.csv, src/test/resources/min110901.csv,
<br>  src/test/resources/min120209.csv, src/test/resources/minEmpty.csv:
<br>* Initial import, v1.2-SNAPSHOT

<p>Bugfix falls leere Strings/Felder in verbrauch_daysum_cols und
verbrauch_sum_cols stehen, Integer-ParseException verhindert.

<h1>SolarImporter v1.1</h1>

<p>Du musst die datenbank nochmal neu erstellen, oder du fuegst die spalten hinzu. Ist aber 
eine frickelei, hau die alte weg und importierer alle CSVs neu. Heb dir aber 
die alte config-datei auf, sonst musste da alles neu eingeben. Die 
Verbrauchergeschichte funktioniert folgendermassen:

<p>In der Anlagentabelle gibst du Spaltennummern, die als Verbrauch betrachtet 
werden sollen - auch dafuer gibts wieder daySum und Pac, also 
momentanverbrauch und summenwert. Allerdings kann man natuerlich nicht den 
Spaltenenamen in die Anlagentabelle schreiben, denn der ist ja im Zweifelsfall 
einfach wieder Pac/Daysum. Also musst du den index der spalte angeben, 
beginnend mit 1. 
In deiner angehangenen Tabelle vom RZ waeren das die folgenden:
<p>
verbrauch_daysum_cols: 5 
verbrauch_sum_cols: 4

<p>Die logik versteht auch durch kommas getrennte werte, wenn du also mehr werte 
in der leistungsberechnung ignorieren und dafuer als verbrauch 
zusammenaddieren willst, dann einfach durch komma trennen und in die db. Ich 
weiss allerdings nicht, ob es leerzeichen zwischen komma und zahl versteht, 
also im Zweifelsfall eher "1,4" schreiben als "1, 4".

<h1>SolarImporter v1.0</h1>

<p>
So, wie versprochen der Solarcsvimporter. Ich habe es der einfachheit halber 
mal angehangen, ich werde irgendwann den Quelltext auf meinen GitHub-Account 
hochladen. Bis dahin gibts direkten support  Ist viel Text, aber alles 
einfache dinge, keine Angst.

<p>
Folgendes musst du tun:
<br>
1. Java herunterladen, falls nicht schon vorhanden:
http://javadl.sun.com/webapps/download/AutoDL?BundleId=58124

<p>
2.solarcsvimporter.zip auspacken und in solarcsvimporter.bat den Pfad von Java 
anpassen (gleich die zweite Zeile).

<p>
3. Probieren, ob das programm laeuft: Auf der kommandozeile nach c:\ gehen 
(NICHT in das verzeichnis, in dem du die zip-datei ausgepackt hast!!) und die 
bat-datei ins kommandozeilenfenster ziehen. Damit kopiert windows den 
kompletten pfad der datei in die kommandozeile. Enter druecken. Da wir nicht 
im Verzeichnis sind, in dem das programm ausgepackt wurde (wenn doch, und 
alles klappt, sieht man naemlich gar nichts, weil das programm sich sofort mit 
der Datenbank verbinden will - die es aber bei dir unter der addresse sicher 
nicht gibt!), kann es seine config-datei nicht finden und meckert:

<p>
Exception in thread "main" java.io.FileNotFoundException: config.properties (No 
such file or directory)
<br>        at java.io.FileInputStream.open(Native Method)
<br>        at java.io.FileInputStream.<init>(FileInputStream.java:120)
<br>        at com.google.common.io.Files$1.getInput(Files.java:100)
<br>        at com.google.common.io.Files$1.getInput(Files.java:97)
<br>        at net.rzaw.solar.CSVImporter.main(CSVImporter.java:736)

<p>
Wenn genau das kommt, ist alles in Ordnung, das Programm konnte von Java 
ausgefuert werden, es kann alle Bibliotheken finden und java geht auch. JEDE 
ANDERE MELDUNG IST SCHLECHT! In dem Fall ist Java nicht richtig installiert, 
der pfad stimmt nicht oder meine bat-datei ist scheisse (konnte sie hier nicht 
testen).

<p>
4. Konfigurationsdatei anpassen, Mysql passwort und benutzernamen setzen, 
(lieber nicht root), hostname etc. setzen. Eventuell mit Wordpad oder einem 
richtigen Editor wie Ultraedit bearbeiten, weil die Datei vermutlich Unix-
Zeilenende hat (erkennt man in Notepad daran, dass alles in einer Zeile steht. 
Nicht schoen.) Vorsicht am ende der Zeile, beispielsweise vom Passwort. Ich 
hatte dort noch ein leerzeichen stehen. Das wird eingelesen und als password 
verwendet (und funktioniert dann natuerlich nicht, hat mich 2 Tage gekostet, 
bis ich das rausbekommen hab).

<p>
5. Datenbank neu anlegen. Ich habe noch einige Aenderungen am Schema 
vorgenommen, sollte aber alles selbsterklaerend sein. Die create_tables.sql-
Datei enthaelt auch die drop-Anweisungen fuer die alten tabellen, also 
vorsicht wenn du das in 2 Jahren nochmal ausfuehren solltest - er haut etwaige 
alte Tabellen dann weg! Am besten gleich nach dem ausfuehren auskommentieren.

<p>
6. (Endlich) in das Verzeichnis wechseln, in dem du das zip-file ausgepackt 
hast. dort nochmal solarimporter.bat ausfuehren. Jetzt sollte das hier kommen:

<p>
22:40:40,872 [main] INFO  net.rzaw.solar.CSVImporter  - Starting up 
CSVImporter, processing the following directories: []

<p>
Das sagt dir, dass er sich erfolgreich mit der Datenbank verbunden hat, aber 
in der Tabelle anlage keine verzeichnisse zum verarbeiten aufgefunden hat: [].

<p>
7. Du ahnst es sicher schon: Verzeichnisse in die Tabelle anlage schreiben. 
Einfach den vollstaendigen Pfad in verzeichnis schreiben und aktiv auf "true" 
setzen. Dann wird beim naechsten lauf das verzeichnis verarbeitet:

<p>
22:40:40,872 [main] INFO  net.rzaw.solar.CSVImporter  - Starting up 
CSVImporter, processing the following directories: 
[/home/cbayer/projects/computer/solar/solarcsvimporter/src/test/resources]
22:40:40,875 [main] INFO  net.rzaw.solar.CSVImporter  - file min110901.csv is 
already done, not processing again.
22:40:40,877 [main] INFO  net.rzaw.solar.CSVImporter  - file min110527.csv is 
already done, not processing again.

<p>
Bei mir hat er die Dateien schonmal verarbeitet, sie sind als fertig markiert. 
Bei dir duerfte er etwas mehr zu tun haben. 

<p>
8. Christian antworten, welche Fehlermeldungen aufgetreten sind. Ich freue 
mich auf bug-reports  Java-Fehler heissen Exception. Wann immer eine 
auftritt, mir ne mail schreiben mit dem kompletten Stacktrace: Also nicht nur 
die Meldung sondern alle Zeilen, die danach noch kommen: (oder du schickst mir 
eben gleich das ganze logfile)

<p>
Exception in thread "main" java.io.FileNotFoundException: config.properties (No 
such file or directory)
<br>        at java.io.FileInputStream.open(Native Method)
<br>        at java.io.FileInputStream.<init>(FileInputStream.java:120)
<br>        at com.google.common.io.Files$1.getInput(Files.java:100)
<br>        at com.google.common.io.Files$1.getInput(Files.java:97)
<br>        at net.rzaw.solar.CSVImporter.main(CSVImporter.java:736)

<p>
Das Ding schreibt automatisch logfiles in ein verzeichnis log im 
projektverzeichnis. Diese Dateien werden auch automatisch gewechselt, morgen 
wird die datei von heute umbenannt in solarimporter-20120902.log usw.

</html>
</body>