-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `anlage`
--

drop TABLE IF TABLE `verbrauch`;
drop TABLE IF TABLE `leistung_verbrauch`;
drop TABLE IF TABLE `leistung`;
drop TABLE IF TABLE `ertrag`;
drop TABLE IF TABLE `dateien`;
drop TABLE IF TABLE `anlage`;

CREATE TABLE IF NOT EXISTS `anlage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `verzeichnis` varchar(4096) CHARACTER SET utf8 NOT NULL,
  `aktiv` tinyint(1) NOT NULL,
  `verbrauch_daysum_cols` varchar(256) CHARACTER SET utf8,
  `verbrauch_sum_cols` varchar(256) CHARACTER SET utf8,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `dateien`
--

CREATE TABLE IF NOT EXISTS `dateien` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dateiname` varchar(4096) CHARACTER SET utf8 NOT NULL,
  `md5sum` varchar(32) CHARACTER SET utf8 NOT NULL,
  `fertig` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `ertrag`
--

CREATE TABLE IF NOT EXISTS `ertrag` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `anlage` int(11) NOT NULL,
  `datum` datetime NOT NULL,
  `typ` varchar(16) NOT NULL,
  `daysum` double NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;


CREATE TABLE IF NOT EXISTS `verbrauch` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `anlage` int(11) NOT NULL,
  `datum` datetime NOT NULL,
  `typ` varchar(16) NOT NULL,
  `daysum` double NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Tabellenstruktur für Tabelle `leistung`
--

CREATE TABLE IF NOT EXISTS `leistung` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `anlage` int(11) NOT NULL,
  `datum` datetime NOT NULL,
  `typ` varchar(16) NOT NULL,
  `leistung` double NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;


CREATE TABLE IF NOT EXISTS `leistung_verbrauch` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `anlage` int(11) NOT NULL,
  `datum` datetime NOT NULL,
  `typ` varchar(16) NOT NULL,
  `leistung` double NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;


--
-- Constraints der exportierten Tabellen
--

--
-- Constraints der Tabelle `ertrag`
--
ALTER TABLE `ertrag`
  ADD CONSTRAINT `ertrag_ibfk_1` FOREIGN KEY (`anlage`) REFERENCES `anlage` (`id`);

ALTER TABLE `verbrauch`
  ADD CONSTRAINT `verbrauch_ibfk_1` FOREIGN KEY (`anlage`) REFERENCES `anlage` (`id`);

--
-- Constraints der Tabelle `leistung`
--
ALTER TABLE `leistung`
  ADD CONSTRAINT `leistung_ibfk_1` FOREIGN KEY (`anlage`) REFERENCES `anlage` (`id`);

ALTER TABLE `leistung_verbrauch`
  ADD CONSTRAINT `leistung_verbrauch_ibfk_1` FOREIGN KEY (`anlage`) REFERENCES `anlage` (`id`);
  
CREATE UNIQUE INDEX uniq_leistung ON leistung (anlage, datum, typ);
CREATE UNIQUE INDEX uniq_leistung_verbrauch ON leistung_verbrauch (anlage, datum, typ);
CREATE UNIQUE INDEX uniq_ertrag ON ertrag (anlage, datum, typ);
CREATE UNIQUE INDEX uniq_verbrauch ON verbrauch (anlage, datum, typ);
  