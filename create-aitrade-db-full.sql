--
-- create-aitrade-db-full.sql
--
-- SQL to create empty AITrade database and users
-- run as mysql root user .. careful: will drop the
-- db and users if they exist
--
-- v1.0 2025/02/05
-- initial version
--

DROP DATABASE IF EXISTS `aitrade`;
CREATE DATABASE aitrade;

DROP USER IF EXISTS 'aitrade'@'%';
CREATE USER 'aitrade'@'%' IDENTIFIED BY 'aitrade1';
GRANT ALL ON aitrade.* TO 'aitrade'@'%';

USE aitrade;

CREATE TABLE `stock_data` (
  `id` int(11) NOT NULL auto_increment,
  `symbol` varchar(15) NOT NULL,
  `timestamp` datetime NOT NULL,
  `close` decimal(15,6) NOT NULL,
  `open` decimal(15,6) NOT NULL,
  `high` decimal(15,6) NOT NULL,
  `low` decimal(15,6) NOT NULL,
  `volume` bigint NOT NULL,
  `rsi` decimal(15,6),
  `ma50` decimal(15,6),
  `ma200` decimal(15,6),
  `macd` decimal(15,6),
  `macd_signal` decimal(15,6),
  `bb_upper` decimal(15,6),
  `bb_middle` decimal(15,6),
  `bb_lower` decimal(15,6),
  `adx` decimal(15,6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `symbol_timestamp` (`symbol`, `timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE market_cap (
    id INT AUTO_INCREMENT PRIMARY KEY,  -- Unique ID for each row
    date DATE NOT NULL,                 -- Date when the data was updated
    symbol VARCHAR(20) NOT NULL,        -- Symbol of the stock (e.g., AAPL)
    marketcap BIGINT NOT NULL,          -- Market cap in dollars (as an integer, assuming it's in dollars)
    UNIQUE KEY(symbol, date)            -- Ensure each symbol/date pair is unique
);
