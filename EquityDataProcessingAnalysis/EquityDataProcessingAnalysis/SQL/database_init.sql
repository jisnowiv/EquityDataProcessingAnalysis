CREATE DATABASE EDBA_DB;
USE EBDA_DB

CREATE TABLE Prices(
	ticker_id int,
	ticker varchar(100),
	price_id int,
	price_date date,
	price_open decimal,
	price_high decimal,
	price_low decimal,
	price_close decimal,
	volume decimal,
	closeadj decimal,
	closeunadj decimal,
	lastupdated date,
	FOREIGN KEY (ticker_id) REFERENCES Tickers(ticker_id)
);

CREATE TABLE Tickers(
	ticker_id int IDENTITY(1, 1) PRIMARY KEY,
	ticker_table varchar(100),
	permaticker varchar(100),
	ticker varchar(100),
	ticker_name varchar(100),
	exchange varchar(100),
	isdelisted varchar(1),
	category varchar(100),
	cusips varchar(100),
	siccode varchar(100),
	sicsector varchar(100),
	famasector varchar(100),
	famaindustry varchar(100),
	sector varchar(100),
	industry varchar(100),
	scalemarketcap varchar(100),
	scalerevenue varchar(100),
	relatedtickers varchar(100),
	currency varchar(100),
	loc varchar(100),
	lastupdated date,
	firstadded date,
	firstpricedate date,
	lastpricedate date,
	firstquarter date,
	lastquarter date,
	secfilings varchar(200),
	companysite varchar(200),
);