CREATE TABLE IF NOT EXISTS stock (
    date date NOT NULL,
    symbol varchar(40) NOT NULL,
    open float,
    high float,
    low float,
    close float,
    adj_close float,
    volume int,
    CONSTRAINT date_symbol PRIMARY KEY (date,symbol)
);