-- Keyspace  and Column families:

CREATE KEYSPACE cassandra_load WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'}  AND durable_writes = true;



	
CREATE TABLE cassandra_load.summary (
    tenant_id text,
    year int,
    month int,
    week_of_month int,
    ticket_id text,
    account text,
    basket_id text,
    customer text,
    destination text,
    exec_price double,
    exec_qty double,
    stargate text,
    order_price double,
    order_qty double,
    parent_tickets text,
    sender_comp_id text,
    sender_sub_id text,
    sent_date text,
    sent_time text,
    side text,
    status text,
    strategy text,
    symbol text,
    tif text,
    type text,
    wave_id text,
    PRIMARY KEY ((tenant_id, year, month,week_of_month), sent_date,ticket_id)
);




CREATE CUSTOM INDEX summary_idx ON cassandra_load.summary (stargate) USING 'com.stratio.cassandra.lucene.Index' 
WITH OPTIONS = { 'refresh_seconds' : '1','schema': '{
fields : {
	symbol    : {type : "string"},
	destination  : {type : "string"},
	side  : {type : "text"},
	ticket_id : {type : "string"},
	tif : {type : "string"},
	status : {type : "string"},
	account : {type : "string"}
	}
}'
};


CREATE TABLE cassandra_load.messages (
    tenant_id text,
    year int,
    month int,
    week_of_month int,
    id text,
    basket_id text,
    destination text,
    stargate text,
    msg_type text,
    order_price double,
    order_qty double,
    order_type text,
    row_type text,
    sender_comp_id text,
    sender_sub_id text,
    sent_date text,
    sent_time text,
    side text,
    symbol text,
    ticket_id text,
    tif text,
    trader_id text,
    wave_id text,
    PRIMARY KEY ((tenant_id, year, month,week_of_month), ticket_id,id)
);



CREATE CUSTOM INDEX cassandra_load_idx ON cassandra_load.messages (stargate) USING 'com.stratio.cassandra.lucene.Index' 
WITH OPTIONS = { 'refresh_seconds' : '1','schema': '{
fields : {
	symbol    : {type : "string"},
	destination  : {type : "string"},
	side  : {type : "text"},
	trader_id : {type : "string"},
	msg_type : {type : "string"}
	}
}'
};
	
	