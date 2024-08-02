DROP SCHEMA IF EXISTS v1 CASCADE;

CREATE SCHEMA v1 AUTHORIZATION myuser;
-- v1.invoice definition

-- Drop table

-- DROP TABLE v1.invoice;

CREATE TABLE v1.invoice (
	code text NOT NULL,
	status int8 NULL,
	issue_on float8 NULL,
	created_on timestamp NULL,
	org text NULL,
	total_value int8 NULL,
	total_quantity int8 NULL,
	src_stock_code text NULL,
	src_stock int8 NULL,
	vendor_id int8 NULL,
	payment_type text NULL,
	note_type int8 NULL,
	channel text NULL,
	total_discount int8 NULL,
	seq int8 NULL,
	src_ancestors text NULL,
	so_chung_tu text NULL,
	CONSTRAINT pk_invoice PRIMARY KEY (code)
);


-- v1.invoice_items definition

-- Drop table

-- DROP TABLE v1.invoice_items;

CREATE TABLE v1.invoice_items (
	discount_rate text NULL,
	quantity_change int8 NULL,
	issue_type int8 NULL,
	issue_code text NULL,
	price int8 NULL,
	invoice_code text NULL,
	product_code text NULL,
	discount text NULL,
	quantity int8 NULL,
	unit text NULL,
	pkg_quantity float8 NULL,
	CONSTRAINT fk_invoice_code FOREIGN KEY (invoice_code) REFERENCES v1.invoice(code)
);