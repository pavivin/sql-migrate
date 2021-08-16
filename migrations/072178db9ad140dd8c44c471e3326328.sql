CREATE TABLE migration_versions (
	version character varying NOT NULL
);

CREATE TABLE user (
	name character varying NULL,
	score integer NOT NULL,
	email character varying NOT NULL,
	password character varying NULL,
	email_confirm character varying NULL,
	city character varying NULL,
	phone_number character varying NULL,
	donations_number integer NULL,
	donations_sum integer NULL,
	id integer NOT NULL
);

