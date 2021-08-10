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

CREATE TABLE trash_can (
	weight double precision NULL,
	latitude double precision NOT NULL,
	longitude double precision NOT NULL,
	full_paper double precision NULL,
	full_glass double precision NULL,
	full_plastic double precision NULL,
	state integer NULL,
	state_user integer NULL,
	id integer NOT NULL
);

CREATE TABLE organization (
	name character varying NULL,
	score integer NOT NULL,
	district character varying NULL,
	id integer NOT NULL
);

CREATE TABLE target (
	name character varying NULL,
	score integer NOT NULL,
	total_score integer NULL,
	organization_id integer NULL,
	id integer NOT NULL
);

CREATE TABLE history (
	id integer NOT NULL,
	trash_can_id integer NULL,
	user_id integer NULL,
	paper double precision NULL,
	glass double precision NULL,
	plastic double precision NULL,
	weight double precision NULL,
	datetime character varying NULL,
	longitude double precision NULL,
	latitude double precision NULL,
	is_processed boolean NULL,
	score integer NULL
);

