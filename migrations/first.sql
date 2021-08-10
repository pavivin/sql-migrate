CREATE TABLE user (
	name character varying NULL,
	score integer NOT NULL,
	email character varying NOT NULL,
	id integer NOT NULL
);

CREATE TABLE target (
	name character varying NULL,
	score integer NOT NULL,
	total_score integer NULL,
	organization_id integer NULL,
	id integer NOT NULL
);