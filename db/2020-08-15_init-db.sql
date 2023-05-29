CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    company_name VARCHAR NOT NULL
);

CREATE TABLE location (
    id SERIAL PRIMARY KEY,
    person_id INT NOT NULL,
    coordinate GEOMETRY NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (person_id) REFERENCES person(id)
);
CREATE INDEX coordinate_idx ON location (coordinate);
CREATE INDEX creation_time_idx ON location (creation_time);

CREATE TABLE connection (
    id SERIAL PRIMARY KEY,
    from_person_id INT NOT NULL,
    to_person_id INT NOT NULL,
    FOREIGN KEY (from_person_id) REFERENCES person(id),
    FOREIGN KEY (to_person_id) REFERENCES person(id),
    exposed_time TIMESTAMP NOT NULL DEFAULT NOW(),
    longitude VARCHAR NOT NULL,
    latitude VARCHAR NOT NULL
);

CREATE TABLE location_sentinel (
    last_location_id SERIAL PRIMARY KEY
);

insert into public.location_sentinel (last_location_id) values (68);
