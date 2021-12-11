create table weather
(
    date text,
    id   integer,
    min  double precision,
    max  double precision
);

create table humidity
(
    date text,
    id integer,
    relative_humidity float
);