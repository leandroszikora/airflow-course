create table temperature
(
    date text,
    id   integer,
    min  double precision,
    max  double precision
);

create table humidity
(
    date              text,
    id                integer,
    relative_humidity float,
    did_rain          boolean
);

create table precipitation
(
    date        text,
    id          integer,
    milliliters double precision,
    hours       double precision
);