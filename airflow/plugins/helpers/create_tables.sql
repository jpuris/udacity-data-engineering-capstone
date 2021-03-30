-- drop stage tables
DROP TABLE IF EXISTS public.stage_demo;
DROP TABLE IF EXISTS public.stage_temp;

-- drop dim tables
DROP TABLE IF EXISTS public.dim_city;
DROP TABLE IF EXISTS public.dim_date;

-- drop fact tables
DROP TABLE IF EXISTS public.fact_demo;
DROP TABLE IF EXISTS public.fact_temp;

-- create stage tables
CREATE TABLE public.stage_demo (
    city_name               TEXT,
    state_name              TEXT,
    "date"                  DATE,
    number_of_veterans      BIGINT,
    male_population         BIGINT,
    foreign_born            BIGINT,
    average_household_size  BIGINT,
    median_age              BIGINT,
    total_population        BIGINT,
    female_population       BIGINT,
);

CREATE TABLE public.stage_temp (
    city_name       TEXT,
    state_name      TEXT,
    date_id         BIGINT,
    avg_temp        BIGINT
);

-- create dim tables
CREATE TABLE public.dim_date (
	"date"  DATE PRIMARY KEY,
	"day"   INT,
	week    INT,
	"month" TEXT,
	"year"  INT,
	weekday TEXT
) ;

CREATE TABLE public.dim_city (
    city_id         SERIAL PRIMARY KEY,
    city_name       TEXT
);

-- create fact tables
CREATE TABLE public.fact_demo (
    demo_id                 SERIAL PRIMARY KEY,
    city_id                 BIGINT,
    "date"                  DATE,
    number_of_veterans      BIGINT,
    male_population         BIGINT,
    foreign_born            BIGINT,
    average_household_size  BIGINT,
    median_age              BIGINT,
    total_population        BIGINT,
    female_population       BIGINT,
);

CREATE TABLE public.fact_temp (
    temp_id     SERIAL PRIMARY KEY,
    city_id     BIGINT,
    date_id     BIGINT,
    avg_temp    BIGINT
);
