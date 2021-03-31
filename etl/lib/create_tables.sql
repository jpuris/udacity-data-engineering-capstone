-- drop stage tables
DROP TABLE IF EXISTS public.stage_demo;
DROP TABLE IF EXISTS public.stage_temp;

-- drop fact tables
DROP TABLE IF EXISTS public.fact_demo;
DROP TABLE IF EXISTS public.fact_temp;

-- drop dim tables
DROP TABLE IF EXISTS public.dim_city;
DROP TABLE IF EXISTS public.dim_date;

-- create stage tables
CREATE TABLE public.stage_demo (
    city                    TEXT,
    record_timestamp        DATE,
    number_of_veterans      NUMERIC,
    male_population         NUMERIC,
    foreign_born            NUMERIC,
    average_household_size  NUMERIC,
    median_age              NUMERIC,
    total_population        NUMERIC,
    female_population       NUMERIC
);

CREATE TABLE public.stage_temp (
    city        TEXT,
    dt          DATE,
    avg_temp    NUMERIC
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
    CONSTRAINT fk_city FOREIGN KEY(city_id)
        REFERENCES dim_city(city_id),
    CONSTRAINT fk_date FOREIGN KEY("date")
        REFERENCES dim_date("date")
);

CREATE TABLE public.fact_temp (
    temp_id     SERIAL PRIMARY KEY,
    city_id     BIGINT,
    date        DATE,
    avg_temp    BIGINT,
    CONSTRAINT fk_city FOREIGN KEY(city_id)
        REFERENCES dim_city(city_id),
    CONSTRAINT fk_date FOREIGN KEY("date")
        REFERENCES dim_date("date")
);
