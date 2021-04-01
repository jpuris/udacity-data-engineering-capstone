LOAD_DIM_DATE = """
INSERT INTO dim_date
WITH data_demo AS (
    SELECT DISTINCT
        record_timestamp                     AS "date",
        EXTRACT(DAY FROM record_timestamp)   AS "day",
        EXTRACT(WEEK FROM record_timestamp)  AS "week",
        EXTRACT(MONTH FROM record_timestamp) AS "month",
        EXTRACT(YEAR FROM record_timestamp)  AS "year",
        EXTRACT(DOW FROM record_timestamp)   AS "weekday"
    FROM
        stage_demo
),
    data_temp AS (
        SELECT DISTINCT
            dt                     AS "date",
            EXTRACT(DAY FROM dt)   AS "day",
            EXTRACT(WEEK FROM dt)  AS "week",
            EXTRACT(MONTH FROM dt) AS "month",
            EXTRACT(YEAR FROM dt)  AS "year",
            EXTRACT(DOW FROM dt)   AS "weekday"
        FROM
            stage_temp
    )
SELECT * FROM data_demo
UNION
SELECT * FROM data_temp;
"""

LOAD_DIM_CITY = """
INSERT INTO dim_city(city_name)
SELECT DISTINCT city
FROM stage_demo
UNION
SELECT DISTINCT city
FROM stage_temp;
"""

LOAD_FACT_DEMO = """
INSERT INTO fact_demo (
    city_id,
    date,
    number_of_veterans,
    male_population,
    foreign_born,
    average_household_size,
    median_age,
    total_population,
    female_population
)
SELECT
    dim_c.city_id,
    dim_d.date,
    number_of_veterans,
    male_population,
    foreign_born,
    average_household_size,
    median_age,
    total_population,
    female_population
FROM
    stage_demo    sd
    JOIN dim_date dim_d ON sd.record_timestamp = dim_d.date
    JOIN dim_city dim_c ON sd.city = dim_c.city_name;
"""

LOAD_FACT_TEMP = """
INSERT INTO fact_temp (city_id, date, avg_temp)
SELECT
    dc.city_id,
    dt.date,
    avg_temp
FROM
    stage_temp    st
    JOIN dim_city dc ON dc.city_name = st.city
    JOIN dim_date dt ON dt.date = st.dt;
"""

TABLE_ROW_COUNT = """
SELECT
    count(*) as count
FROM
    %(table)s
"""
