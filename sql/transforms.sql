-- ============================================
-- Transform Queries for Production Load
-- ============================================

-- Deduplicate exchange rates: keep latest rate per currency pair per day
INSERT INTO production.exchange_rates (base_currency, target_currency, rate, effective_date)
SELECT DISTINCT ON (base_currency, target_currency, DATE(extraction_timestamp))
    base_currency,
    target_currency,
    rate,
    DATE(extraction_timestamp) AS effective_date
FROM staging.exchange_rates
WHERE extraction_timestamp > NOW() - INTERVAL '2 days'
ORDER BY base_currency, target_currency, DATE(extraction_timestamp), extraction_timestamp DESC
ON CONFLICT (base_currency, target_currency, effective_date)
DO UPDATE SET
    rate = EXCLUDED.rate,
    loaded_at = NOW();

-- Pivot World Bank indicators into country_indicators table
INSERT INTO production.country_indicators (
    country_code, country_name, year, gdp_usd, population_total,
    inflation_rate, unemployment_rate
)
SELECT
    country_code,
    MAX(country_name) AS country_name,
    year,
    MAX(CASE WHEN indicator_code = 'NY.GDP.MKTP.CD' THEN value END) AS gdp_usd,
    MAX(CASE WHEN indicator_code = 'SP.POP.TOTL' THEN value END)::BIGINT AS population_total,
    MAX(CASE WHEN indicator_code = 'FP.CPI.TOTL.ZG' THEN value END) AS inflation_rate,
    MAX(CASE WHEN indicator_code = 'SL.UEM.TOTL.ZS' THEN value END) AS unemployment_rate
FROM staging.worldbank_indicators
WHERE indicator_code IN (
    'NY.GDP.MKTP.CD',    -- GDP (current US$)
    'SP.POP.TOTL',       -- Population total
    'FP.CPI.TOTL.ZG',    -- Inflation, consumer prices (annual %)
    'SL.UEM.TOTL.ZS'     -- Unemployment, total (% of labor force)
)
AND year IS NOT NULL
GROUP BY country_code, year
ON CONFLICT (country_code, year)
DO UPDATE SET
    gdp_usd = EXCLUDED.gdp_usd,
    population_total = EXCLUDED.population_total,
    inflation_rate = EXCLUDED.inflation_rate,
    unemployment_rate = EXCLUDED.unemployment_rate,
    loaded_at = NOW();

-- Calculate derived metrics: GDP per capita
INSERT INTO production.derived_metrics (metric_name, metric_value, source_tables)
SELECT
    'gdp_per_capita_' || country_code || '_' || year AS metric_name,
    (gdp_usd / NULLIF(population_total, 0))::NUMERIC(20, 4) AS metric_value,
    ARRAY['production.country_indicators'] AS source_tables
FROM production.country_indicators
WHERE gdp_usd IS NOT NULL AND population_total IS NOT NULL
ON CONFLICT DO NOTHING;
