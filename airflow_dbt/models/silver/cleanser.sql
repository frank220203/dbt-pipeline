{{ config(alias='edgar', materialized='table') }}

WITH base AS (
    SELECT * FROM {{ source('bronze', 'portfolio') }}
)

SELECT
    (header_data->>'submission_type')::VARCHAR AS submission_type,
    (header_data->'filer_info'->'filer'->'credentials'->>'cik')::VARCHAR AS filer_cik,
    TRY_STRPTIME(header_data->'filer_info'->>'period_of_report', '%m-%d-%Y')::DATE AS period_of_report,
    (form_data->'cover_page'->'filing_manager'->>'name')::VARCHAR AS filer_name,
    UNNEST(issuers)->>'name_of_issuer' AS name_of_issuer,
    UNNEST(issuers)->>'title_of_class' AS title_of_class,
    UNNEST(issuers)->>'cusip' AS cusip,
    (UNNEST(issuers)->>'value')::INT AS value,
    (UNNEST(issuers)->'shrs_or_prn_amt'->>'sshPrnamt')::INT AS ssh_prn_amt,
    UNNEST(issuers)->'shrs_or_prn_amt'->>'sshPrnamtType' AS ssh_prn_amt_type
FROM base