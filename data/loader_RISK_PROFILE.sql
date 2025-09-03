-- COPY INTO loader command for RISK_PROFILE
COPY INTO RISK_PROFILE (RISK_PROFILE)
FROM @repo_stage/Risk_Profile.csv
FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'ABORT_STATEMENT';
--------------------------------------------------------
