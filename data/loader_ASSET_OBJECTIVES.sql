-- COPY INTO loader command for ASSET_OBJECTIVES
COPY INTO ASSET_OBJECTIVES (ASSET_OBJECTIVE_NAME)
FROM @repo_stage/Asset_Objectives.csv
FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
ON_ERROR = 'ABORT_STATEMENT';
--------------------------------------------------------