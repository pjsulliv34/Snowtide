-- =====================================================
-- DEMO: Using the San Diego Municipal Code Cortex Search Service (SEARCH_PREVIEW)
-- =====================================================

-- Step 1: Verify available Cortex search services
SHOW CORTEX SEARCH SERVICES;

-- Step 2: Run a simple search preview
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'SANDIEGO_AI.MUNI_CODE.MUNI_CODE_SEARCH',
        '{
            "query": "noise ordinance after 10pm",
            "columns": ["CHUNK", "RELATIVE_PATH", "PDF_URL","CHAPTER_URL"],
            "limit": 5
        }'
    )
)['results'] AS results;

-- Step 3: Search with a different topic
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'SANDIEGO_AI.MUNI_CODE.MUNI_CODE_SEARCH',
        '{
            "query": "business license requirements",
            "columns": ["CHUNK", "RELATIVE_PATH", "PDF_URL","CHAPTER_URL"],
            "limit": 5
        }'
    )
)['results'] AS results;

-- Step 4: Use search preview results inside a query with metadata
WITH search_results AS (
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'SANDIEGO_AI.MUNI_CODE.MUNI_CODE_SEARCH',
            '{
                "query": "animal control regulations",
                "columns": ["CHUNK", "RELATIVE_PATH", "PDF_URL","CHAPTER_URL"],
                "limit": 5
            }'
        )
    )['results'] AS results
)
SELECT r.value:CHUNK::STRING AS chunk,
       r.value:RELATIVE_PATH::STRING AS relative_path,
       r.value:PDF_URL::STRING AS pdf_url,
       r.value:CHAPTER_URL::string as chapter_url
FROM search_results,
     LATERAL FLATTEN(input => search_results.results) r;
