-- Query to deduplicate game_details table
-- This query removes duplicate records based on game_id and player_id combination
-- Using ROW_NUMBER() window function to identify and keep only the first occurrence

-- Option 1: Using ROW_NUMBER() to keep the first occurrence (recommended)
WITH deduplicated_game_details AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, player_id 
            ORDER BY game_id, player_id
        ) as row_num
    FROM game_details
)
SELECT 
    game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO",
    pf,
    pts,
    plus_minus
FROM deduplicated_game_details
WHERE row_num = 1;

-- Option 2: Using DISTINCT ON (PostgreSQL specific) - more efficient for large datasets
-- This keeps the first row for each game_id, player_id combination based on natural order
/*
SELECT DISTINCT ON (game_id, player_id)
    game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO",
    pf,
    pts,
    plus_minus
FROM game_details
ORDER BY game_id, player_id;
*/

-- Option 3: Create a new deduplicated table
/*
CREATE TABLE game_details_deduplicated AS
WITH deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, player_id 
            ORDER BY game_id, player_id
        ) as row_num
    FROM game_details
)
SELECT 
    game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO",
    pf,
    pts,
    plus_minus
FROM deduplicated
WHERE row_num = 1;
*/

-- To check for duplicates before running the deduplication:
/*
SELECT 
    game_id, 
    player_id, 
    COUNT(*) as duplicate_count
FROM game_details
GROUP BY game_id, player_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
*/
