{{ config(materialized='table') }}

SELECT 
    season_year,
    player_name,
    position,
    age,
    team_code,
    
    -- Advanced Efficiency Metrics
    player_efficiency_rating as PER,
    true_shooting_pct as TS_PCT,
    three_point_attempt_rate as ThreePAr,
    free_throw_rate as FTr,
    effective_field_goal_pct as eFG_PCT,
    
    -- Percentage Stats
    offensive_rebound_pct as ORB_PCT,
    defensive_rebound_pct as DRB_PCT,
    total_rebound_pct as TRB_PCT,
    assist_pct as AST_PCT,
    steal_pct as STL_PCT,
    block_pct as BLK_PCT,
    turnover_pct as TOV_PCT,
    usage_pct as USG_PCT,
    
    -- Win Shares
    offensive_win_shares as OWS,
    defensive_win_shares as DWS,
    win_shares as WS,
    win_shares_per_48 as WS_48,
    
    -- Plus/Minus Metrics
    offensive_box_plus_minus as OBPM,
    defensive_box_plus_minus as DBPM,
    box_plus_minus as BPM,
    value_over_replacement_player as VORP,
    
    -- Calculated Efficiency Ratios (need base fields from staging)
    CASE WHEN turnovers > 0 THEN assists / turnovers ELSE NULL END as assist_to_turnover_ratio,
    CASE WHEN personal_fouls > 0 THEN points / personal_fouls ELSE NULL END as points_per_foul,
    CASE WHEN field_goal_attempts > 0 THEN points / field_goal_attempts ELSE NULL END as points_per_fga,
    
    -- Usage and Efficiency Combined
    CASE 
        WHEN minutes_played > 0 AND games_played > 0 
        THEN (minutes_played / (games_played * 48)) * 100 
        ELSE NULL 
    END as minutes_usage_rate,
    
    -- Base fields needed for calculations (from staging)
    assists,
    turnovers,
    points,
    personal_fouls,
    field_goal_attempts,
    minutes_played,
    games_played,
    
    extraction_timestamp
FROM {{ ref('stg_nba_stats') }}

