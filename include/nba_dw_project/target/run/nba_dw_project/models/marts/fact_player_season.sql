
  
    

create or replace transient table MLDS430.MARMOT_FINAL.fact_player_season
    
    
    
    as (

SELECT 
    season_year,
    player_name,
    position,
    age,
    team_code,
    games_played,
    games_started,
    minutes_played,
    
    -- Core Statistics
    points,
    assists,
    total_rebounds,
    steals,
    blocks,
    turnovers,
    personal_fouls,
    
    -- Shooting Stats
    field_goals,
    field_goal_attempts,
    field_goal_pct,
    three_pointers,
    three_point_attempts,
    three_point_pct,
    two_pointers,
    two_point_attempts,
    two_point_pct,
    effective_field_goal_pct,
    free_throws,
    free_throw_attempts,
    free_throw_pct,
    
    -- Rebounding
    offensive_rebounds,
    defensive_rebounds,
    
    -- Per-Game Metrics (CALCULATED)
    CASE WHEN games_played > 0 THEN points / games_played ELSE NULL END as points_per_game,
    CASE WHEN games_played > 0 THEN assists / games_played ELSE NULL END as assists_per_game,
    CASE WHEN games_played > 0 THEN total_rebounds / games_played ELSE NULL END as rebounds_per_game,
    CASE WHEN games_played > 0 THEN steals / games_played ELSE NULL END as steals_per_game,
    CASE WHEN games_played > 0 THEN blocks / games_played ELSE NULL END as blocks_per_game,
    CASE WHEN games_played > 0 THEN turnovers / games_played ELSE NULL END as turnovers_per_game,
    
    -- Per-36 Minutes Metrics (CALCULATED)
    CASE WHEN minutes_played > 0 THEN (points / minutes_played) * 36 ELSE NULL END as points_per_36min,
    CASE WHEN minutes_played > 0 THEN (assists / minutes_played) * 36 ELSE NULL END as assists_per_36min,
    CASE WHEN minutes_played > 0 THEN (total_rebounds / minutes_played) * 36 ELSE NULL END as rebounds_per_36min,
    CASE WHEN minutes_played > 0 THEN (steals / minutes_played) * 36 ELSE NULL END as steals_per_36min,
    CASE WHEN minutes_played > 0 THEN (blocks / minutes_played) * 36 ELSE NULL END as blocks_per_36min,
    
    extraction_timestamp
FROM MLDS430.MARMOT_FINAL.stg_nba_stats
    )
;


  