CREATE TABLE IF NOT EXISTS raw_players (
    puuid VARCHAR(78) PRIMARY KEY,
    league_points INTEGER NOT NULL,
    rank VARCHAR(10),
    wins INTEGER,
    losses INTEGER,
    veteran BOOLEAN,
    inactive BOOLEAN,
    fresh_blood BOOLEAN,
    hot_streak BOOLEAN,
    tier VARCHAR(20) NOT NULL,  -- 'MASTER', 'GRANDMASTER', 'CHALLENGER'
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for querying by tier and LP
CREATE INDEX IF NOT EXISTS idx_players_tier_lp
    ON raw_players(tier, league_points DESC);

-- Index for fetched_at (for tracking data freshness)
CREATE INDEX IF NOT EXISTS idx_players_fetched_at
    ON raw_players(fetched_at DESC);


-- Table: raw_matches
-- Stores complete match data as JSONB
CREATE TABLE IF NOT EXISTS raw_matches (
    match_id VARCHAR(50) PRIMARY KEY,
    match_data JSONB NOT NULL,
    game_datetime BIGINT NOT NULL,  -- Unix timestamp from API
    game_length FLOAT,
    tft_set_number INTEGER,
    queue_id INTEGER,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for querying by game datetime
CREATE INDEX IF NOT EXISTS idx_matches_datetime
    ON raw_matches(game_datetime DESC);

-- Index for querying by TFT set
CREATE INDEX IF NOT EXISTS idx_matches_set
    ON raw_matches(tft_set_number);

-- GIN index for JSONB queries (e.g., searching participants)
CREATE INDEX IF NOT EXISTS idx_matches_data_gin
    ON raw_matches USING GIN (match_data);


-- Table: player_match_history
-- Junction table tracking which players have which matches fetched
-- Useful for incremental updates
CREATE TABLE IF NOT EXISTS player_match_history (
    puuid VARCHAR(78) NOT NULL,
    match_id VARCHAR(50) NOT NULL,
    placement INTEGER,  -- Player's placement in this match (1-8)
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (puuid, match_id),
    FOREIGN KEY (puuid) REFERENCES raw_players(puuid) ON DELETE CASCADE,
    FOREIGN KEY (match_id) REFERENCES raw_matches(match_id) ON DELETE CASCADE
);

-- Index for querying player's match history
CREATE INDEX IF NOT EXISTS idx_player_matches_puuid
    ON player_match_history(puuid, fetched_at DESC);

-- Index for finding which players played in a match
CREATE INDEX IF NOT EXISTS idx_player_matches_match
    ON player_match_history(match_id);


-- Table: data_collection_log
-- Tracks data collection runs for monitoring
CREATE TABLE IF NOT EXISTS data_collection_log (
    id SERIAL PRIMARY KEY,
    collection_type VARCHAR(50) NOT NULL,  -- 'leaderboard', 'matches', 'secondary'
    status VARCHAR(20) NOT NULL,  -- 'started', 'completed', 'failed'
    players_processed INTEGER,
    matches_fetched INTEGER,
    api_calls_made INTEGER,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Index for querying recent runs
CREATE INDEX IF NOT EXISTS idx_collection_log_started
    ON data_collection_log(started_at DESC);
