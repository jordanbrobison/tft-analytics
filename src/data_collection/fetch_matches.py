import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Set
from sqlalchemy import text

from src.config import Config
from src.riot_api import RiotAPIClient
from src.database.connection import get_db_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_and_save_matches(
    limit_players: int = None,
    matches_per_player: int = 20
) -> Dict[str, Any]:
    """Fetch match data for all players and save to database.

    Args:
        limit_players: Limit number of players to process (None = all)
        matches_per_player: Number of matches to fetch per player (max 20)

    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'players_processed': 0,
        'match_ids_fetched': 0,
        'unique_matches': 0,
        'matches_saved': 0,
        'matches_skipped': 0,
        'api_calls': 0,
        'errors': 0
    }

    logger.info("=" * 80)
    logger.info("Steps 2-3: Fetching Match Data")
    logger.info("=" * 80)

    # Start collection log
    log_id = start_collection_log()

    try:
        # Get all players from database
        players = get_all_players(limit=limit_players)
        logger.info(f"Processing {len(players)} players...")

        if not players:
            logger.warning("No players found in database. Run fetch_leaderboard first.")
            return stats

        # Fetch match IDs for all players
        with RiotAPIClient(api_key=Config.RIOT_API_KEY) as client:
            all_match_ids = set()  # Use set for deduplication

            logger.info(f"Fetching match IDs ({matches_per_player} per player)...")

            for i, player in enumerate(players, 1):
                puuid = player['puuid']

                try:
                    match_ids = client.get_match_ids_by_puuid(
                        puuid,
                        count=matches_per_player
                    )
                    all_match_ids.update(match_ids)
                    stats['match_ids_fetched'] += len(match_ids)
                    stats['api_calls'] += 1
                    stats['players_processed'] += 1

                    if i % 50 == 0:
                        logger.info(
                            f"Progress: {i}/{len(players)} players, "
                            f"{len(all_match_ids)} unique matches"
                        )

                except Exception as e:
                    logger.error(f"Error fetching matches for {puuid}: {e}")
                    stats['errors'] += 1

            stats['unique_matches'] = len(all_match_ids)
            logger.info(f"Found {len(all_match_ids)} unique matches after deduplication")

            # Filter out already-fetched matches
            new_match_ids = filter_new_matches(all_match_ids)
            logger.info(f"Need to fetch {len(new_match_ids)} new matches")
            stats['matches_skipped'] = len(all_match_ids) - len(new_match_ids)

            # Fetch full match details
            if new_match_ids:
                logger.info("Fetching full match details...")
                saved = fetch_and_store_matches(client, new_match_ids, stats)
                stats['matches_saved'] = saved

        # Complete collection log
        complete_collection_log(log_id, 'completed', stats)

        logger.info("=" * 80)
        logger.info("Steps 2-3 Complete: Match data saved to database")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error during match fetch: {e}")
        import traceback
        traceback.print_exc()
        stats['errors'] += 1
        complete_collection_log(log_id, 'failed', stats, error_message=str(e))

    return stats


def get_all_players(limit: int = None) -> List[Dict[str, Any]]:
    """Get all players from raw_players table.

    Args:
        limit: Limit number of players (None = all)

    Returns:
        List of player dictionaries
    """
    engine = get_db_engine()

    with engine.connect() as conn:
        query = "SELECT puuid, tier, league_points FROM raw_players ORDER BY league_points DESC"

        if limit:
            query += f" LIMIT {limit}"

        result = conn.execute(text(query))
        return [{"puuid": row[0], "tier": row[1], "lp": row[2]} for row in result]


def filter_new_matches(match_ids: Set[str]) -> Set[str]:
    """Filter out matches that are already in the database.

    Args:
        match_ids: Set of match IDs to check

    Returns:
        Set of match IDs that need to be fetched
    """
    if not match_ids:
        return set()

    engine = get_db_engine()

    with engine.connect() as conn:
        # Check which matches already exist
        placeholders = ", ".join([f":id{i}" for i in range(len(match_ids))])
        query = f"SELECT match_id FROM raw_matches WHERE match_id IN ({placeholders})"

        params = {f"id{i}": mid for i, mid in enumerate(match_ids)}
        result = conn.execute(text(query), params)

        existing_ids = {row[0] for row in result}

    return match_ids - existing_ids


def fetch_and_store_matches(
    client: RiotAPIClient,
    match_ids: Set[str],
    stats: Dict[str, Any]
) -> int:
    """Fetch and store match details.

    Args:
        client: RiotAPIClient instance
        match_ids: Set of match IDs to fetch
        stats: Statistics dictionary to update

    Returns:
        Number of matches saved
    """
    engine = get_db_engine()
    saved_count = 0
    match_list = list(match_ids)

    for i, match_id in enumerate(match_list, 1):
        try:
            # Fetch match data
            match_data = client.get_match_by_id(match_id)
            stats['api_calls'] += 1

            # Extract metadata
            info = match_data.get('info', {})
            game_datetime = info.get('game_datetime')
            game_length = info.get('game_length')
            tft_set = info.get('tft_set_number')
            queue_id = info.get('queue_id')

            # Save to database
            with engine.begin() as conn:
                # Insert match
                conn.execute(text("""
                    INSERT INTO raw_matches (
                        match_id, match_data, game_datetime,
                        game_length, tft_set_number, queue_id, fetched_at
                    )
                    VALUES (
                        :match_id, CAST(:match_data AS jsonb), :game_datetime,
                        :game_length, :tft_set, :queue_id, :now
                    )
                    ON CONFLICT (match_id) DO NOTHING
                """), {
                    "match_id": match_id,
                    "match_data": json.dumps(match_data),
                    "game_datetime": game_datetime,
                    "game_length": game_length,
                    "tft_set": tft_set,
                    "queue_id": queue_id,
                    "now": datetime.now()
                })

                # Insert player-match relationships (only for known players)
                participants = match_data.get('metadata', {}).get('participants', [])
                for participant_puuid in participants:
                    # Check if this player is in our database
                    check_result = conn.execute(text("""
                        SELECT 1 FROM raw_players WHERE puuid = :puuid
                    """), {"puuid": participant_puuid})

                    if check_result.fetchone():
                        # Find placement for this participant
                        placement = None
                        for p in info.get('participants', []):
                            if p.get('puuid') == participant_puuid:
                                placement = p.get('placement')
                                break

                        conn.execute(text("""
                            INSERT INTO player_match_history (
                                puuid, match_id, placement, fetched_at
                            )
                            VALUES (:puuid, :match_id, :placement, :now)
                            ON CONFLICT (puuid, match_id) DO NOTHING
                        """), {
                            "puuid": participant_puuid,
                            "match_id": match_id,
                            "placement": placement,
                            "now": datetime.now()
                        })

            saved_count += 1

            # Progress logging
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(match_list)} matches saved")

        except Exception as e:
            logger.error(f"Error fetching match {match_id}: {e}")
            stats['errors'] += 1

    return saved_count


def start_collection_log() -> int:
    """Start a collection log entry."""
    engine = get_db_engine()

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO data_collection_log (collection_type, status, started_at)
            VALUES ('matches', 'started', :now)
            RETURNING id
        """), {"now": datetime.now()})

        log_id = result.fetchone()[0]
        logger.info(f"Started collection log (ID: {log_id})")
        return log_id


def complete_collection_log(
    log_id: int,
    status: str,
    stats: Dict[str, Any],
    error_message: str = None
):
    """Complete a collection log entry."""
    engine = get_db_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE data_collection_log
            SET status = :status,
                players_processed = :players,
                matches_fetched = :matches,
                api_calls_made = :api_calls,
                error_message = :error,
                completed_at = :now
            WHERE id = :log_id
        """), {
            "status": status,
            "players": stats.get('players_processed', 0),
            "matches": stats.get('matches_saved', 0),
            "api_calls": stats.get('api_calls', 0),
            "error": error_message,
            "now": datetime.now(),
            "log_id": log_id
        })

    logger.info(f"Completed collection log (ID: {log_id}, status: {status})")


if __name__ == "__main__":
    import sys

    # Parse command line args
    limit = None
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
        logger.info(f"Limiting to {limit} players for testing")

    stats = fetch_and_save_matches(limit_players=limit)

    print("\nCollection Statistics:")
    print(f"  Players processed: {stats['players_processed']}")
    print(f"  Match IDs fetched: {stats['match_ids_fetched']}")
    print(f"  Unique matches: {stats['unique_matches']}")
    print(f"  Matches saved: {stats['matches_saved']}")
    print(f"  Matches skipped (already in DB): {stats['matches_skipped']}")
    print(f"  API calls made: {stats['api_calls']}")
    print(f"  Errors: {stats['errors']}")
