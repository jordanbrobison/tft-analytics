import logging
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import text

from src.config import Config
from src.riot_api import RiotAPIClient
from src.database.connection import get_db_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_and_save_leaderboard() -> Dict[str, Any]:
    """Fetch Masters+ leaderboard and save to database.

    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'players_fetched': 0,
        'players_inserted': 0,
        'players_updated': 0,
        'api_calls': 0,
        'errors': 0
    }

    logger.info("=" * 80)
    logger.info("Step 1: Fetching Grandmaster+ Leaderboard")
    logger.info("=" * 80)

    # Start collection log
    log_id = start_collection_log()

    try:
        # Fetch leaderboard data
        with RiotAPIClient(api_key=Config.RIOT_API_KEY) as client:
            logger.info("Fetching Grandmaster+ players (excluding Masters)...")
            all_players = client.get_grandmaster_plus_players()
            stats['players_fetched'] = len(all_players)
            stats['api_calls'] = 2  # Grandmaster, Challenger only

            logger.info(f"Fetched {len(all_players)} players from API")

        # Save to database
        if all_players:
            inserted, updated = save_players_to_db(all_players)
            stats['players_inserted'] = inserted
            stats['players_updated'] = updated

            logger.info(f"Inserted {inserted} new players, updated {updated} existing players")

        # Complete collection log
        complete_collection_log(log_id, 'completed', stats)

        logger.info("=" * 80)
        logger.info("Step 1 Complete: Grandmaster+ leaderboard saved to database")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error during leaderboard fetch: {e}")
        import traceback
        traceback.print_exc()
        stats['errors'] = 1
        complete_collection_log(log_id, 'failed', stats, error_message=str(e))

    return stats


def start_collection_log() -> int:
    """Start a collection log entry.

    Returns:
        Log entry ID
    """
    engine = get_db_engine()

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO data_collection_log (collection_type, status, started_at)
            VALUES ('leaderboard', 'started', :now)
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
    """Complete a collection log entry.

    Args:
        log_id: Log entry ID
        status: 'completed' or 'failed'
        stats: Collection statistics
        error_message: Error message if failed
    """
    engine = get_db_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE data_collection_log
            SET status = :status,
                players_processed = :players,
                api_calls_made = :api_calls,
                error_message = :error,
                completed_at = :now
            WHERE id = :log_id
        """), {
            "status": status,
            "players": stats.get('players_fetched', 0),
            "api_calls": stats.get('api_calls', 0),
            "error": error_message,
            "now": datetime.now(),
            "log_id": log_id
        })

    logger.info(f"Completed collection log (ID: {log_id}, status: {status})")


def save_players_to_db(players: List[Dict[str, Any]]) -> tuple[int, int]:
    """Save player data to raw_players table.

    Uses INSERT ... ON CONFLICT to handle updates.

    Args:
        players: List of player dictionaries from leaderboard API

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    engine = get_db_engine()
    inserted = 0
    updated = 0

    # Note: Leaderboard API doesn't return tier directly, we need to track which
    # API call each player came from. For now, we'll infer from LP thresholds
    # or pass tier as parameter when fetching

    with engine.begin() as conn:
        for player in players:
            # Determine tier (needs to be passed from API fetch context)
            # For now, we'll use a simple heuristic based on the data source
            tier = player.get('tier', 'MASTER')  # Default to MASTER

            result = conn.execute(text("""
                INSERT INTO raw_players (
                    puuid, league_points, rank, wins, losses,
                    veteran, inactive, fresh_blood, hot_streak,
                    tier, fetched_at, updated_at
                )
                VALUES (
                    :puuid, :lp, :rank, :wins, :losses,
                    :veteran, :inactive, :fresh_blood, :hot_streak,
                    :tier, :now, :now
                )
                ON CONFLICT (puuid)
                DO UPDATE SET
                    league_points = EXCLUDED.league_points,
                    rank = EXCLUDED.rank,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    veteran = EXCLUDED.veteran,
                    inactive = EXCLUDED.inactive,
                    fresh_blood = EXCLUDED.fresh_blood,
                    hot_streak = EXCLUDED.hot_streak,
                    tier = EXCLUDED.tier,
                    updated_at = EXCLUDED.updated_at
                RETURNING (xmax = 0) AS inserted
            """), {
                "puuid": player['puuid'],
                "lp": player.get('leaguePoints', 0),
                "rank": player.get('rank', 'I'),
                "wins": player.get('wins', 0),
                "losses": player.get('losses', 0),
                "veteran": player.get('veteran', False),
                "inactive": player.get('inactive', False),
                "fresh_blood": player.get('freshBlood', False),
                "hot_streak": player.get('hotStreak', False),
                "tier": tier,
                "now": datetime.now()
            })

            # xmax = 0 means INSERT, xmax > 0 means UPDATE
            is_insert = result.fetchone()[0]
            if is_insert:
                inserted += 1
            else:
                updated += 1

    return inserted, updated


if __name__ == "__main__":
    stats = fetch_and_save_leaderboard()

    print("\nCollection Statistics:")
    print(f"  Players fetched: {stats['players_fetched']}")
    print(f"  Players inserted: {stats['players_inserted']}")
    print(f"  Players updated: {stats['players_updated']}")
    print(f"  API calls made: {stats['api_calls']}")
    print(f"  Errors: {stats['errors']}")
