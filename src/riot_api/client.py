import time
import logging
from typing import Dict, List, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .rate_limiter import RateLimiter
from .endpoints import RiotAPIEndpoints


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RiotAPIError(Exception):
    """Base exception for Riot API errors."""
    pass


class RateLimitError(RiotAPIError):
    """Raised when rate limit is exceeded."""
    pass


class DataNotFoundError(RiotAPIError):
    """Raised when requested data is not found (404)."""
    pass


class RiotAPIClient:
    """High-level client for Riot Games TFT API (NA region).

    Features:
    - Automatic rate limiting (20 req/sec, 100 req/2min)
    - Retry logic for transient failures
    - Proper error handling
    - Connection pooling
    - Request/response logging
    """

    def __init__(
        self,
        api_key: str,
        max_retries: int = 3,
        timeout: int = 30
    ):
        """Initialize Riot API client for NA region.

        Args:
            api_key: Riot Games API key
            max_retries: Maximum number of retries for failed requests (default 3)
            timeout: Request timeout in seconds (default 30)
        """
        self.api_key = api_key
        self.timeout = timeout

        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            short_limit=20,
            short_window=1.0,
            long_limit=100,
            long_window=120.0
        )

        # Configure HTTP session with retries and connection pooling
        self.session = self._create_session(max_retries)

        # Request headers
        self.headers = {
            "X-Riot-Token": self.api_key,
            "Accept": "application/json"
        }

        logger.info("Initialized RiotAPIClient for NA region")

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create HTTP session with retry logic and connection pooling.

        Args:
            max_retries: Maximum number of retries

        Returns:
            Configured requests Session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        # Mount adapter with retry strategy
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make rate-limited API request with error handling.

        Args:
            url: Full API endpoint URL
            params: Optional query parameters

        Returns:
            JSON response as dictionary

        Raises:
            RateLimitError: If rate limit is exceeded
            DataNotFoundError: If data is not found (404)
            RiotAPIError: For other API errors
        """
        # Acquire rate limit token (blocks if needed)
        self.rate_limiter.acquire()

        try:
            logger.debug(f"GET {url}")
            response = self.session.get(
                url,
                headers=self.headers,
                params=params,
                timeout=self.timeout
            )

            # Handle different status codes
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise DataNotFoundError(f"Data not found: {url}")
            elif response.status_code == 429:
                # Rate limit exceeded - should be rare due to our rate limiter
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                raise RateLimitError(f"Rate limit exceeded. Retry after {retry_after}s")
            elif response.status_code == 403:
                raise RiotAPIError("API key invalid or expired")
            else:
                raise RiotAPIError(
                    f"API request failed: {response.status_code} - {response.text}"
                )

        except requests.exceptions.Timeout:
            raise RiotAPIError(f"Request timeout after {self.timeout} seconds")
        except requests.exceptions.ConnectionError as e:
            raise RiotAPIError(f"Connection error: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise RiotAPIError(f"Request failed: {str(e)}")

    # League Endpoints

    def get_master_league(self) -> Dict[str, Any]:
        """Fetch Master tier leaderboard for NA.

        Returns:
            Dictionary containing Master tier players with fields:
            - tier: "MASTER"
            - entries: List of player entries with summonerId, summonerName,
                      leaguePoints, wins, losses

        Example:
            {
                "tier": "MASTER",
                "leagueId": "...",
                "entries": [
                    {
                        "summonerId": "...",
                        "summonerName": "...",
                        "leaguePoints": 450,
                        "wins": 120,
                        "losses": 80
                    },
                    ...
                ]
            }
        """
        url = RiotAPIEndpoints.get_master_league()
        return self._make_request(url)

    def get_grandmaster_league(self) -> Dict[str, Any]:
        """Fetch Grandmaster tier leaderboard for NA.

        Returns:
            Dictionary containing Grandmaster tier players (same format as Master)
        """
        url = RiotAPIEndpoints.get_grandmaster_league()
        return self._make_request(url)

    def get_challenger_league(self) -> Dict[str, Any]:
        """Fetch Challenger tier leaderboard for NA.

        Returns:
            Dictionary containing Challenger tier players (same format as Master)
        """
        url = RiotAPIEndpoints.get_challenger_league()
        return self._make_request(url)

    # Match Endpoints

    def get_match_ids_by_puuid(
        self,
        puuid: str,
        count: int = 20,
        start: int = 0
    ) -> List[str]:
        """Fetch match IDs for a player by PUUID.

        Args:
            puuid: Player UUID
            count: Number of match IDs to return (max 20, default 20)
            start: Starting index for pagination (default 0)

        Returns:
            List of match IDs (e.g., ["NA1_1234567890", ...])

        Example:
            # Get last 20 matches
            match_ids = client.get_match_ids_by_puuid(puuid)

            # Get next 20 matches (pagination)
            older_matches = client.get_match_ids_by_puuid(puuid, start=20)
        """
        url = RiotAPIEndpoints.get_match_ids_by_puuid(puuid, count=count, start=start)
        return self._make_request(url)

    def get_match_by_id(self, match_id: str) -> Dict[str, Any]:
        """Fetch full match details by match ID.

        Args:
            match_id: TFT match ID (e.g., "NA1_1234567890")

        Returns:
            Dictionary containing complete match data with fields:
            - metadata: Match metadata (match_id, participants list)
            - info: Match info including:
                - game_datetime: Unix timestamp
                - game_length: Duration in seconds
                - game_version: Patch version
                - participants: List of 8 participant objects
                - queue_id: Queue type (1100 = ranked)
                - tft_set_number: TFT set number

        Example:
            {
                "metadata": {
                    "match_id": "NA1_1234567890",
                    "participants": ["puuid1", "puuid2", ...]
                },
                "info": {
                    "game_datetime": 1640000000000,
                    "game_length": 1800.5,
                    "game_version": "12.10.1234",
                    "participants": [
                        {
                            "puuid": "...",
                            "placement": 1,
                            "level": 9,
                            "players_eliminated": 3,
                            "traits": [...],
                            "units": [...],
                            "augments": [...],
                            ...
                        },
                        ...
                    ],
                    ...
                }
            }
        """
        url = RiotAPIEndpoints.get_match_by_id(match_id)
        return self._make_request(url)

    def get_matches_bulk(
        self,
        match_ids: List[str],
        max_errors: int = 10
    ) -> List[Dict[str, Any]]:
        """Fetch multiple matches in bulk.

        Continues fetching even if some matches fail. Useful for processing
        large batches of match IDs where some may be invalid.

        Args:
            match_ids: List of match IDs to fetch
            max_errors: Maximum number of errors before stopping (default 10)

        Returns:
            List of match data dictionaries (successful fetches only)
        """
        matches = []
        error_count = 0

        logger.info(f"Fetching {len(match_ids)} matches in bulk...")

        for i, match_id in enumerate(match_ids):
            try:
                match_data = self.get_match_by_id(match_id)
                matches.append(match_data)

                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i + 1}/{len(match_ids)} matches fetched")

            except DataNotFoundError:
                logger.warning(f"Match not found: {match_id}")
                error_count += 1
            except RiotAPIError as e:
                logger.error(f"Error fetching match {match_id}: {str(e)}")
                error_count += 1

            if error_count >= max_errors:
                logger.error(f"Max errors ({max_errors}) reached. Stopping bulk fetch.")
                break

        logger.info(f"Bulk fetch complete: {len(matches)} matches fetched, {error_count} errors")
        return matches

    # Convenience Methods

    def get_all_masters_plus_players(self) -> List[Dict[str, Any]]:
        """Fetch all Masters+ players (Master, Grandmaster, Challenger) for NA.

        Returns:
            List of all player entries from all three tiers combined
        """
        logger.info("Fetching all Masters+ players...")

        all_players = []

        # Fetch each tier
        for tier_name, tier_method in [
            ("MASTER", self.get_master_league),
            ("GRANDMASTER", self.get_grandmaster_league),
            ("CHALLENGER", self.get_challenger_league)
        ]:
            try:
                tier_data = tier_method()
                entries = tier_data.get("entries", [])

                # Add tier to each player entry
                for entry in entries:
                    entry['tier'] = tier_name

                all_players.extend(entries)
                logger.info(f"Fetched {len(entries)} {tier_name} players")
            except RiotAPIError as e:
                logger.error(f"Error fetching {tier_name} tier: {str(e)}")

        logger.info(f"Total Masters+ players: {len(all_players)}")
        return all_players

    def get_grandmaster_plus_players(self) -> List[Dict[str, Any]]:
        """Fetch Grandmaster and Challenger players only (excludes Master).

        Returns:
            List of all player entries from Grandmaster and Challenger tiers
        """
        logger.info("Fetching Grandmaster+ players...")

        all_players = []

        # Fetch only GM and Challenger
        for tier_name, tier_method in [
            ("GRANDMASTER", self.get_grandmaster_league),
            ("CHALLENGER", self.get_challenger_league)
        ]:
            try:
                tier_data = tier_method()
                entries = tier_data.get("entries", [])

                # Add tier to each player entry
                for entry in entries:
                    entry['tier'] = tier_name

                all_players.extend(entries)
                logger.info(f"Fetched {len(entries)} {tier_name} players")
            except RiotAPIError as e:
                logger.error(f"Error fetching {tier_name} tier: {str(e)}")

        logger.info(f"Total Grandmaster+ players: {len(all_players)}")
        return all_players

    def close(self):
        """Close the HTTP session."""
        self.session.close()
        logger.info("RiotAPIClient session closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
