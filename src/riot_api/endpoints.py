class RiotAPIEndpoints:
    """Riot API endpoint builder for TFT NA region.

    Base URLs:
    - Regional (for league data): https://na1.api.riotgames.com
    - Platform (for match data): https://americas.api.riotgames.com
    """

    # Hardcoded for NA region
    REGIONAL_BASE = "https://na1.api.riotgames.com"
    PLATFORM_BASE = "https://americas.api.riotgames.com"

    # League Endpoints
    @classmethod
    def get_master_league(cls) -> str:
        """Get Master tier league endpoint.

        Returns all players in Master tier for NA region.

        Returns:
            Full URL for Master league endpoint
        """
        return f"{cls.REGIONAL_BASE}/tft/league/v1/master"

    @classmethod
    def get_grandmaster_league(cls) -> str:
        """Get Grandmaster tier league endpoint.

        Returns all players in Grandmaster tier for NA region.

        Returns:
            Full URL for Grandmaster league endpoint
        """
        return f"{cls.REGIONAL_BASE}/tft/league/v1/grandmaster"

    @classmethod
    def get_challenger_league(cls) -> str:
        """Get Challenger tier league endpoint.

        Returns all players in Challenger tier for NA region.

        Returns:
            Full URL for Challenger league endpoint
        """
        return f"{cls.REGIONAL_BASE}/tft/league/v1/challenger"

    # Match Endpoints
    @classmethod
    def get_match_ids_by_puuid(cls, puuid: str, count: int = 20, start: int = 0) -> str:
        """Get match IDs by PUUID endpoint.

        Args:
            puuid: Player UUID
            count: Number of match IDs to return (max 20, default 20)
            start: Starting index for pagination (default 0)

        Returns:
            Full URL for match IDs endpoint with query parameters
        """
        # Enforce Riot API limit
        count = min(count, 20)
        return f"{cls.PLATFORM_BASE}/tft/match/v1/matches/by-puuid/{puuid}/ids?start={start}&count={count}"

    @classmethod
    def get_match_by_id(cls, match_id: str) -> str:
        """Get match details by match ID endpoint.

        Args:
            match_id: TFT match ID (format: NA1_1234567890)

        Returns:
            Full URL for match details endpoint
        """
        return f"{cls.PLATFORM_BASE}/tft/match/v1/matches/{match_id}"
