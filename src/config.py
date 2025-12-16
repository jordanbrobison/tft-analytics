import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration manager for TFT pipeline."""

    # Riot API Configuration
    RIOT_API_KEY: str = os.getenv("RIOT_API_KEY", "")
    RIOT_REGION: str = os.getenv("RIOT_REGION", "na1")
    RIOT_PLATFORM: str = os.getenv("RIOT_PLATFORM", "americas")

    # Database Configuration (for future use)
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "tft_data")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    @classmethod
    def validate(cls) -> None:
        """Validate that required configuration is present.

        Raises:
            ValueError: If required configuration is missing
        """
        if not cls.RIOT_API_KEY:
            raise ValueError(
                "RIOT_API_KEY not found in environment variables. "
                "Please set it in your .env file."
            )

    @classmethod
    def get_database_url(cls) -> str:
        """Get PostgreSQL connection URL.

        Returns:
            Database connection URL string
        """
        return (
            f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        )
