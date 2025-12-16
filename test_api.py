import json
from src.config import Config
from src.riot_api import RiotAPIClient

def main():
    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        print(f"Configuration error: {e}")
        return

    print("=" * 80)
    print("TFT API Client Test")
    print("=" * 80)

    # Initialize client
    with RiotAPIClient(api_key=Config.RIOT_API_KEY) as client:

        # Test 1: Fetch Masters+ leaderboard
        print("\nTEST 1: Fetching Masters+ leaderboard")
        print("-" * 80)

        try:
            all_players = client.get_all_masters_plus_players()
            print(f"Successfully fetched {len(all_players)} Masters+ players")

            # Show first player as example
            if all_players:
                first_player = all_players[0]
                print(f"\nExample player data:")
                print(f"  Available fields: {list(first_player.keys())}")
                print(json.dumps(first_player, indent=2))
        except Exception as e:
            print(f"Error fetching leaderboard: {e}")
            return

        # Test 2: Get match IDs for first player
        print("\n\nTEST 2: Fetching match IDs for first player")
        print("-" * 80)

        test_puuid = None
        match_ids = []

        try:
            if all_players:
                test_puuid = all_players[0]['puuid']
                print(f"Using PUUID: {test_puuid[:20]}...")

                match_ids = client.get_match_ids_by_puuid(test_puuid, count=5)
                print(f"Successfully fetched {len(match_ids)} match IDs")
                print(f"   First match ID: {match_ids[0] if match_ids else 'None'}")
        except Exception as e:
            print(f"Error fetching match IDs: {e}")
            return

        # Test 3: Fetch full match details
        print("\n\nTEST 3: Fetching full match details")
        print("-" * 80)

        try:
            if match_ids:
                first_match_id = match_ids[0]
                match_data = client.get_match_by_id(first_match_id)

                print(f"Successfully fetched match: {first_match_id}")
                print(f"\nMatch Info:")
                print(f"  Game datetime: {match_data['info']['game_datetime']}")
                print(f"  Game length: {match_data['info']['game_length']:.1f}s")
                print(f"  TFT Set: {match_data['info']['tft_set_number']}")
                print(f"  Queue ID: {match_data['info']['queue_id']}")

                # Show first participant's data (the winner)
                participants = match_data['info']['participants']
                winner = next(p for p in participants if p['placement'] == 1)

                print(f"\n1st Place Player:")
                print(f"  Level: {winner['level']}")
                print(f"  Gold left: {winner['gold_left']}")
                print(f"  Last round: {winner['last_round']}")

                print(f"\n  Augments ({len(winner.get('augments', []))}):")
                for aug in winner.get('augments', []):
                    print(f"    - {aug}")

                print(f"\n  Traits ({len(winner.get('traits', []))}):")
                for trait in winner.get('traits', [])[:5]:
                    print(f"    - {trait['name']}: {trait['num_units']} units (Tier {trait['tier_current']})")

                print(f"\n  Units ({len(winner.get('units', []))}):")
                for unit in winner.get('units', []):
                    items_str = ", ".join(unit.get('itemNames', [])) if unit.get('itemNames') else "No items"
                    print(f"    - {unit['character_id']} ({unit['tier']}★): {items_str}")

                print(f"\nThis is the match data")

        except Exception as e:
            print(f"Error fetching match details: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 80)
    print("API Client test complete")
    print("=" * 80)

if __name__ == "__main__":
    main()