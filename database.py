import os
from supabase import create_client, Client
import pandas as pd
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
# For admin operations that need to bypass RLS, use service key
supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_SERVICE_KEY else supabase

class FPLDatabase:
    def __init__(self):
        self.client = supabase
        self.admin_client = supabase_admin
    
    def store_league_info(self, league_id: int, league_name: str) -> bool:
        """Store or update league information"""
        try:
            # Upsert league info (insert or update if exists)
            result = self.client.table('leagues').upsert({
                'id': league_id,
                'name': league_name,
                'updated_at': datetime.now().isoformat()
            }).execute()
            
            return len(result.data) > 0
        except Exception as e:
            print(f"Error storing league info: {e}")
            return False
    
    def store_players(self, league_id: int, players_df: pd.DataFrame) -> bool:
        """Store league players information"""
        try:
            # Prepare data for upsert
            players_data = []
            for _, player in players_df.iterrows():
                players_data.append({
                    'league_id': league_id,
                    'entry_id': player['entry'],
                    'player_name': player['player_name'],
                    'team_name': player['entry_name'],
                    'updated_at': datetime.now().isoformat()
                })
            
            # Upsert players (insert or update if exists)
            result = self.client.table('league_players').upsert(
                players_data,
                on_conflict='league_id,entry_id'
            ).execute()
            
            return len(result.data) > 0
        except Exception as e:
            print(f"Error storing players: {e}")
            return False
    
    def store_gameweek_data(self, gameweek_df: pd.DataFrame) -> bool:
        """Store gameweek data from your processed DataFrame - FIXED VERSION"""
        try:
            gameweek_data = []
            
            def safe_int(value, default=0):
                """Safely convert value to integer"""
                if pd.isna(value) or value is None or value == '':
                    return default
                try:
                    # Handle float strings like "0.0"
                    if isinstance(value, str) and '.' in value:
                        return int(float(value))
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            def safe_str(value, default=None):
                """Safely convert value to string"""
                if pd.isna(value) or value is None:
                    return default
                return str(value) if value != '' else default
            
            for _, row in gameweek_df.iterrows():
                # Extract gameweek number from your data structure
                gameweek = safe_int(row.get('gameweek', 1), 1)
                
                # Handle active_chip - it might be a list or single value
                active_chip = row.get('Active chip')
                if isinstance(active_chip, list):
                    active_chip = active_chip[0] if len(active_chip) > 0 and active_chip[0] is not None else None
                active_chip = safe_str(active_chip)
                
                gameweek_record = {
                    'league_id': safe_int(row.get('league_id')),
                    'entry_id': safe_int(row.get('Player Entry')),
                    'gameweek': gameweek,
                    'points': safe_int(row.get('points_1')),
                    'total_points': safe_int(row.get('Player Points')),
                    'points_net': safe_int(row.get('pointsnet_1')),
                    'bank': safe_int(row.get('bank_1')),
                    'team_value': safe_int(row.get('value_1', 0) * 10 if row.get('value_1') is not None else 0),
                    'transfers': safe_int(row.get('event_transfers_1')),
                    'transfers_cost': safe_int(row.get('event_transfers_cost_1')),
                    'points_on_bench': safe_int(row.get('points_on_bench_1')),
                    'captain': safe_str(row.get('Captain')),
                    'vice_captain': safe_str(row.get('Vice-captain')),
                    'active_chip': active_chip,
                    'updated_at': datetime.now().isoformat()
                }
                
                # Only add if we have required fields
                if gameweek_record['entry_id'] and gameweek_record['league_id']:
                    gameweek_data.append(gameweek_record)
            
            if not gameweek_data:
                print("No valid gameweek data to store")
                return False
            
            print(f"📊 Attempting to store {len(gameweek_data)} records...")
            
            # Upsert gameweek data
            result = self.client.table('gameweek_data').upsert(
                gameweek_data,
                on_conflict='entry_id,gameweek'
            ).execute()
            
            print(f"✅ Successfully stored {len(result.data)} records")
            return len(result.data) > 0
            
        except Exception as e:
            print(f"Error storing gameweek data: {e}")
            print(f"Sample data that failed: {gameweek_data[0] if gameweek_data else 'No data'}")
            return False
    
    def store_chip_usage(self, chips_df: pd.DataFrame) -> bool:
        """Store chip usage data"""
        try:
            if chips_df.empty:
                return True
                
            chips_data = []
            for _, chip in chips_df.iterrows():
                chips_data.append({
                    'league_id': chip.get('league_id'),
                    'entry_id': chip.get('entry_id'),
                    'chip_name': chip.get('name'),
                    'gameweek_used': chip.get('event'),
                })
            
            # Upsert chip data
            result = self.client.table('chip_usage').upsert(
                chips_data,
                on_conflict='entry_id,chip_name'
            ).execute()
            
            return len(result.data) >= 0  # Some might be duplicates, that's OK
            
        except Exception as e:
            print(f"Error storing chip usage: {e}")
            return False
    
    def get_league_standings(self, league_id: int, gameweek: Optional[int] = None) -> pd.DataFrame:
        """Get current league standings from database (super fast!)"""
        try:
            if gameweek:
                # Get specific gameweek data
                response = self.client.table('gameweek_data')\
                    .select('*, league_players(player_name, team_name)')\
                    .eq('league_id', league_id)\
                    .eq('gameweek', gameweek)\
                    .order('total_points', desc=True)\
                    .execute()
            else:
                # Use the pre-built view for latest standings
                response = self.client.table('league_standings')\
                    .select('*')\
                    .eq('league_id', league_id)\
                    .order('position')\
                    .execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting league standings: {e}")
            return pd.DataFrame()
    
    def get_player_history(self, entry_id: int) -> pd.DataFrame:
        """Get historical data for a specific player"""
        try:
            response = self.client.table('gameweek_data')\
                .select('*')\
                .eq('entry_id', entry_id)\
                .order('gameweek')\
                .execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting player history: {e}")
            return pd.DataFrame()
    
    def get_player_trends(self, entry_id: int) -> pd.DataFrame:
        """Get player trends using the pre-built view"""
        try:
            response = self.client.table('player_trends')\
                .select('*')\
                .eq('entry_id', entry_id)\
                .order('gameweek')\
                .execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting player trends: {e}")
            return pd.DataFrame()
    
    def get_captain_analysis(self, league_id: int, gameweek: Optional[int] = None) -> pd.DataFrame:
        """Get captain choices analysis for the league"""
        try:
            query = self.client.table('gameweek_data')\
                .select('captain, vice_captain, points, gameweek, league_players(player_name)')\
                .eq('league_id', league_id)
            
            if gameweek:
                query = query.eq('gameweek', gameweek)
            
            response = query.execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting captain analysis: {e}")
            return pd.DataFrame()
    
    def get_chip_usage_summary(self, league_id: int) -> pd.DataFrame:
        """Get chip usage summary for the league"""
        try:
            response = self.client.table('chip_usage')\
                .select('*, league_players(player_name)')\
                .eq('league_id', league_id)\
                .order('gameweek_used')\
                .execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting chip usage summary: {e}")
            return pd.DataFrame()
    
    def get_league_summary(self, league_id: int) -> Dict[str, Any]:
        """Get comprehensive league summary"""
        try:
            # Get basic league info
            league_info = self.client.table('leagues')\
                .select('*')\
                .eq('id', league_id)\
                .execute()
            
            # Get current standings
            standings = self.get_league_standings(league_id)
            
            # Get latest gameweek
            latest_gw = self.client.table('gameweek_data')\
                .select('gameweek')\
                .eq('league_id', league_id)\
                .order('gameweek', desc=True)\
                .limit(1)\
                .execute()
            
            return {
                'league_info': league_info.data[0] if league_info.data else {},
                'current_standings': standings.to_dict('records') if not standings.empty else [],
                'latest_gameweek': latest_gw.data[0]['gameweek'] if latest_gw.data else 1,
                'total_players': len(standings)
            }
            
        except Exception as e:
            print(f"Error getting league summary: {e}")
            return {}
    
    def update_current_gameweek(self, gameweek: int, is_finished: bool = False) -> bool:
        """Update the current gameweek tracker"""
        try:
            # Upsert current gameweek info
            result = self.client.table('current_gameweek').upsert({
                'gameweek': gameweek,
                'is_finished': is_finished,
                'updated_at': datetime.now().isoformat()
            }).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            print(f"Error updating current gameweek: {e}")
            return False
    
    def get_current_gameweek(self) -> int:
        """Get current gameweek from database"""
        try:
            response = self.client.table('current_gameweek')\
                .select('gameweek')\
                .order('updated_at', desc=True)\
                .limit(1)\
                .execute()
            
            if response.data:
                return response.data[0]['gameweek']
            return 1
            
        except Exception as e:
            print(f"Error getting current gameweek from DB: {e}")
            return 1
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            response = self.client.table('leagues').select('id').limit(1).execute()
            print("✅ Supabase connection successful!")
            return True
        except Exception as e:
            print(f"❌ Supabase connection failed: {e}")
            return False

# Initialize database instance
fpl_db = FPLDatabase()

# Test connection on import
if __name__ == "__main__":
    fpl_db.test_connection()