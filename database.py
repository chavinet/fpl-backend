# database.py - Complete fix aligned with actual Supabase schema

import os
from supabase import create_client, Client
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv
from datetime import datetime
import traceback

# Load environment variables
load_dotenv()

# Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_SERVICE_KEY else supabase

class FPLDatabase:
    def __init__(self):
        self.client = supabase
        self.admin_client = supabase_admin

    def get_current_gameweek(self) -> Optional[int]:
        """Fetches current gameweek from FPL API with better error handling"""
        try:
            import requests
            import json
            from pandas import json_normalize
            
            response = requests.get(
                'https://fantasy.premierleague.com/api/bootstrap-static/', 
                verify=False,
                timeout=10
            )
            response.raise_for_status()
            
            fpl_data = json.loads(response.text)
            events_df = json_normalize(fpl_data['events'])
            
            for i, is_current in enumerate(events_df['is_current']):
                if is_current:
                    current_gw = int(events_df['id'].iloc[i])
                    print(f"Current gameweek identified: {current_gw}")
                    return current_gw
            
            print("Warning: No current gameweek found in API response")
            return 1  # Default fallback
            
        except Exception as e:
            print(f"Error fetching current gameweek: {e}")
            return 1  # Default fallback

    def update_current_gameweek(self) -> bool:
        """Updates the stored current gameweek"""
        try:
            current_gw = self.get_current_gameweek()
            if current_gw is not None:
                print(f"Current gameweek updated to: {current_gw}")
                return True
            else:
                print("Failed to update current gameweek")
                return False
        except Exception as e:
            print(f"Error updating current gameweek: {e}")
            return False

    def store_league_info(self, league_id: int, league_name: str) -> bool:
        """Store or update league information - aligned with schema"""
        try:
            league_record = {
                'id': league_id,
                'name': league_name,
                'updated_at': datetime.now().isoformat()  # leagues table has updated_at
            }
            
            result = self.client.table('leagues').upsert(
                league_record,
                on_conflict='id'
            ).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            print(f"Error storing league info: {e}")
            traceback.print_exc()
            return False

    def store_global_players(self, players_df: pd.DataFrame) -> bool:
        """Store players in global_players table - SCHEMA ALIGNED"""
        try:
            if players_df.empty:
                print("No players data to store")
                return True

            print(f"Attempting to store {len(players_df)} global players...")
            
            players_data = []
            current_time = datetime.now().isoformat()
            
            for _, player in players_df.iterrows():
                player_record = {
                    'entry_id': int(player['entry']),
                    'player_name': str(player['player_name']),
                    'current_team_name': str(player['entry_name']),
                    'last_updated': current_time  # Note: global_players uses 'last_updated', not 'updated_at'
                    # first_seen will be set by DEFAULT now() on first insert
                }
                players_data.append(player_record)
            
            if not players_data:
                print("No valid player records to store")
                return False

            print(f"Sample record: {players_data[0]}")
            
            # Try bulk upsert first
            try:
                result = self.client.table('global_players').upsert(
                    players_data,
                    on_conflict='entry_id'
                ).execute()
                
                print(f"Successfully stored {len(result.data)} global players via bulk upsert")
                return True
                
            except Exception as bulk_error:
                print(f"Bulk upsert failed: {bulk_error}")
                
                # Fallback to individual upserts
                return self._store_players_individually(players_data)
                
        except Exception as e:
            print(f"Error in store_global_players: {e}")
            traceback.print_exc()
            return False

    def _store_players_individually(self, players_data: List[Dict[str, Any]]) -> bool:
        """Fallback method to store players individually"""
        try:
            print("Trying individual player upserts...")
            success_count = 0
            errors = []
            
            for i, player_record in enumerate(players_data):
                try:
                    # Try insert first, then update if exists
                    try:
                        result = self.client.table('global_players').insert(player_record).execute()
                        success_count += 1
                    except Exception as insert_error:
                        if "duplicate key value violates unique constraint" in str(insert_error).lower():
                            # Player exists, try update
                            update_record = {k: v for k, v in player_record.items() if k != 'entry_id'}
                            result = self.client.table('global_players')\
                                .update(update_record)\
                                .eq('entry_id', player_record['entry_id'])\
                                .execute()
                            success_count += 1
                        else:
                            raise insert_error
                    
                    # Progress indicator
                    if (i + 1) % 5 == 0:
                        print(f"Progress: {i + 1}/{len(players_data)} players processed")
                        
                except Exception as player_error:
                    error_msg = f"Player {player_record.get('entry_id')}: {player_error}"
                    errors.append(error_msg)
                    print(f"Failed to store {error_msg}")
                    continue
            
            print(f"Individual inserts: {success_count}/{len(players_data)} players stored")
            
            if errors and len(errors) <= 3:
                print("Sample errors:")
                for error in errors[:3]:
                    print(f"  - {error}")
            
            return success_count > 0
            
        except Exception as e:
            print(f"Individual inserts also failed: {e}")
            return False

    def store_league_memberships(self, league_id: int, players_df: pd.DataFrame) -> bool:
        """Store league memberships - SCHEMA ALIGNED"""
        try:
            if players_df.empty:
                print("No membership data to store")
                return True

            memberships_data = []
            current_time = datetime.now().isoformat()
            
            for _, player in players_df.iterrows():
                membership_record = {
                    'league_id': int(league_id),
                    'entry_id': int(player['entry']),
                    'team_name': str(player['entry_name']),
                    'last_active': current_time  # Note: league_memberships uses 'last_active', not 'updated_at'
                    # joined_at will be set by DEFAULT now() on first insert
                }
                memberships_data.append(membership_record)
            
            print(f"Attempting to store {len(memberships_data)} league memberships...")
            print(f"Sample membership: {memberships_data[0] if memberships_data else 'No data'}")
            
            # Try bulk upsert
            try:
                result = self.client.table('league_memberships').upsert(
                    memberships_data,
                    on_conflict='league_id,entry_id'  # This matches the unique constraint
                ).execute()
                
                print(f"Successfully stored {len(result.data)} league memberships via bulk upsert")
                return True
                
            except Exception as bulk_error:
                print(f"Bulk membership upsert failed: {bulk_error}")
                
                # Check if it's just a "already exists" error
                if "duplicate key value violates unique constraint" in str(bulk_error).lower():
                    print("Memberships already exist, trying individual updates...")
                    return self._store_memberships_individually(memberships_data)
                else:
                    print(f"Other error: {bulk_error}")
                    return False
                
        except Exception as e:
            print(f"Error in store_league_memberships: {e}")
            traceback.print_exc()
            return False

    def _store_memberships_individually(self, memberships_data: List[Dict[str, Any]]) -> bool:
        """Fallback for individual membership storage"""
        try:
            print("Trying individual membership upserts...")
            success_count = 0
            
            for membership_record in memberships_data:
                try:
                    # Try insert first, then update if exists
                    try:
                        result = self.client.table('league_memberships').insert(membership_record).execute()
                        success_count += 1
                    except Exception as insert_error:
                        if "duplicate key value violates unique constraint" in str(insert_error).lower():
                            # Membership exists, try update
                            update_record = {k: v for k, v in membership_record.items() 
                                           if k not in ['league_id', 'entry_id']}
                            result = self.client.table('league_memberships')\
                                .update(update_record)\
                                .eq('league_id', membership_record['league_id'])\
                                .eq('entry_id', membership_record['entry_id'])\
                                .execute()
                            success_count += 1
                        else:
                            raise insert_error
                        
                except Exception as member_error:
                    print(f"Failed to store membership {membership_record.get('entry_id')}: {member_error}")
                    continue
            
            print(f"Individual membership inserts: {success_count}/{len(memberships_data)} memberships stored")
            return success_count > 0
            
        except Exception as e:
            print(f"Individual membership inserts failed: {e}")
            return False

    def store_fpl_footballers(self, footballers_df: pd.DataFrame) -> bool:
        """Store FPL footballers data - SCHEMA ALIGNED"""
        try:
            if footballers_df.empty:
                print("No footballers data to store")
                return True

            footballers_data = []
            current_time = datetime.now().isoformat()
            
            for _, player in footballers_df.iterrows():
                footballer_record = {
                    'id': int(player['id']),
                    'first_name': str(player.get('first_name', '')),
                    'second_name': str(player.get('second_name', '')),
                    'web_name': str(player['web_name']),
                    'team_id': int(player.get('team', 0)) if player.get('team') else None,
                    'element_type': int(player.get('element_type', 0)) if player.get('element_type') else None,
                    'now_cost': int(player.get('now_cost', 0)) if player.get('now_cost') else None,
                    'total_points': int(player.get('total_points', 0)) if player.get('total_points') else 0,
                    'form': float(player.get('form', 0)) if player.get('form') and str(player.get('form')).replace('.', '').isdigit() else None,
                    'selected_by_percent': float(player.get('selected_by_percent', 0)) if player.get('selected_by_percent') and str(player.get('selected_by_percent')).replace('.', '').isdigit() else None,
                    'updated_at': current_time  # fpl_footballers table has updated_at
                    # created_at will be set by DEFAULT now()
                }
                footballers_data.append(footballer_record)
            
            print(f"Attempting to store {len(footballers_data)} FPL footballers...")
            
            # Upsert footballers data
            result = self.client.table('fpl_footballers').upsert(
                footballers_data,
                on_conflict='id'
            ).execute()
            
            print(f"FPL footballers upsert result: {len(result.data)} records processed")
            return True
            
        except Exception as e:
            print(f"Error storing FPL footballers: {e}")
            traceback.print_exc()
            return False

    def store_gameweek_data_normalized(self, gameweek_df: pd.DataFrame) -> bool:
        """Store gameweek data using normalized schema - SCHEMA ALIGNED"""
        try:
            gameweek_data = []
            current_time = datetime.now().isoformat()
            
            def safe_int(value, default=0):
                if pd.isna(value) or value is None or value == '':
                    return default
                try:
                    if isinstance(value, str) and '.' in value:
                        return int(float(value))
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            def safe_str(value, default=None):
                if pd.isna(value) or value is None:
                    return default
                return str(value) if value != '' else default
            
            for _, row in gameweek_df.iterrows():
                gameweek = safe_int(row.get('gameweek', 1), 1)
                
                # Handle active_chip
                active_chip = row.get('Active chip')
                if isinstance(active_chip, list):
                    active_chip = active_chip[0] if len(active_chip) > 0 and active_chip[0] is not None else None
                active_chip = safe_str(active_chip)
                
                # Extract captain/vice-captain IDs
                captain_id = safe_int(row.get('captain_id')) if row.get('captain_id') is not None else None
                vice_captain_id = safe_int(row.get('vice_captain_id')) if row.get('vice_captain_id') is not None else None
                
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
                    'captain_id': captain_id,
                    'captain_name': safe_str(row.get('Captain')),
                    'vice_captain_id': vice_captain_id,
                    'vice_captain_name': safe_str(row.get('Vice-captain')),
                    'active_chip': active_chip,
                    'updated_at': current_time  # gameweek_data_new table has updated_at
                    # created_at will be set by DEFAULT now()
                }
                
                if gameweek_record['entry_id'] and gameweek_record['league_id']:
                    gameweek_data.append(gameweek_record)
            
            if not gameweek_data:
                print("No valid gameweek data to store")
                return False
            
            print(f"Attempting to store {len(gameweek_data)} records in normalized schema...")
            print(f"Sample gameweek record: {gameweek_data[0] if gameweek_data else 'No data'}")
            
            # Upsert to normalized table
            result = self.client.table('gameweek_data_new').upsert(
                gameweek_data,
                on_conflict='league_id,entry_id,gameweek'  # Matches the unique constraint
            ).execute()
            
            print(f"Successfully stored {len(result.data)} records")
            return len(result.data) > 0
            
        except Exception as e:
            print(f"Error storing normalized gameweek data: {e}")
            traceback.print_exc()
            return False

    def store_chip_usage_normalized(self, chips_df: pd.DataFrame) -> bool:
        """Store chip usage in normalized schema - SCHEMA ALIGNED"""
        try:
            if chips_df.empty:
                print("No chip data to store")
                return True
                
            chips_data = []
            for _, chip in chips_df.iterrows():
                chip_record = {
                    'league_id': int(chip.get('league_id')),
                    'entry_id': int(chip.get('entry_id')),
                    'chip_name': str(chip.get('name')),
                    'gameweek_used': int(chip.get('event'))
                    # created_at will be set automatically by DEFAULT now()
                    # Note: chip_usage_new table doesn't have updated_at
                }
                chips_data.append(chip_record)
            
            print(f"Attempting to store {len(chips_data)} chip usage records...")
            
            result = self.client.table('chip_usage_new').upsert(
                chips_data,
                on_conflict='league_id,entry_id,chip_name'  # Matches the unique constraint
            ).execute()
            
            print(f"Chip usage upsert result: {len(result.data)} records processed")
            return True
            
        except Exception as e:
            print(f"Error storing normalized chip usage: {e}")
            traceback.print_exc()
            return False

    def get_league_standings_normalized(self, league_id: int, gameweek: Optional[int] = None) -> pd.DataFrame:
        """Get league standings using normalized schema with proper relationships"""
        try:
            # Build the query
            if gameweek:
                response = self.client.table('gameweek_data_new')\
                    .select('''
                        entry_id,
                        gameweek,
                        points,
                        total_points,
                        captain_name,
                        vice_captain_name,
                        active_chip,
                        transfers_cost,
                        points_on_bench,
                        team_value,
                        global_players(player_name),
                        league_memberships(team_name)
                    ''')\
                    .eq('league_id', league_id)\
                    .eq('gameweek', gameweek)\
                    .order('total_points', desc=True)\
                    .execute()
            else:
                # Get current gameweek
                current_gw = self.get_current_gameweek() or 1
                
                response = self.client.table('gameweek_data_new')\
                    .select('''
                        entry_id,
                        gameweek,
                        points,
                        total_points,
                        captain_name,
                        vice_captain_name,
                        active_chip,
                        transfers_cost,
                        points_on_bench,
                        team_value,
                        global_players(player_name),
                        league_memberships(team_name)
                    ''')\
                    .eq('league_id', league_id)\
                    .eq('gameweek', current_gw)\
                    .order('total_points', desc=True)\
                    .execute()
            
            if response.data:
                df = pd.DataFrame(response.data)
                
                # Add league_position
                df = df.sort_values('total_points', ascending=False).reset_index(drop=True)
                df['league_position'] = df.index + 1
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting normalized league standings: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    def get_captain_analysis_normalized(self, league_id: int) -> pd.DataFrame:
        """Get captain analysis using normalized schema"""
        try:
            response = self.client.table('gameweek_data_new')\
                .select('''
                    captain_id,
                    captain_name,
                    points
                ''')\
                .eq('league_id', league_id)\
                .not_.is_('captain_id', 'null')\
                .not_.is_('captain_name', 'null')\
                .execute()
            
            if response.data:
                # Process manually for aggregation
                captain_stats = {}
                for record in response.data:
                    captain_name = record['captain_name']
                    if captain_name not in captain_stats:
                        captain_stats[captain_name] = {
                            'captain_id': record['captain_id'],
                            'captain_name': captain_name,
                            'times_captained': 0,
                            'total_points': 0,
                            'points_list': []
                        }
                    
                    captain_stats[captain_name]['times_captained'] += 1
                    captain_stats[captain_name]['total_points'] += record['points']
                    captain_stats[captain_name]['points_list'].append(record['points'])
                
                # Convert to DataFrame
                captain_data = []
                for stats in captain_stats.values():
                    if stats['times_captained'] > 0:
                        captain_data.append({
                            'captain_id': stats['captain_id'],
                            'captain_name': stats['captain_name'],
                            'times_captained': stats['times_captained'],
                            'total_points': stats['total_points'],
                            'average_points': round(stats['total_points'] / stats['times_captained'], 1),
                            'best_performance': max(stats['points_list']) if stats['points_list'] else 0,
                            'worst_performance': min(stats['points_list']) if stats['points_list'] else 0
                        })
                
                return pd.DataFrame(captain_data).sort_values('total_points', ascending=False)
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting normalized captain analysis: {e}")
            return pd.DataFrame()

    def get_player_cross_league_stats(self, entry_id: int) -> pd.DataFrame:
        """Get player's performance across all leagues"""
        try:
            response = self.client.table('gameweek_data_new')\
                .select('*, leagues(name), league_memberships(team_name)')\
                .eq('entry_id', entry_id)\
                .order('gameweek')\
                .execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error getting cross-league stats: {e}")
            return pd.DataFrame()

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            response = self.client.table('leagues').select('id').limit(1).execute()
            print("Supabase connection successful!")
            return True
        except Exception as e:
            print(f"Supabase connection failed: {e}")
            return False

# Initialize database instance
fpl_db = FPLDatabase()

# Test connection on import
if __name__ == "__main__":
    fpl_db.test_connection()