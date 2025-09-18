# fpl_service.py - Improved version with better error handling

import pandas as pd
import json
import requests
from pandas import json_normalize
import urllib3
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time
from database import fpl_db

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class FPLService:
    def __init__(self):
        self.base_url = "https://fantasy.premierleague.com/api"
        self.session = requests.Session()
        self.session.verify = False
        self.max_retries = 3
        self.retry_delay = 1.0
        
    def get_current_gameweek(self) -> int:
        """Get current gameweek number from FPL API with retries"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(f'{self.base_url}/bootstrap-static/', timeout=10)
                response.raise_for_status()
                fplurl = response.json()
                fplgw = json_normalize(fplurl['events'])
                
                for i in range(len(fplgw)):
                    if fplgw.is_current[i]:
                        current_gw = int(fplgw.id[i])
                        return current_gw
                return 1
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    print(f"All attempts failed, using fallback")
                    return fpl_db.get_current_gameweek() or 1
    
    def get_league_info(self, league_id: int) -> Dict:
        """Get league information and standings with retries"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(f'{self.base_url}/leagues-classic/{league_id}/standings/', timeout=15)
                response.raise_for_status()
                league_data = response.json()
                return league_data
            except Exception as e:
                print(f"League info attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    print(f"Failed to get league {league_id} after {self.max_retries} attempts")
                    return {}
    
    def get_footballers_data(self) -> pd.DataFrame:
        """Get all FPL players data with retries"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(f'{self.base_url}/bootstrap-static/', timeout=15)
                response.raise_for_status()
                bootstrap_data = response.json()
                return pd.DataFrame.from_records(bootstrap_data['elements'])
            except Exception as e:
                print(f"Footballers data attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    return pd.DataFrame()
    
    def get_player_history(self, entry_id: int) -> Dict:
        """Get player's full history with retries"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(f'{self.base_url}/entry/{entry_id}/history/', timeout=10)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    print(f"Failed to get player {entry_id} history after {self.max_retries} attempts: {e}")
                    return {}
    
    def get_player_gameweek_picks(self, entry_id: int, gameweek: int) -> Dict:
        """Get player's picks for specific gameweek with retries"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(f'{self.base_url}/entry/{entry_id}/event/{gameweek}/picks/', timeout=10)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    print(f"Failed to get player {entry_id} GW {gameweek} picks: {e}")
                    return {}
    
    def process_league_data_normalized(self, league_id: int, store_in_db: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process league data using normalized schema with PROPER ordering and error handling
        """
        print(f"Processing league {league_id} with normalized schema...")
        
        dfresults = pd.DataFrame()
        dfresultschips = pd.DataFrame()
        
        try:
            # Step 1: Update current gameweek
            fpl_db.update_current_gameweek()
            current_gw = fpl_db.get_current_gameweek()
            print(f"Current gameweek: {current_gw}")
            
            # Step 2: Get league data
            league_data = self.get_league_info(league_id)
            if not league_data or 'standings' not in league_data:
                print("❌ No league data found")
                return dfresults, dfresultschips
            
            # Step 3: Store league info FIRST
            if store_in_db:
                league_name = league_data.get('league', {}).get('name', f'League {league_id}')
                league_success = fpl_db.store_league_info(league_id, league_name)
                print(f"League info stored: {'✅' if league_success else '❌'} - {league_name}")
            
            # Step 4: Process league standings
            dfleague = pd.DataFrame.from_records(league_data['standings']['results'])
            number_players = len(dfleague.index)
            print(f"Found {number_players} players in league")
            
            if number_players == 0:
                print("❌ No players found in league")
                return dfresults, dfresultschips
            
            # Step 5: CRITICAL - Store global players FIRST (they must exist before foreign key references)
            if store_in_db:
                print("🔄 Step 1/4: Storing global players (required for foreign keys)...")
                players_success = fpl_db.store_global_players(dfleague)
                print(f"Global players stored: {'✅' if players_success else '❌'}")
                
                if not players_success:
                    print("❌ CRITICAL: Failed to store global players - cannot proceed with gameweek data")
                    return dfresults, dfresultschips
                
                # Step 6: Store league memberships (depends on global_players)
                print("🔄 Step 2/4: Storing league memberships...")
                memberships_success = fpl_db.store_league_memberships(league_id, dfleague)
                print(f"League memberships stored: {'✅' if memberships_success else '❌'}")
            
            # Step 7: Get and store footballers data (independent, can be done anytime)
            print("🔄 Step 3/4: Getting FPL footballers data...")
            dffootballers = self.get_footballers_data()
            if dffootballers.empty:
                print("⚠️  Warning: Could not get footballers data - captain names may be missing")
                # Don't return here - we can still process without captain names
            
            if store_in_db and not dffootballers.empty:
                footballers_success = fpl_db.store_fpl_footballers(dffootballers)
                print(f"FPL footballers stored: {'✅' if footballers_success else '❌'}")
            
            # Step 8: Process each player's gameweek data (AFTER players are stored)
            print("🔄 Step 4/4: Processing individual player gameweek data...")
            
            successful_players = 0
            failed_players = 0
            
            for j in range(number_players):
                entry_id = dfleague.entry[j]
                player_name = dfleague.player_name[j]
                print(f"Processing player {j+1}/{number_players}: {player_name} (ID: {entry_id})")
                
                try:
                    # Get player history
                    player_json = self.get_player_history(entry_id)
                    if not player_json or 'current' not in player_json:
                        print(f"⚠️  Warning: No data for player {player_name}")
                        failed_players += 1
                        continue
                    
                    # Process current season data
                    dfplayer = pd.DataFrame.from_records(player_json['current'])
                    if dfplayer.empty:
                        failed_players += 1
                        continue
                    
                    # Calculate points net
                    points_net = dfplayer['points'] - dfplayer['event_transfers_cost']
                    dfplayer.insert(2, 'pointsnet', points_net, True)
                    dfplayer['value'] = dfplayer['value'] / 10
                    
                    # Get captain & vice-captain with IDs
                    captain_id = None
                    vice_captain_id = None
                    captain_name = "Unknown"
                    vice_captain_name = "Unknown"
                    active_chip = None
                    
                    try:
                        player_gw_json = self.get_player_gameweek_picks(entry_id, current_gw)
                        if player_gw_json and 'picks' in player_gw_json:
                            dfplayergw = pd.DataFrame.from_records(player_gw_json['picks'])
                            
                            if not dfplayergw.empty:
                                captain_picks = dfplayergw[dfplayergw.is_captain == True]
                                vice_captain_picks = dfplayergw[dfplayergw.is_vice_captain == True]
                                
                                if not captain_picks.empty:
                                    captain_id = int(captain_picks.element.iloc[0])
                                    if not dffootballers.empty:
                                        captain_row = dffootballers[dffootballers.id == captain_id]
                                        if not captain_row.empty:
                                            captain_name = captain_row.web_name.item()
                                
                                if not vice_captain_picks.empty:
                                    vice_captain_id = int(vice_captain_picks.element.iloc[0])
                                    if not dffootballers.empty:
                                        vice_captain_row = dffootballers[dffootballers.id == vice_captain_id]
                                        if not vice_captain_row.empty:
                                            vice_captain_name = vice_captain_row.web_name.item()
                                
                                if 'active_chip' in player_gw_json:
                                    active_chip = player_gw_json['active_chip']
                        
                    except Exception as picks_error:
                        print(f"⚠️  Warning: Could not get picks for {player_name}: {picks_error}")
                    
                    # Transform DataFrame to single row
                    dfplayer.index = dfplayer.index + 1
                    dfplayer_out = dfplayer.stack()
                    dfplayer_out.index = dfplayer_out.index.map('{0[1]}_{0[0]}'.format)
                    dfplayer = dfplayer_out.to_frame().T
                    
                    # Add player info
                    dfplayer.insert(0, 'Player Name', dfleague.player_name[j], True)
                    dfplayer.insert(1, 'Team Name', dfleague.entry_name[j], True)
                    dfplayer.insert(2, 'Player Entry', dfleague.entry[j], True)
                    dfplayer.insert(3, 'Player Points', dfleague.total[j], True)
                    
                    # Add captain info with IDs
                    dfplayer['Captain'] = captain_name
                    dfplayer['Vice-captain'] = vice_captain_name
                    dfplayer['captain_id'] = captain_id
                    dfplayer['vice_captain_id'] = vice_captain_id
                    dfplayer['Active chip'] = active_chip
                    dfplayer['league_id'] = league_id
                    dfplayer['gameweek'] = current_gw
                    
                    # Update results DataFrame
                    dfresults = pd.concat([dfresults, dfplayer], ignore_index=True)
                    successful_players += 1
                    
                    # Process chips
                    if 'chips' in player_json:
                        dfplayerchips = pd.DataFrame.from_records(player_json['chips'])
                        if not dfplayerchips.empty:
                            dfplayerchips.insert(0, 'Player Name', dfleague.player_name[j], True)
                            dfplayerchips.insert(1, 'Player Points', dfleague.total[j], True)
                            dfplayerchips['league_id'] = league_id
                            dfplayerchips['entry_id'] = entry_id
                            
                            dfresultschips = pd.concat([dfresultschips, dfplayerchips], ignore_index=True)
                    
                    # Rate limiting
                    time.sleep(0.1)
                    
                except Exception as player_error:
                    print(f"❌ Error processing player {player_name}: {player_error}")
                    failed_players += 1
                    continue
            
            print(f"Player processing complete: ✅ {successful_players} success, ❌ {failed_players} failed")
            
            # Step 9: Store gameweek data ONLY AFTER all players are confirmed in global_players
            if store_in_db and not dfresults.empty:
                print(f"🔄 Storing {len(dfresults)} gameweek records...")
                gameweek_success = fpl_db.store_gameweek_data_normalized(dfresults)
                print(f"Gameweek data stored: {'✅' if gameweek_success else '❌'}")
                
                if not dfresultschips.empty:
                    print(f"🔄 Storing {len(dfresultschips)} chip records...")
                    chip_success = fpl_db.store_chip_usage_normalized(dfresultschips)
                    print(f"Chip data stored: {'✅' if chip_success else '❌'}")
            
            print(f"✅ Processing complete! Processed {len(dfresults)} player records")
            return dfresults, dfresultschips
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in process_league_data_normalized for league {league_id}: {e}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
            return dfresults, dfresultschips
    
    # ... (rest of the methods remain the same as in the original file)
    
    def get_league_standings_from_db_normalized(self, league_id: int, gameweek: Optional[int] = None) -> Dict:
        """Get league standings with smart gameweek selection - FIXED JSON SERIALIZATION"""
        try:
            df = fpl_db.get_league_standings_normalized(league_id, gameweek)
            if df.empty:
                return {"standings": [], "message": "No data found - try running data collection first"}
            
            # DEBUG: Print available columns
            print(f"Available DataFrame columns: {list(df.columns)}")
            if not df.empty:
                print(f"Sample row data: {dict(df.iloc[0])}")
            
            standings = []
            selected_gameweek = df['selected_gameweek'].iloc[0] if 'selected_gameweek' in df.columns else gameweek
            
            for _, row in df.iterrows():
                player_name = str(row.get('player_name', 'Unknown Player'))
                team_name = str(row.get('team_name', 'Unknown Team'))
                
                # Debug
                gameweek_pts = row.get('gameweek_points', 0)
                print(f"Player {player_name}: gameweek_points = {gameweek_pts} (type: {type(gameweek_pts)})")

                standings.append({
                    "position": int(row.get('league_position', len(standings) + 1)) if pd.notna(row.get('league_position')) else len(standings) + 1,
                    "entry_id": int(row.get('entry_id', 0)) if pd.notna(row.get('entry_id')) else 0,
                    "player_name": player_name,
                    "team_name": team_name,
                    "total_points": int(row.get('total_points', 0)) if pd.notna(row.get('total_points')) else 0,
                    "gameweek_points": int(row.get('gameweek_points', 0)) if pd.notna(row.get('gameweek_points')) else 0,  # FIXED: Changed from 'gameweek_points' to 'points'
                    "captain": str(row.get('captain_name', '')) if pd.notna(row.get('captain_name')) and row.get('captain_name') else 'No Captain',
                    "vice_captain": str(row.get('vice_captain_name', '')) if pd.notna(row.get('vice_captain_name')) and row.get('vice_captain_name') else 'No Vice Captain',
                    "active_chip": str(row.get('active_chip', '')) if pd.notna(row.get('active_chip')) and row.get('active_chip') else None,
                    "gameweek": int(selected_gameweek) if pd.notna(selected_gameweek) else 0,
                    "transfers_cost": int(row.get('transfers_cost', 0)) if pd.notna(row.get('transfers_cost')) else 0,
                    "points_on_bench": int(row.get('points_on_bench', 0)) if pd.notna(row.get('points_on_bench')) else 0
                })
            
            # Sort by total_points and update positions
            standings.sort(key=lambda x: x['total_points'], reverse=True)
            for i, standing in enumerate(standings):
                standing['position'] = i + 1
            
            return {
                "league_id": int(league_id),
                "gameweek": int(selected_gameweek) if pd.notna(selected_gameweek) else "latest",
                "total_players": len(standings),
                "standings": standings,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Error getting normalized league standings: {e}")
            import traceback
            traceback.print_exc()
            return {"error": str(e), "standings": []}
    
    def get_captain_analysis_from_db_normalized(self, league_id: int) -> Dict:
        """Get captain analysis using database function - scalable approach"""
        try:
            df = fpl_db.get_captain_analysis_normalized(league_id)
            
            if df.empty:
                return {"analysis": [], "message": "No captain data found"}
            
            # Convert DataFrame to the expected format
            captain_analysis = []
            for _, row in df.iterrows():
                captain_analysis.append({
                    "player_id": int(row.get('captain_id', 0)),
                    "player_name": str(row.get('captain_name', 'Unknown')),
                    "times_captained": int(row.get('times_captained', 0)),
                    "total_points": int(row.get('total_points', 0)),
                    "average_points": float(row.get('average_points', 0.0)),
                    "best_performance": int(row.get('best_performance', 0)),
                    "worst_performance": int(row.get('worst_performance', 0))
                })
            
            # Get additional data for full analysis
            try:
                # Get all gameweek data for manager information
                response = fpl_db.client.table('gameweek_data_new')\
                    .select('''
                        entry_id,
                        gameweek,
                        points,
                        total_points,
                        captain_name,
                        vice_captain_name,
                        active_chip,
                        transfers_cost,
                        team_value,
                        points_on_bench,
                        global_players(player_name),
                        league_memberships(team_name)
                    ''')\
                    .eq('league_id', league_id)\
                    .execute()
                
                fpl_managers_data = []
                if response.data:
                    for record in response.data:
                        # Handle nested data
                        player_info = record.get('global_players', [])
                        membership_info = record.get('league_memberships', [])
                        
                        player_name = player_info[0].get('player_name', 'Unknown') if player_info else 'Unknown'
                        team_name = membership_info[0].get('team_name', 'Unknown') if membership_info else 'Unknown'
                        
                        fpl_managers_data.append({
                            "fpl_manager": player_name,
                            "team_name": team_name,
                            "entry_id": record.get('entry_id'),
                            "gameweek": record.get('gameweek', 0),
                            "total_points": record.get('total_points', 0),
                            "gameweek_points": record.get('points', 0),
                            "captain": record.get('captain_name') or 'No Captain',
                            "vice_captain": record.get('vice_captain_name') or 'No Vice Captain',
                            "transfers_cost": record.get('transfers_cost', 0),
                            "team_value": round(record.get('team_value', 0) / 10, 1) if record.get('team_value') else 0,
                            "active_chip": record.get('active_chip'),
                            "points_on_bench": record.get('points_on_bench', 0)
                        })
                
                # Sort data
                captain_analysis.sort(key=lambda x: x['total_points'], reverse=True)
                fpl_managers_data.sort(key=lambda x: x['total_points'], reverse=True)
                
                # Get latest gameweek data
                latest_gw = max((record['gameweek'] for record in fpl_managers_data), default=0)
                latest_gw_data = [record for record in fpl_managers_data if record['gameweek'] == latest_gw]
                latest_gw_data.sort(key=lambda x: x['gameweek_points'], reverse=True)
                
            except Exception as manager_error:
                print(f"Error getting manager data: {manager_error}")
                fpl_managers_data = []
                latest_gw = 0
                latest_gw_data = []
            
            return {
                "league_id": league_id,
                "latest_gameweek": latest_gw,
                "total_records": len(fpl_managers_data),
                "total_unique_captains": len(captain_analysis),
                "fpl_managers_data": fpl_managers_data,
                "captain_performance": captain_analysis,
                "latest_gameweek_captains": latest_gw_data,
                "summary": {
                    "most_popular_captain": captain_analysis[0]["player_name"] if captain_analysis else "None",
                    "highest_scoring_captain": max(captain_analysis, key=lambda x: x["average_points"])["player_name"] if captain_analysis else "None"
                }
            }
            
        except Exception as e:
            print(f"Error getting normalized captain analysis: {e}")
            import traceback  
            print(f"Full traceback: {traceback.format_exc()}")
            return {"error": str(e), "analysis": []}
    
    def get_player_cross_league_analysis(self, entry_id: int) -> Dict:
        """Get player's performance across all leagues"""
        try:
            df = fpl_db.get_player_cross_league_stats(entry_id)
            if df.empty:
                return {"leagues": [], "message": "No data found for this player"}
            
            leagues_data = []
            for league_id in df['league_id'].unique():
                league_data = df[df['league_id'] == league_id]
                league_info = league_data.iloc[0].get('leagues', {})
                
                if isinstance(league_info, list) and len(league_info) > 0:
                    league_info = league_info[0]
                
                leagues_data.append({
                    "league_id": league_id,
                    "league_name": league_info.get('name', f'League {league_id}'),
                    "total_gameweeks": len(league_data),
                    "best_gameweek": league_data['points'].max(),
                    "average_points": round(league_data['points'].mean(), 1),
                    "total_points": league_data['total_points'].iloc[-1] if not league_data.empty else 0
                })
            
            return {
                "entry_id": entry_id,
                "player_name": df.iloc[0].get('global_players', {}).get('player_name', 'Unknown'),
                "total_leagues": len(leagues_data),
                "leagues": leagues_data
            }
            
        except Exception as e:
            print(f"Error getting cross-league analysis: {e}")
            return {"error": str(e), "leagues": []}

# Initialize FPL service
fpl_service = FPLService()