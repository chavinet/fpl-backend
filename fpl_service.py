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
        self.session.verify = False  # Match your original script
        
    def get_current_gameweek(self) -> int:
        """Get current gameweek number from FPL API"""
        try:
            response = self.session.get(f'{self.base_url}/bootstrap-static/')
            fplurl = response.json()
            fplgw = json_normalize(fplurl['events'])
            
            for i in range(len(fplgw)):
                if fplgw.is_current[i]:
                    current_gw = int(fplgw.id[i])
                    # Update database with current gameweek
                    fpl_db.update_current_gameweek(current_gw)
                    return current_gw
            return 1
        except Exception as e:
            print(f"Error getting current gameweek: {e}")
            # Fallback to database
            return fpl_db.get_current_gameweek()
    
    def get_league_info(self, league_id: int) -> Dict:
        """Get league information and standings"""
        try:
            response = self.session.get(f'{self.base_url}/leagues-classic/{league_id}/standings/')
            league_data = response.json()
            return league_data
        except Exception as e:
            print(f"Error getting league info: {e}")
            return {}
    
    def get_footballers_data(self) -> pd.DataFrame:
        """Get all FPL players data (for captain/vice-captain names)"""
        try:
            response = self.session.get(f'{self.base_url}/bootstrap-static/')
            bootstrap_data = response.json()
            return pd.DataFrame.from_records(bootstrap_data['elements'])
        except Exception as e:
            print(f"Error getting footballers data: {e}")
            return pd.DataFrame()
    
    def get_player_history(self, entry_id: int) -> Dict:
        """Get player's full history"""
        try:
            response = self.session.get(f'{self.base_url}/entry/{entry_id}/history/')
            return response.json()
        except Exception as e:
            print(f"Error getting player {entry_id} history: {e}")
            return {}
    
    def get_player_gameweek_picks(self, entry_id: int, gameweek: int) -> Dict:
        """Get player's picks for specific gameweek"""
        try:
            response = self.session.get(f'{self.base_url}/entry/{entry_id}/event/{gameweek}/picks/')
            return response.json()
        except Exception as e:
            print(f"Error getting player {entry_id} gameweek {gameweek} picks: {e}")
            return {}
    
    def process_league_data(self, league_id: int, store_in_db: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Your original logic enhanced with Supabase storage
        Returns (gameweek_data_df, chips_df)
        """
        print(f"🏈 Processing league {league_id}...")
        
        # Initialize DataFrames
        dfresults = pd.DataFrame()
        dfresultschips = pd.DataFrame()
        
        # Get current gameweek
        current_gw = self.get_current_gameweek()
        print(f"📅 Current gameweek: {current_gw}")
        
        # Get league data
        league_data = self.get_league_info(league_id)
        if not league_data or 'standings' not in league_data:
            print("❌ No league data found")
            return dfresults, dfresultschips
        
        # Store league info in database
        if store_in_db:
            league_name = league_data.get('league', {}).get('name', f'League {league_id}')
            success = fpl_db.store_league_info(league_id, league_name)
            print(f"💾 League info stored: {'✅' if success else '❌'}")
        
        # Process standings
        dfleague = pd.DataFrame.from_records(league_data['standings']['results'])
        number_players = len(dfleague.index)
        print(f"👥 Processing {number_players} players...")
        
        # Store players in database
        if store_in_db:
            success = fpl_db.store_players(league_id, dfleague)
            print(f"👤 Players stored: {'✅' if success else '❌'}")
        
        # Get footballers data for captain/vice-captain names
        dffootballers = self.get_footballers_data()
        if dffootballers.empty:
            print("⚠️ Warning: Could not get footballers data")
            return dfresults, dfresultschips
        
        # Process each player
        for j in range(number_players):
            entry_id = dfleague.entry[j]
            player_name = dfleague.player_name[j]
            print(f"🔄 Processing player {j+1}/{number_players}: {player_name}")
            
            try:
                # Get player history
                player_json = self.get_player_history(entry_id)
                if not player_json or 'current' not in player_json:
                    print(f"⚠️ Warning: No data for player {player_name}")
                    continue
                
                # Process current season data
                dfplayer = pd.DataFrame.from_records(player_json['current'])
                if dfplayer.empty:
                    continue
                
                # Calculate points net (your original logic)
                points_net = dfplayer['points'] - dfplayer['event_transfers_cost']
                dfplayer.insert(2, 'pointsnet', points_net, True)
                dfplayer['value'] = dfplayer['value'] / 10
                
                # Get captain & vice-captain for current gameweek
                captain_name = "Unknown"
                vice_captain_name = "Unknown"
                active_chip = None
                
                try:
                    player_gw_json = self.get_player_gameweek_picks(entry_id, current_gw)
                    if player_gw_json and 'picks' in player_gw_json:
                        dfplayergw = pd.DataFrame.from_records(player_gw_json['picks'])
                        
                        if not dfplayergw.empty:
                            # Get captain and vice-captain IDs
                            captain_picks = dfplayergw[dfplayergw.is_captain == True]
                            vice_captain_picks = dfplayergw[dfplayergw.is_vice_captain == True]
                            
                            if not captain_picks.empty:
                                captain_id = int(captain_picks.element.iloc[0])
                                captain_row = dffootballers[dffootballers.id == captain_id]
                                if not captain_row.empty:
                                    captain_name = captain_row.web_name.item()
                            
                            if not vice_captain_picks.empty:
                                vice_captain_id = int(vice_captain_picks.element.iloc[0])
                                vice_captain_row = dffootballers[dffootballers.id == vice_captain_id]
                                if not vice_captain_row.empty:
                                    vice_captain_name = vice_captain_row.web_name.item()
                            
                            # Get active chip
                            if 'active_chip' in player_gw_json:
                                active_chip = player_gw_json['active_chip']
                    
                except Exception as e:
                    print(f"⚠️ Warning: Could not get picks for {player_name}: {e}")
                
                # Transform DataFrame to single row (your original logic)
                dfplayer.index = dfplayer.index + 1
                dfplayer_out = dfplayer.stack()
                dfplayer_out.index = dfplayer_out.index.map('{0[1]}_{0[0]}'.format)
                dfplayer = dfplayer_out.to_frame().T
                
                # Add player info
                dfplayer.insert(0, 'Player Name', dfleague.player_name[j], True)
                dfplayer.insert(1, 'Team Name', dfleague.entry_name[j], True)
                dfplayer.insert(2, 'Player Entry', dfleague.entry[j], True)
                dfplayer.insert(3, 'Player Points', dfleague.total[j], True)
                
                # Add captain, vice-captain, and active chip info
                dfplayer['Captain'] = captain_name
                dfplayer['Vice-captain'] = vice_captain_name
                dfplayer['Active chip'] = active_chip
                dfplayer['league_id'] = league_id
                dfplayer['gameweek'] = current_gw
                
                # Update results DataFrame
                dfresults = pd.concat([dfresults, dfplayer], ignore_index=True)
                
                # Process chips used
                if 'chips' in player_json:
                    dfplayerchips = pd.DataFrame.from_records(player_json['chips'])
                    if not dfplayerchips.empty:
                        dfplayerchips.insert(0, 'Player Name', dfleague.player_name[j], True)
                        dfplayerchips.insert(1, 'Player Points', dfleague.total[j], True)
                        dfplayerchips['league_id'] = league_id
                        dfplayerchips['entry_id'] = entry_id
                        
                        # Update chips DataFrame
                        dfresultschips = pd.concat([dfresultschips, dfplayerchips], ignore_index=True)
                
                # Small delay to be nice to FPL API
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Error processing player {player_name}: {e}")
                continue
        
        # Store in database
        if store_in_db and not dfresults.empty:
            print("💾 Storing gameweek data in Supabase...")
            success = fpl_db.store_gameweek_data(dfresults)
            print(f"📊 Gameweek data stored: {'✅' if success else '❌'}")
            
            # Store chip data
            if not dfresultschips.empty:
                chip_success = fpl_db.store_chip_usage(dfresultschips)
                print(f"🎯 Chip data stored: {'✅' if chip_success else '❌'}")
        
        print(f"🎉 Processing complete! Processed {len(dfresults)} player records")
        return dfresults, dfresultschips
    
    def get_league_standings_from_db(self, league_id: int, gameweek: Optional[int] = None) -> Dict:
        """Get league standings from Supabase (lightning fast!)"""
        try:
            df = fpl_db.get_league_standings(league_id, gameweek)
            if df.empty:
                return {"standings": [], "message": "No data found - try running data collection first"}
            
            standings = []
            for _, row in df.iterrows():
                standings.append({
                    "position": row.get('position', 0),
                    "entry_id": row.get('entry_id'),
                    "player_name": row.get('player_name', 'Unknown'),
                    "team_name": row.get('team_name', 'Unknown Team'),
                    "total_points": row.get('total_points', 0),
                    "gameweek_points": row.get('points', 0),
                    "captain": row.get('captain'),
                    "vice_captain": row.get('vice_captain'),
                    "active_chip": row.get('active_chip'),
                    "gameweek": row.get('gameweek', 0),
                    "transfers_cost": row.get('transfers_cost', 0),
                    "points_on_bench": row.get('points_on_bench', 0)
                })
            
            return {
                "league_id": league_id,
                "gameweek": gameweek or "latest",
                "total_players": len(standings),
                "standings": standings,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Error getting league standings from DB: {e}")
            return {"error": str(e), "standings": []}
    
    def get_player_trends_from_db(self, entry_id: int) -> Dict:
        """Get player performance trends over time"""
        try:
            df = fpl_db.get_player_trends(entry_id)
            if df.empty:
                return {"trends": [], "message": "No data found for this player"}
            
            trends = []
            for _, row in df.iterrows():
                trends.append({
                    "gameweek": row.get('gameweek'),
                    "points": row.get('points', 0),
                    "total_points": row.get('total_points', 0),
                    "overall_rank": row.get('overall_rank'),
                    "previous_rank": row.get('previous_rank'),
                    "rank_change": (row.get('previous_rank', 0) - row.get('overall_rank', 0)) if row.get('previous_rank') else 0,
                    "captain": row.get('captain'),
                    "transfers_cost": row.get('transfers_cost', 0),
                    "team_value": row.get('team_value', 0) / 10,  # Convert back to millions
                    "active_chip": row.get('active_chip')
                })
            
            return {
                "entry_id": entry_id,
                "player_name": df.iloc[0].get('player_name', 'Unknown'),
                "total_gameweeks": len(trends),
                "trends": trends
            }
            
        except Exception as e:
            print(f"Error getting player trends: {e}")
            return {"error": str(e), "trends": []}
    
    def get_captain_analysis_from_db(self, league_id: int) -> Dict:
        """Get captain choices analysis for the league"""
        try:
            df = fpl_db.get_captain_analysis(league_id)
            if df.empty:
                return {"analysis": [], "message": "No captain data found"}
            
            # Analyze captain choices
            captain_stats = df.groupby('captain').agg({
                'points': ['count', 'mean', 'sum'],
                'gameweek': 'nunique'
            }).round(1)
            
            captain_analysis = []
            for captain in captain_stats.index:
                if captain and captain != "Unknown":
                    stats = captain_stats.loc[captain]
                    captain_analysis.append({
                        "player_name": captain,
                        "times_captained": int(stats[('points', 'count')]),
                        "average_points": float(stats[('points', 'mean')]),
                        "total_points": int(stats[('points', 'sum')]),
                        "gameweeks_captained": int(stats[('gameweek', 'nunique')])
                    })
            
            # Sort by total points
            captain_analysis.sort(key=lambda x: x['total_points'], reverse=True)
            
            return {
                "league_id": league_id,
                "total_captains": len(captain_analysis),
                "analysis": captain_analysis[:10]  # Top 10 captains
            }
            
        except Exception as e:
            print(f"Error getting captain analysis: {e}")
            return {"error": str(e), "analysis": []}
    
    def get_league_summary_from_db(self, league_id: int) -> Dict:
        """Get comprehensive league summary with all stats"""
        try:
            summary = fpl_db.get_league_summary(league_id)
            
            if not summary:
                return {"error": "League not found", "league_id": league_id}
            
            # Add captain analysis and chip usage
            captain_analysis = self.get_captain_analysis_from_db(league_id)
            
            # Get chip usage
            chips_df = fpl_db.get_chip_usage_summary(league_id)
            chip_summary = {}
            if not chips_df.empty:
                chip_counts = chips_df['chip_name'].value_counts().to_dict()
                chip_summary = {
                    "total_chips_used": len(chips_df),
                    "chip_breakdown": chip_counts,
                    "players_used_chips": chips_df['entry_id'].nunique()
                }
            
            return {
                **summary,
                "captain_analysis": captain_analysis.get('analysis', [])[:5],  # Top 5 captains
                "chip_usage": chip_summary
            }
            
        except Exception as e:
            print(f"Error getting league summary: {e}")
            return {"error": str(e), "league_id": league_id}

# Initialize FPL service
fpl_service = FPLService()

# Example usage
if __name__ == "__main__":
    # Test with your league ID
    league_id = 646571
    
    # Process fresh data from FPL API
    print("Processing fresh FPL data...")
    df_gameweek, df_chips = fpl_service.process_league_data(league_id)
    
    # Get processed data from database
    print("\nGetting data from Supabase...")
    standings = fpl_service.get_league_standings_from_db(league_id)
    print(f"Found {len(standings.get('standings', []))} players in standings")
    
    # Get league summary
    summary = fpl_service.get_league_summary_from_db(league_id)
    print(f"League: {summary.get('league_info', {}).get('name', 'Unknown')}")
    print(f"Players: {summary.get('total_players', 0)}")
    print(f"Latest GW: {summary.get('latest_gameweek', 0)}")