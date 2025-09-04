from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uvicorn
from datetime import datetime
import os
from fpl_service import fpl_service
from database import fpl_db

# Pydantic models for API responses
class LeagueStandingsResponse(BaseModel):
    league_id: int
    gameweek: str
    total_players: int
    standings: List[Dict[str, Any]]
    last_updated: str

class PlayerTrendsResponse(BaseModel):
    entry_id: int
    player_name: str
    total_gameweeks: int
    trends: List[Dict[str, Any]]

class LeagueSummaryResponse(BaseModel):
    league_info: Dict[str, Any]
    current_standings: List[Dict[str, Any]]
    latest_gameweek: int
    total_players: int
    captain_analysis: List[Dict[str, Any]]
    chip_usage: Dict[str, Any]

class ProcessDataRequest(BaseModel):
    league_id: int
    force_refresh: bool = False

# Initialize FastAPI app
app = FastAPI(
    title="FPL Mini-League API",
    description="API for Fantasy Premier League mini-league statistics and analysis",
    version="1.0.0"
)

# Add CORS middleware for Flutter app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your Flutter app domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "FPL Mini-League API is running! 🏈",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "supabase_connected": fpl_db.test_connection()
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    try:
        db_status = fpl_db.test_connection()
        current_gw = fpl_db.get_current_gameweek()
        
        return {
            "status": "healthy" if db_status else "unhealthy",
            "database": "connected" if db_status else "disconnected",
            "current_gameweek": current_gw,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# Data collection endpoints
@app.post("/collect-data/{league_id}")
async def collect_league_data(league_id: int, background_tasks: BackgroundTasks):
    """Collect fresh data from FPL API using normalized schema"""
    try:
        background_tasks.add_task(process_league_data_background_normalized, league_id)
        
        return {
            "message": f"Data collection started for league {league_id}",
            "league_id": league_id,
            "status": "processing",
            "note": "Using normalized schema - no duplicate players across leagues"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start data collection: {str(e)}")

async def process_league_data_background_normalized(league_id: int):
    """Background task using normalized schema"""
    try:
        print(f"Starting normalized data collection for league {league_id}")
        df_gameweek, df_chips = fpl_service.process_league_data_normalized(league_id, store_in_db=True)
        print(f"Normalized data collection completed for league {league_id}")
    except Exception as e:
        print(f"Normalized data collection failed for league {league_id}: {e}")

@app.post("/collect-data-sync/{league_id}")
async def collect_league_data_sync(league_id: int):
    """Collect fresh data synchronously using normalized schema"""
    try:
        df_gameweek, df_chips = fpl_service.process_league_data_normalized(league_id, store_in_db=True)
        
        return {
            "message": "Data collection completed successfully",
            "league_id": league_id,
            "players_processed": len(df_gameweek),
            "chips_processed": len(df_chips),
            "schema": "normalized",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data collection failed: {str(e)}")

# League endpoints
@app.get("/league/{league_id}/standings")
async def get_league_standings(league_id: int, gameweek: Optional[int] = None):
    """Get league standings using normalized schema"""
    try:
        standings = fpl_service.get_league_standings_from_db_normalized(league_id, gameweek)
        
        if "error" in standings:
            raise HTTPException(status_code=404, detail=standings["error"])
        
        return standings
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get league standings: {str(e)}")

@app.get("/league/{league_id}/summary", response_model=Dict[str, Any])
async def get_league_summary(league_id: int):
    """Get comprehensive league summary with all statistics"""
    try:
        summary = fpl_service.get_league_summary_from_db(league_id)
        
        if "error" in summary:
            raise HTTPException(status_code=404, detail=summary["error"])
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get league summary: {str(e)}")

@app.get("/league/{league_id}/captain-analysis")
async def get_captain_analysis(league_id: int):
    """Get captain analysis using normalized schema"""
    try:
        analysis = fpl_service.get_captain_analysis_from_db_normalized(league_id)
        
        if "error" in analysis:
            raise HTTPException(status_code=404, detail=analysis["error"])
        
        return analysis
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get captain analysis: {str(e)}")

# Player endpoints
@app.get("/player/{entry_id}/trends", response_model=Dict[str, Any])
async def get_player_trends(entry_id: int):
    """Get performance trends for a specific player over time"""
    try:
        trends = fpl_service.get_player_trends_from_db(entry_id)
        
        if "error" in trends:
            raise HTTPException(status_code=404, detail=trends["error"])
        
        return trends
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get player trends: {str(e)}")

@app.get("/player/{entry_id}/history")
async def get_player_history(entry_id: int):
    """Get historical gameweek data for a specific player"""
    try:
        df = fpl_db.get_player_history(entry_id)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for this player")
        
        history = []
        for _, row in df.iterrows():
            history.append({
                "gameweek": row.get('gameweek'),
                "points": row.get('points'),
                "total_points": row.get('total_points'),
                "overall_rank": row.get('overall_rank'),
                "captain": row.get('captain'),
                "vice_captain": row.get('vice_captain'),
                "transfers_cost": row.get('transfers_cost'),
                "team_value": row.get('team_value', 0) / 10,  # Convert to millions
                "active_chip": row.get('active_chip')
            })
        
        return {
            "entry_id": entry_id,
            "total_gameweeks": len(history),
            "history": history
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get player history: {str(e)}")

# NEW ENDPOINT: Cross-league player analysis
@app.get("/player/{entry_id}/cross-league-analysis")
async def get_cross_league_analysis(entry_id: int):
    """Get player's performance across all leagues they participate in"""
    try:
        analysis = fpl_service.get_player_cross_league_analysis(entry_id)
        
        if "error" in analysis:
            raise HTTPException(status_code=404, detail=analysis["error"])
        
        return analysis
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get cross-league analysis: {str(e)}")

# NEW ENDPOINT: Global player search
@app.get("/players/search/{player_name}")
async def search_global_players(player_name: str):
    """Search for players across all leagues by name"""
    try:
        response = fpl_db.client.table('global_players')\
            .select('entry_id, player_name, current_team_name')\
            .ilike('player_name', f'%{player_name}%')\
            .limit(20)\
            .execute()
        
        if not response.data:
            return {"players": [], "message": "No players found"}
        
        players = []
        for player in response.data:
            # Get leagues this player participates in
            leagues_response = fpl_db.client.table('league_memberships')\
                .select('league_id, leagues(name)')\
                .eq('entry_id', player['entry_id'])\
                .execute()
            
            player_leagues = []
            if leagues_response.data:
                for membership in leagues_response.data:
                    league_info = membership.get('leagues', {})
                    if isinstance(league_info, list) and len(league_info) > 0:
                        league_info = league_info[0]
                    
                    player_leagues.append({
                        "league_id": membership['league_id'],
                        "league_name": league_info.get('name', f"League {membership['league_id']}")
                    })
            
            players.append({
                "entry_id": player['entry_id'],
                "player_name": player['player_name'],
                "current_team_name": player['current_team_name'],
                "leagues": player_leagues,
                "total_leagues": len(player_leagues)
            })
        
        return {
            "search_term": player_name,
            "total_found": len(players),
            "players": players
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Player search failed: {str(e)}")

# NEW ENDPOINT: League statistics
@app.get("/league/{league_id}/stats")
async def get_league_statistics(league_id: int):
    """Get comprehensive league statistics"""
    try:
        # Get basic league info
        league_response = fpl_db.client.table('leagues')\
            .select('*')\
            .eq('id', league_id)\
            .execute()
        
        if not league_response.data:
            raise HTTPException(status_code=404, detail="League not found")
        
        # Get membership count and player diversity
        memberships = fpl_db.client.table('league_memberships')\
            .select('entry_id')\
            .eq('league_id', league_id)\
            .execute()
        
        # Get gameweek data for stats
        gameweek_data = fpl_db.client.table('gameweek_data_new')\
            .select('*')\
            .eq('league_id', league_id)\
            .execute()
        
        stats = {
            "league_info": league_response.data[0],
            "total_players": len(memberships.data) if memberships.data else 0,
            "total_gameweeks": len(set(gw['gameweek'] for gw in gameweek_data.data)) if gameweek_data.data else 0,
            "total_records": len(gameweek_data.data) if gameweek_data.data else 0
        }
        
        if gameweek_data.data:
            points = [gw['points'] for gw in gameweek_data.data if gw['points']]
            if points:
                stats.update({
                    "highest_gameweek_score": max(points),
                    "lowest_gameweek_score": min(points),
                    "average_gameweek_score": round(sum(points) / len(points), 1),
                    "total_transfers": sum(gw['transfers'] or 0 for gw in gameweek_data.data),
                    "total_transfer_costs": sum(gw['transfers_cost'] or 0 for gw in gameweek_data.data)
                })
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get league statistics: {str(e)}")

# Utility endpoints
@app.get("/gameweek/current")
async def get_current_gameweek():
    """Get current gameweek number"""
    try:
        # Try to get from FPL API first, fallback to database
        current_gw = fpl_service.get_current_gameweek()
        
        return {
            "current_gameweek": current_gw,
            "source": "fpl_api",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        # Fallback to database
        try:
            current_gw = fpl_db.get_current_gameweek()
            return {
                "current_gameweek": current_gw,
                "source": "database",
                "timestamp": datetime.now().isoformat(),
                "note": "Retrieved from database due to API error"
            }
        except Exception as db_error:
            raise HTTPException(status_code=500, detail=f"Failed to get current gameweek: {str(e)}")

# Example leagues endpoint (for testing)
@app.get("/examples")
async def get_example_leagues():
    """Get example league IDs for testing"""
    return {
        "message": "Use these league IDs to test the API",
        "examples": [
            {
                "league_id": 646571,
                "name": "Your Test League",
                "description": "Use /collect-data/646571 first, then /league/646571/standings"
            }
        ],
        "instructions": [
            "1. First run: POST /collect-data/{league_id} to gather data",
            "2. Then use: GET /league/{league_id}/standings to see results",
            "3. For full analysis: GET /league/{league_id}/summary"
        ]
    }

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return {
        "error": "Not found",
        "message": "The requested resource was not found",
        "suggestion": "Try collecting data first using POST /collect-data/{league_id}"
    }

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return {
        "error": "Internal server error", 
        "message": "Something went wrong on our end",
        "suggestion": "Please try again or check the health endpoint"
    }

# Run the app
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"🚀 Starting server on port {port}")
    uvicorn.run(
        "main:app",  # Use string format for Railway
        host="0.0.0.0",
        port=port,
        reload=False  # Disable reload in production
    )
