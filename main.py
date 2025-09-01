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
    """
    Collect fresh data from FPL API for a specific league
    This runs in background to avoid timeout
    """
    try:
        # Add the data collection task to background
        background_tasks.add_task(process_league_data_background, league_id)
        
        return {
            "message": f"Data collection started for league {league_id}",
            "league_id": league_id,
            "status": "processing",
            "note": "Check /league/{league_id}/standings in a few minutes for updated data"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start data collection: {str(e)}")

async def process_league_data_background(league_id: int):
    """Background task to process league data"""
    try:
        print(f"🔄 Starting background data collection for league {league_id}")
        df_gameweek, df_chips = fpl_service.process_league_data(league_id, store_in_db=True)
        print(f"✅ Background data collection completed for league {league_id}")
    except Exception as e:
        print(f"❌ Background data collection failed for league {league_id}: {e}")

@app.post("/collect-data-sync/{league_id}")
async def collect_league_data_sync(league_id: int):
    """
    Collect fresh data from FPL API synchronously (may take time)
    Use this for testing or when you need immediate results
    """
    try:
        df_gameweek, df_chips = fpl_service.process_league_data(league_id, store_in_db=True)
        
        return {
            "message": "Data collection completed successfully",
            "league_id": league_id,
            "players_processed": len(df_gameweek),
            "chips_processed": len(df_chips),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data collection failed: {str(e)}")

# League endpoints
@app.get("/league/{league_id}/standings", response_model=Dict[str, Any])
async def get_league_standings(league_id: int, gameweek: Optional[int] = None):
    """Get current or specific gameweek standings for a league"""
    try:
        standings = fpl_service.get_league_standings_from_db(league_id, gameweek)
        
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
    """Get captain choices analysis for the league"""
    try:
        analysis = fpl_service.get_captain_analysis_from_db(league_id)
        
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
