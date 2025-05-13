#!/usr/bin/env python3
# agents/api_agents.py - API-based data collection agents
from __future__ import annotations
import logging
import utils

log = logging.getLogger("api_agents")

async def windy(ctx, session):
    """Fetch data from Windy.com API"""
    key = ctx.cfg["API"]["WINDY_KEY"].strip()
    if not key: 
        return []
    
    # Include more North Shore locations
    locs = [
        ("north_shore", 21.6168, -158.0968),
        ("pipeline", 21.6656, -158.0539),
        ("sunset", 21.6782, -158.0407),  # Sunset Beach
        ("waimea", 21.6420, -158.0666),  # Waimea Bay
        ("haleiwa", 21.5962, -158.1050), # Haleiwa
        ("south_shore", 21.2734, -157.8257),
        ("ala_moana", 21.2873, -157.8521)
    ]
    
    out = []
    for name, lat, lon in locs:
        # Enhanced parameters for swell components
        body = {
            "lat": lat, 
            "lon": lon,
            "model": "gfs", 
            "parameters": ["wind", "swell1", "swell2", "swell3", "waves", "windWaves"],
            "key": key
        }
        
        d = await ctx.fetch(session, "https://api.windy.com/api/point-forecast/v2",
                      method="POST", json_body=body)
        if d:
            fn = ctx.save(f"windy_{name}.json", d)
            is_north_shore = name in ["north_shore", "pipeline", "sunset", "waimea", "haleiwa"]
            out.append({
                "source": "Windy", 
                "filename": fn,
                "location": {"name": name, "lat": lat, "lon": lon},
                "type": "forecast", 
                "priority": 0,
                "timestamp": utils.utcnow(),
                "south_facing": name in ["south_shore", "ala_moana"],
                "north_facing": is_north_shore
            })
    
    return out

async def open_meteo(ctx, session):
    """Fetch Open-Meteo Marine & Weather API data"""
    # Enhanced with more North Shore locations
    locations = [
        ("north_shore", 21.6168, -158.0968),
        ("pipeline", 21.6656, -158.0539),
        ("sunset", 21.6782, -158.0407),
        ("waimea", 21.6420, -158.0666),
        ("south_shore", 21.2734, -157.8257),
        ("ala_moana", 21.2873, -157.8521)
    ]
    
    out = []
    
    # Marine API - use only wave_height which works
    for name, lat, lon in locations:
        url = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lon}&hourly=wave_height,wave_period,wave_direction"
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"open_meteo_{name}_marine.json", d)
            is_north_shore = name in ["north_shore", "pipeline", "sunset", "waimea"]
            out.append({
                "source": "Open-Meteo", 
                "type": "marine_forecast",
                "filename": fn, 
                "location": {"name": name, "lat": lat, "lon": lon},
                "url": url, 
                "priority": 1, 
                "timestamp": utils.utcnow(),
                "south_facing": name in ["south_shore", "ala_moana"],
                "north_facing": is_north_shore
            })
    
    # Weather API for wind data (which marine API doesn't support correctly)
    for name, lat, lon in locations:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=windspeed_10m,winddirection_10m,windgusts_10m&forecast_days=7"
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"open_meteo_{name}_wind.json", d)
            is_north_shore = name in ["north_shore", "pipeline", "sunset", "waimea"]
            out.append({
                "source": "Open-Meteo", 
                "type": "wind_forecast",
                "filename": fn, 
                "location": {"name": name, "lat": lat, "lon": lon},
                "url": url, 
                "priority": 1, 
                "timestamp": utils.utcnow(),
                "south_facing": name in ["south_shore", "ala_moana"],
                "north_facing": is_north_shore
            })
    
    return out