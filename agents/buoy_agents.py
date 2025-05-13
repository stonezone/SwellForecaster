#!/usr/bin/env python3
# agents/buoy_agents.py - NDBC, CDIP, and other buoy data collection agents
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
import utils

log = logging.getLogger("buoy_agents")

async def buoys(ctx, session):
    """Enhanced NDBC buoy implementation with time series data"""
    # ctx should provide: fetch, save, cfg
    # Get both standard and North Shore specific buoys
    buoy_ids = ["51001", "51002", "51101"]  # Standard buoys
    north_shore_buoys = ["51000", "51003", "51004", "51101"]  # North Pacific buoys
    cdip_buoys = ["106", "188", "098", "096"]  # CDIP buoys (Waimea, Mokapu, etc.)
    
    # Combine lists, removing duplicates while preserving order
    all_buoy_ids = []
    for bid in buoy_ids + north_shore_buoys:
        if bid not in all_buoy_ids:
            all_buoy_ids.append(bid)
    
    out = []
    
    # Process NDBC buoys
    for bid in all_buoy_ids:
        # Get recent data (latest observation)
        url = f"https://www.ndbc.noaa.gov/data/realtime2/{bid}.txt"
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"ndbc_{bid}.txt", d)
            out.append({"source": "NDBC", "filename": fn, "buoy": bid,
                        "type": "realtime", "priority": 0, "timestamp": utils.utcnow(),
                        "south_facing": bid in ["51002", "51004"]})
        
        # Get hourly data (last 24 hours) for time series analysis
        hourly_url = f"https://www.ndbc.noaa.gov/data/hourly2/{bid}.txt"
        d = await ctx.fetch(session, hourly_url)
        if d:
            fn = ctx.save(f"ndbc_{bid}_hourly.txt", d)
            out.append({"source": "NDBC", "filename": fn, "buoy": bid,
                       "type": "hourly", "priority": 0, "timestamp": utils.utcnow(),
                       "south_facing": bid in ["51002", "51004"]})
            
        # Get spectral wave data if available
        spectral_url = f"https://www.ndbc.noaa.gov/data/realtime2/{bid}.spec"
        d = await ctx.fetch(session, spectral_url)
        if d:
            fn = ctx.save(f"ndbc_{bid}_spec.txt", d)
            out.append({"source": "NDBC", "filename": fn, "buoy": bid,
                       "type": "spectral", "priority": 0, "timestamp": utils.utcnow(),
                       "south_facing": bid in ["51002", "51004"]})
    
    # Process CDIP buoys
    for bid in cdip_buoys:
        cdip_url = f"https://cdip.ucsd.edu/data_access/cdip_json.php?sp=1&xyrr=1&station={bid}&period=latest&tz=UTC"
        d = await ctx.fetch(session, cdip_url)
        if d:
            fn = ctx.save(f"cdip_{bid}.json", d)
            out.append({"source": "CDIP", "filename": fn, "buoy": bid,
                       "type": "spectra", "priority": 0, "timestamp": utils.utcnow(),
                       "south_facing": bid in ["096"]})
    
    return out

async def noaa_coops(ctx, session):
    """Fetch NOAA CO-OPS station wind data for Hawaii"""
    # All three variants worked, but the date range option provides the most data
    stations = {"1612340": "Honolulu", "1612480": "Kaneohe"}
    out = []
    
    for station_id, name in stations.items():
        # Date range version - get 24 hours of data
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        url = f"https://tidesandcurrents.noaa.gov/api/datagetter?station={station_id}&product=wind&begin_date={today}&range=24&units=english&time_zone=gmt&format=json"
        
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"coops_{name.lower()}_wind.json", d)
            out.append({"source": "NOAA-COOPS", "type": "wind_observation",
                        "filename": fn, "station": station_id, 
                        "location": name, "url": url,
                        "priority": 0, "timestamp": utils.utcnow()})
    
    return out