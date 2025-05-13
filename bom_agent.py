#!/usr/bin/env python3
# bom_agent.py - Australian Bureau of Meteorology API integration
from __future__ import annotations
import json, logging, os, re, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import aiohttp
import utils

log = logging.getLogger("bom_agent")

# BOM data endpoints via HTTP
# Changed from FTP to HTTP since FTP might be blocked or unsupported
FTP_BASE = "http://www.bom.gov.au/anon/gen"

# Alternative data sources for Southern Hemisphere analysis
ALT_SOURCES = {
    "swell": "https://www.surf-forecast.com/breaks/Pipeline/forecasts/latest/six_day",
    "charts": "https://www.weatherzone.com.au/synoptic",
    "ww3": "https://www.surf-forecast.com/maps/Australia/significant-wave-height/6"
}

# Regions of interest for South Pacific storm tracking
# These are the Australian BOM forecast districts relevant for Hawaii south swells
DISTRICTS = {
    "IDY20301": {"description": "Southern Ocean", "priority": 1, "path": "/fwo/IDY20301.xml"},
    "IDY20302": {"description": "Southern Australia Waters", "priority": 1, "path": "/fwo/IDY20302.xml"}, 
    "IDY20100": {"description": "Australia Pacific Waters", "priority": 1, "path": "/fwo/IDY20100.xml"},
    "IDY20010": {"description": "New Zealand Waters", "priority": 0, "path": "/fwo/IDY20010.xml"}
}

# Chart products relevant for surf forecasting
CHARTS = [
    # Southern Ocean wave heights
    {"path": "/gms/IDE00439.jpg", "description": "Wave Height Analysis", "priority": 1},
    # Mean sea level pressure analysis
    {"path": "/gms/IDE00033.jpg", "description": "Mean Sea Level Pressure", "priority": 1},
    # Wind speed and direction
    {"path": "/gms/IDE00035.jpg", "description": "Wind Streamlines", "priority": 2}
]

# Marine observations endpoints
OBSERVATIONS = [
    {"path": "/fwo/IDY25001.xml", "description": "NSW Coast Observations", "priority": 2},
    {"path": "/fwo/IDY25002.xml", "description": "VIC Coast Observations", "priority": 2},
    {"path": "/fwo/IDY25003.xml", "description": "QLD Coast Observations", "priority": 2},
    {"path": "/fwo/IDY25004.xml", "description": "SA Coast Observations", "priority": 2},
    {"path": "/fwo/IDY25005.xml", "description": "WA Coast Observations", "priority": 2}
]

async def fetch_bom_data(ctx, sess, fetch_func=None, save_func=None) -> List[dict]:
    """Fetch Australian BOM data for Southern Hemisphere analysis.

    Args:
        ctx: Context object with bundle dir and config
        sess: aiohttp ClientSession for HTTP requests
        fetch_func: Optional function to use for fetching data (falls back to direct session use)
        save_func: Optional function to use for saving data (falls back to local save function)
    """
    # Set up fallbacks for fetch and save functions
    if fetch_func is None:
        async def fetch_func(ctx, sess, url):
            try:
                response = await sess.get(url, headers=ctx.headers, timeout=30)
                if response.status == 200:
                    return await response.read()
                return None
            except Exception as e:
                log.warning(f"Fetch error {type(e).__name__} for {url}: {e}")
                return None

    if save_func is None:
        def save_func(ctx, name, data):
            path = ctx.bundle / name
            if isinstance(data, str):
                data = data.encode('utf-8')
            path.write_bytes(data)
            return path.name
    """Fetch Australian BOM data for Southern Hemisphere analysis"""
    results = []
    
    # Fetch marine text forecasts for key districts
    for district_id, district_info in DISTRICTS.items():
        url = f"{FTP_BASE}{district_info['path']}"

        try:
            # Try with HTTP URL first
            log.info(f"Fetching BOM forecast for district {district_id}")
            data = await fetch_func(ctx, sess, url)

            # If HTTP fails, try HTTPS version
            if not data and url.startswith('http:'):
                https_url = url.replace('http:', 'https:')
                log.info(f"Retrying with HTTPS for district {district_id}")
                data = await fetch_func(ctx, sess, https_url)

            if data:
                # Save to bundle
                filename = f"bom_{district_id.lower()}_forecast.xml"
                saved_filename = save_func(ctx, filename, data)
                log.info(f"Saved BOM forecast for district {district_id}")

                results.append({
                    "source": "BOM",
                    "type": "text_forecast",
                    "subtype": "marine_forecast",
                    "district": district_id,
                    "description": district_info["description"],
                    "filename": saved_filename,
                    "priority": district_info["priority"],
                    "timestamp": utils.utcnow(),
                    "south_facing": True  # Southern Hemisphere data is for South Shore
                })
            else:
                log.warning(f"Could not fetch BOM forecast for district {district_id}")

        except Exception as e:
            log.error(f"Failed to fetch BOM forecast for district {district_id}: {e}")
    
    # Fetch marine charts
    for chart in CHARTS:
        url = f"{FTP_BASE}{chart['path']}"
        try:
            # Fetch the chart image
            data = await fetch_func(ctx, sess, url)
            
            if data:
                # Get filename from path
                chart_filename = Path(chart["path"]).name
                saved_filename = save_func(ctx, f"bom_{chart_filename}", data)
                
                results.append({
                    "source": "BOM",
                    "type": "chart",
                    "subtype": chart["description"],
                    "filename": saved_filename,
                    "url": url,
                    "priority": chart["priority"],
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                })
                
        except Exception as e:
            log.error(f"Failed to fetch BOM chart {chart['path']}: {e}")
    
    # Fetch marine observations
    for obs in OBSERVATIONS:
        url = f"{FTP_BASE}{obs['path']}"
        
        try:
            # Fetch the data
            data = await fetch_func(ctx, sess, url)
            
            if data:
                # Get filename from path
                obs_filename = Path(obs["path"]).name
                saved_filename = save_func(ctx, f"bom_{obs_filename}", data)
                
                results.append({
                    "source": "BOM",
                    "type": "observations",
                    "subtype": obs["description"],
                    "filename": saved_filename,
                    "priority": obs["priority"],
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                })
                
        except Exception as e:
            log.error(f"Failed to fetch BOM observations {obs['path']}: {e}")
    
    return results

# Example standalone usage
if __name__ == "__main__":
    import argparse, configparser, asyncio
    
    parser = argparse.ArgumentParser(description="BOM Data Agent")
    parser.add_argument("--config", default="config.ini", help="INI file")
    args = parser.parse_args()
    
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    
    async def main():
        class Context:
            def __init__(self, cfg):
                self.cfg = cfg
                self.headers = {"User-Agent": cfg["GENERAL"]["user_agent"]}
                self.timeout = int(cfg["GENERAL"]["timeout"])
                self.bundle = Path("./data")
                self.bundle.mkdir(exist_ok=True)
        
        async def fetch(ctx, sess, url, headers=None):
            async with sess.get(url, headers=headers or ctx.headers, timeout=ctx.timeout) as r:
                if r.status == 200:
                    return await r.read()
                return None
        
        def save(ctx, name, data):
            if isinstance(data, str):
                data = data.encode('utf-8')
            path = ctx.bundle / name
            path.write_bytes(data)
            return path.name
        
        ctx = Context(cfg)
        async with aiohttp.ClientSession() as sess:
            results = await fetch_bom_data(ctx, sess, fetch, save)
            print(f"Retrieved {len(results)} BOM data items")
    
    asyncio.run(main())