#!/usr/bin/env python3
# stormglass_agent.py - Stormglass.io marine data integration

from __future__ import annotations
import json, logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Union
from pathlib import Path

import utils

# Helper function to save data to the bundle
def save(ctx, name: str, data: Union[bytes, str]) -> str:
    """Save data to the bundle directory"""
    path = ctx.bundle / name
    if isinstance(data, str):
        data = data.encode('utf-8')
    path.write_bytes(data)
    return path.name

log = logging.getLogger("stormglass_agent")

# Predefined locations for surf forecasting
LOCATIONS = [
    # North Shore
    {"name": "pipeline", "lat": 21.6656, "lon": -158.0539, "north_facing": True},
    {"name": "sunset", "lat": 21.6782, "lon": -158.0407, "north_facing": True},
    {"name": "waimea", "lat": 21.6420, "lon": -158.0666, "north_facing": True},
    {"name": "haleiwa", "lat": 21.5962, "lon": -158.1050, "north_facing": True},
    
    # South Shore
    {"name": "waikiki", "lat": 21.2734, "lon": -157.8257, "south_facing": True},
    {"name": "ala_moana", "lat": 21.2873, "lon": -157.8521, "south_facing": True},
]

# Stormglass API parameters for marine forecasts
MARINE_PARAMS = [
    "waveHeight",
    "wavePeriod",
    "waveDirection",
    "swellHeight",
    "swellPeriod",
    "swellDirection",
    "secondarySwellHeight",
    "secondarySwellPeriod",
    "secondarySwellDirection",
    "windWaveHeight",
    "windWavePeriod",
    "windWaveDirection",
    "windSpeed",
    "windDirection"
]

# Data sources to request (in order of preference)
SOURCES = [
    "noaa", 
    "meteo", 
    "sg",
    "dwd"
]

async def fetch_marine_forecast(ctx, sess, location: Dict[str, Any], api_key: str) -> Dict[str, Any] | None:
    """Fetch marine forecast data for a specific location."""
    try:
        # Convert parameters to comma-separated string
        params_str = ",".join(MARINE_PARAMS)
        sources_str = ",".join(SOURCES)

        # Calculate time range (now to 5 days from now)
        now = datetime.now(timezone.utc)
        start = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (now + timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Build URL - using correctly formatted API endpoint
        url = f"https://api.stormglass.io/v2/weather/point"

        # Build parameters for the request
        params = {
            "lat": location['lat'],
            "lng": location['lon'],
            "params": params_str,
            "source": sources_str,
            "start": start,
            "end": end
        }

        # Create custom headers with authorization
        custom_headers = {"Authorization": api_key}

        # Fetch data (Stormglass API can be slow)
        log.info(f"Fetching Stormglass marine forecast for {location['name']}")

        # Use custom headers and params for proper request
        try:
            log.info(f"Requesting StormGlass data with params: {params}")
            response = await sess.get(url, headers=custom_headers, params=params, timeout=60)

            if response.status == 200:
                data = await response.read()
                log.info(f"Successfully fetched Stormglass data for {location['name']}")
                return {
                    "data": data,
                    "url": url + '?' + '&'.join([f"{k}={v}" for k, v in params.items()])
                }
            else:
                error_text = await response.text()
                log.warning(f"Stormglass API returned status code {response.status} for {location['name']}: {error_text}")
                return None
        except Exception as e:
            log.warning(f"Error fetching from Stormglass API: {e}")
            return None
    except Exception as e:
        log.warning(f"Failed to fetch Stormglass marine forecast for {location['name']}: {e}")
        return None

async def stormglass_agent(ctx, sess) -> List[dict]:
    """Main function to fetch marine data from Stormglass.io API."""
    api_key = ctx.cfg["API"].get("STORMGLASS_KEY", "").strip()
    if not api_key:
        log.warning("Stormglass API key not configured in config.ini")
        return []
    
    results = []
    
    # Process each location
    for location in LOCATIONS:
        forecast = await fetch_marine_forecast(ctx, sess, location, api_key)
        
        if forecast:
            # Save data
            filename = f"stormglass_{location['name']}_marine.json"
            saved_filename = save(ctx, filename, forecast["data"])
            
            # Add to results
            results.append({
                "source": "Stormglass",
                "type": "marine_forecast",
                "subtype": "point",
                "filename": saved_filename,
                "location": {
                    "name": location["name"],
                    "lat": location["lat"],
                    "lon": location["lon"]
                },
                "url": forecast["url"],
                "priority": 1,  # High priority since this is paid API data
                "timestamp": utils.utcnow(),
                "north_facing": location.get("north_facing", False),
                "south_facing": location.get("south_facing", False)
            })
    
    # Also fetch tidal data if we have any successful forecasts
    if results:
        for location in LOCATIONS[:2]:  # Just get North Shore and South Shore tides
            try:
                # Build tide URL and params
                tide_url = "https://api.stormglass.io/v2/tide/extremes/point"
                tide_params = {
                    "lat": location['lat'],
                    "lng": location['lon'],
                    "start": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "end": (datetime.now(timezone.utc) + timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
                }
                
                # Fetch tide data
                log.info(f"Fetching Stormglass tide data for {location['name']}")
                # Use custom headers for tide data
                custom_headers = {"Authorization": api_key}
                try:
                    tide_response = await sess.get(tide_url, headers=custom_headers, params=tide_params, timeout=60)
                    if tide_response.status == 200:
                        tide_data = await tide_response.read()
                        log.info(f"Successfully fetched tide data for {location['name']}")
                    else:
                        error_text = await tide_response.text()
                        log.warning(f"Stormglass tide API returned status code {tide_response.status} for {location['name']}: {error_text}")
                        tide_data = None
                except Exception as e:
                    log.warning(f"Error fetching from Stormglass tide API: {e}")
                    tide_data = None
                
                if tide_data:
                    # Save tide data
                    tide_filename = f"stormglass_{location['name']}_tides.json"
                    saved_tide_filename = save(ctx, tide_filename, tide_data)
                    
                    # Add to results
                    results.append({
                        "source": "Stormglass",
                        "type": "tide_forecast",
                        "filename": saved_tide_filename,
                        "location": {
                            "name": location["name"],
                            "lat": location["lat"],
                            "lon": location["lon"]
                        },
                        "url": tide_url + '?' + '&'.join([f"{k}={v}" for k, v in tide_params.items()]),
                        "priority": 2,
                        "timestamp": utils.utcnow(),
                        "north_facing": location.get("north_facing", False),
                        "south_facing": location.get("south_facing", False)
                    })
            except Exception as e:
                log.warning(f"Failed to fetch Stormglass tide data for {location['name']}: {e}")
    
    log.info(f"Stormglass agent completed with {len(results)} results")
    return results