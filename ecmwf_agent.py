#!/usr/bin/env python3
# ecmwf_agent.py - ECMWF open data integration

from __future__ import annotations
import json, logging, os, tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

import utils

log = logging.getLogger("ecmwf_agent")

# Try to import the new ecmwf-opendata client
try:
    from ecmwf.opendata import Client as OpenDataClient
    OPENDATA_AVAILABLE = True
except ImportError:
    log.warning("ecmwf-opendata package not installed. Run 'pip install ecmwf-opendata'")
    OPENDATA_AVAILABLE = False
    # Try to import the legacy client as fallback
    try:
        from ecmwfapi import ECMWFDataServer
        LEGACY_AVAILABLE = True
    except ImportError:
        log.warning("ecmwfapi package not installed. Run 'pip install ecmwf-api-client'")
        LEGACY_AVAILABLE = False

# Wave forecast parameters
WAVE_PARAMS = {
    "swh": "Significant wave height",
    "mwd": "Mean wave direction",
    "mwp": "Mean wave period"
}

# Regions of interest (for documentation and post-processing)
REGIONS = {
    "hawaii": {"bounds": [30, -170, 10, -150]},
    "north_pacific": {"bounds": [60, -180, 20, -120]},
    "south_pacific": {"bounds": [-15, -180, -60, -120]},
}

# Alternative chart URLs to try if API fails
CHART_URLS = {
    "swh": "https://charts.ecmwf.int/opencharts-api/v1/products/medium-significant-wave-height?area={region}&format=jpeg",
    "mwd": "https://charts.ecmwf.int/opencharts-api/v1/products/medium-wave-direction?area={region}&format=jpeg",
    "mwp": "https://charts.ecmwf.int/opencharts-api/v1/products/medium-wave-period?area={region}&format=jpeg"
}

async def fetch_ecmwf_opendata(ctx, region_name: str, target_file: str) -> bool:
    """Fetch ECMWF wave forecast data using the new opendata client."""
    try:
        client = OpenDataClient(source="ecmwf")  # Options: 'ecmwf', 'azure', 'aws'
        
        # Prepare request parameters - removing area parameter as it's not supported
        request: Dict[str, Any] = {
            "stream": "wave",
            "type": "fc",
            "step": [0, 6, 12, 18, 24, 30, 36, 42, 48],
            "param": list(WAVE_PARAMS.keys()),  # Get all wave params
            "target": target_file
        }
            
        log.info(f"Requesting ECMWF wave forecast data for {region_name} using opendata client")
        client.retrieve(**request)
        
        # TODO: Post-processing to crop to region if needed
        # This would require wgrib2 or similar tools
        
        return True
    except Exception as e:
        log.error(f"Failed to retrieve ECMWF opendata for {region_name}: {e}")
        return False

async def fetch_ecmwf_legacy(ctx, region_name: str, target_file: str) -> bool:
    """Legacy method to fetch ECMWF data using the traditional API client."""
    if not LEGACY_AVAILABLE:
        return False
        
    try:
        api_key = ctx.cfg["API"].get("ECMWF_KEY", "").strip()
        email = ctx.cfg["API"].get("ECMWF_EMAIL", "").strip()
        
        if not api_key or not email:
            log.warning("ECMWF API key or email not configured")
            return False
        
        server = ECMWFDataServer()
        
        # Get region-specific bounds
        bounds = REGIONS.get(region_name, {}).get("bounds", None)
        area_param = "/".join(map(str, bounds)) if bounds else None
        
        # Prepare request parameters
        params = {
            "class": "od",  # Open data
            "stream": "wave",
            "type": "fc",  # Forecast
            "param": "140.128/141.128/142.128",  # swh/mwd/mwp
            "levtype": "sfc",
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "time": "00",
            "step": "0/6/12/18/24/30/36/42/48",
            "target": target_file
        }
        
        # Try different dataset names
        dataset_options = ["wave", "waef", "wam"]
        
        for dataset in dataset_options:
            try:
                params["dataset"] = dataset
                
                # Add area if specified
                if area_param:
                    params["area"] = area_param
                    
                log.info(f"Requesting ECMWF wave forecast data for {region_name} using legacy API with dataset={dataset}")
                server.retrieve(params)
                log.info(f"Successfully retrieved data with dataset={dataset}")
                return True
            except Exception as e:
                log.warning(f"Failed to retrieve with dataset={dataset}: {e}")
                continue
        
        log.error(f"All dataset options failed for {region_name}")
        return False
    except Exception as e:
        log.error(f"Failed to retrieve ECMWF data via legacy API for {region_name}: {e}")
        return False

async def fetch_ecmwf_fallback(ctx, sess, fetch_func, region_name: str) -> Optional[Dict[str, Any]]:
    """Attempt to fetch ECMWF chart images as fallback."""
    if not fetch_func or not sess:
        return None
    
    # Map region names to chart region codes
    region_mapping = {
        "hawaii": "pacific",  # Use pacific for Hawaii
        "north_pacific": "north_pacific",
        "south_pacific": "south_pacific"
    }
    
    chart_region = region_mapping.get(region_name, "pacific")
    
    # Try alternative chart sources like Tropical Tidbits
    alternative_charts = {
        "swh": f"https://www.tropicaltidbits.com/analysis/ocean/global/global_htsgw_0-000.png",
        "mwd": f"https://www.tropicaltidbits.com/analysis/ocean/global/global_mwd_0-000.png",
        "mwp": f"https://www.tropicaltidbits.com/analysis/ocean/global/global_perpw_0-000.png"
    }
    
    # Try ECMWF charts first
    for param, url_template in CHART_URLS.items():
        url = url_template.format(region=chart_region)
        try:
            # Try with certificate validation first
            data = await fetch_func(ctx, sess, url)
            if not data:
                # Try without certificate validation if failed
                data = await fetch_func(ctx, sess, url, verify_ssl=False)
                
            if data:
                return {
                    "param": param,
                    "data": data,
                    "url": url
                }
        except Exception as e:
            log.warning(f"Failed to fetch ECMWF chart for {param}: {e}")
    
    # Try alternative chart sources if ECMWF failed
    for param, url in alternative_charts.items():
        try:
            data = await fetch_func(ctx, sess, url)
            if data:
                return {
                    "param": param,
                    "data": data,
                    "url": url
                }
        except Exception as e:
            log.warning(f"Failed to fetch alternative chart for {param}: {e}")
    
    return None

async def fetch_ecmwf_data(ctx, sess, fetch_func=None, save_func=None) -> List[dict]:
    """Main function to fetch ECMWF wave forecast data using available methods."""
    if save_func is None:
        save_func = ctx.save if hasattr(ctx, 'save') else lambda ctx, name, data: name
    
    results = []
    tmp_dir = Path(tempfile.mkdtemp())
    
    try:
        # Process each region
        for region_name in REGIONS.keys():
            # Check if we should process this region based on configuration
            north_emphasis = ctx.cfg["FORECAST"].get("north_swell_emphasis", "auto").lower()
            if region_name == "north_pacific" and north_emphasis not in ["auto", "true"]:
                continue
                
            south_emphasis = ctx.cfg["FORECAST"].get("south_swell_emphasis", "auto").lower()
            if region_name == "south_pacific" and south_emphasis not in ["auto", "true"]:
                continue
            
            # Prepare target file path
            target_file = str(tmp_dir / f"ecmwf_{region_name}_wave.grib2")
            success = False
            
            # Try OpenData client first if available
            if OPENDATA_AVAILABLE:
                success = await fetch_ecmwf_opendata(ctx, region_name, target_file)
            
            # Fall back to legacy method if OpenData failed or not available
            if not success and LEGACY_AVAILABLE:
                success = await fetch_ecmwf_legacy(ctx, region_name, target_file)
            
            # Process results if successful
            if success and Path(target_file).exists():
                with open(target_file, "rb") as f:
                    data = f.read()
                
                filename = save_func(ctx, f"ecmwf_{region_name}_wave.grib2", data)
                
                results.append({
                    "source": "ECMWF",
                    "type": "wave_model",
                    "subtype": f"{region_name}_wave",
                    "description": f"ECMWF Wave forecast for {region_name}",
                    "filename": filename,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": region_name == "north_pacific",
                    "south_facing": region_name == "south_pacific"
                })
            else:
                # Try fallback chart images if GRIB retrieval failed
                fallback = await fetch_ecmwf_fallback(ctx, sess, fetch_func, region_name)
                if fallback:
                    filename = save_func(ctx, f"ecmwf_{region_name}_{fallback['param']}.png", fallback['data'])
                    
                    results.append({
                        "source": "ECMWF",
                        "type": "chart",
                        "subtype": f"{region_name}_{fallback['param']}",
                        "description": f"ECMWF {WAVE_PARAMS.get(fallback['param'], 'Wave')} chart for {region_name}",
                        "filename": filename,
                        "url": fallback['url'],
                        "priority": 2,
                        "timestamp": utils.utcnow(),
                        "north_facing": region_name == "north_pacific",
                        "south_facing": region_name == "south_pacific"
                    })
        
        return results
    
    finally:
        # Clean up temporary files
        for file in tmp_dir.glob("*"):
            try:
                file.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass