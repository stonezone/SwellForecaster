# models.py - WaveWatch III data processor
from asyncio import create_subprocess_shell
import tempfile, json, utils, os, subprocess, logging
from datetime import datetime, timezone
from pathlib import Path

async def model_agent(ctx, sess, fetch, save):
    """Process WaveWatch III GRIB2 data for Hawaiian waters"""
    logger = logging.getLogger("model_agent")
    
    # Format date for NOAA URL pattern
    url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/wave/prod/wave.{:%Y%m%d}/multi_1.glo_30m.t00z.grib2".format(
        datetime.now(timezone.utc))
    
    logger.info(f"Downloading WW3 GRIB from: {url}")
    grib = await fetch(ctx, sess, url)
    if not grib:
        logger.warning("Failed to download WW3 GRIB data")
        return []
    
    # Save to temporary file
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.write(grib)
    tmp.close()
    
    # Define paths for processed files
    slice_path = tmp.name + ".slice"
    json_path = slice_path + ".json"
    
    try:
        # Extract region around Hawaii (longitude 140-160, latitude 10-30)
        logger.info("Slicing GRIB to Hawaiian region")
        slice_process = await create_subprocess_shell(
            f"wgrib2 {tmp.name} -small_grib 140:160 10:30 {slice_path}"
        )
        await slice_process.wait()
        
        if not Path(slice_path).exists():
            logger.error("GRIB slicing failed")
            return []
        
        # Convert GRIB to JSON
        logger.info("Converting GRIB to JSON")
        json_process = await create_subprocess_shell(
            f"grib2json -d 2 -n 3 -o {json_path} {slice_path}"
        )
        await json_process.wait()
        
        if not Path(json_path).exists():
            logger.error("GRIB to JSON conversion failed")
            return []
        
        # Read and save the JSON data
        data = Path(json_path).read_bytes()
        fn = save(ctx, "ww3_hawaii.json", data)
        
        # Also get Southern Hemisphere region (for South Shore forecasting)
        south_slice_path = tmp.name + ".south.slice"
        south_json_path = south_slice_path + ".json"
        
        # Extract Southern Hemisphere region (longitude 180-220, latitude -40-0)
        logger.info("Slicing GRIB to Southern Hemisphere region")
        south_slice_process = await create_subprocess_shell(
            f"wgrib2 {tmp.name} -small_grib 180:220 -40:0 {south_slice_path}"
        )
        await south_slice_process.wait()
        
        south_result = []
        if Path(south_slice_path).exists():
            # Convert Southern Hemisphere GRIB to JSON
            logger.info("Converting Southern Hemisphere GRIB to JSON")
            south_json_process = await create_subprocess_shell(
                f"grib2json -d 2 -n 3 -o {south_json_path} {south_slice_path}"
            )
            await south_json_process.wait()
            
            if Path(south_json_path).exists():
                # Read and save the Southern Hemisphere JSON data
                south_data = Path(south_json_path).read_bytes()
                south_fn = save(ctx, "ww3_south_pacific.json", south_data)
                
                south_result = [{
                    "source": "WW3-South",
                    "filename": south_fn,
                    "type": "model",
                    "region": "south_pacific",
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                }]
        
        return [{
            "source": "WW3",
            "filename": fn,
            "type": "model",
            "region": "hawaii",
            "priority": 0,
            "timestamp": utils.utcnow()
        }] + south_result
        
    except Exception as e:
        logger.error(f"Error processing WW3 data: {e}")
        return []
        
    finally:
        # Clean up temporary files
        for path in [tmp.name, slice_path, json_path, 
                    south_slice_path if 'south_slice_path' in locals() else "", 
                    south_json_path if 'south_json_path' in locals() else ""]:
            try:
                if path and Path(path).exists():
                    Path(path).unlink()
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {path}: {e}")

# Add enhanced ECMWF model client if you have API access
async def ecmwf_agent(ctx, sess, fetch, save):
    """Get ECMWF wave forecasts if API key is available"""
    logger = logging.getLogger("ecmwf_agent")
    
    # Check for API key
    api_key = ctx.cfg["API"].get("ECMWF_KEY", "").strip()
    if not api_key:
        logger.info("No ECMWF API key found in config, skipping")
        return []
    
    # Define regions to fetch - Hawaii and surrounding areas
    regions = [
        {"name": "hawaii", "area": [30, -170, 15, -150]},
        {"name": "north_pacific", "area": [60, -180, 30, -120]},
        {"name": "south_pacific", "area": [0, -180, -40, -120]}
    ]
    
    results = []
    
    # Try multiple endpoints - different ECMWF APIs may work
    endpoints = [
        "https://api.ecmwf.int/v1/services/opendata/wave",
        "https://api.ecmwf.int/v1/wave",
        "https://data.ecmwf.int/forecasts/wave",
        "https://api.ecmwf.int/v1/datasets/wave/forecasts"
    ]
    
    for region in regions:
        region_name = region["name"]
        area = region["area"]
        
        logger.info(f"Requesting ECMWF wave forecast data for {region_name} using opendata client")
        
        for endpoint in endpoints:
            try:
                # Prepare request body
                request_data = {
                    "area": area,  # [north, west, south, east]
                    "params": ["swh", "mwd", "pp1d"],  # Significant height, direction, period
                    "apikey": api_key
                }
                
                # Attempt fetch
                data = await fetch(ctx, sess, endpoint, method="POST", json_body=request_data)
                
                if data:
                    # Success! Save data and return
                    fn = save(ctx, f"ecmwf_{region_name}_wave.json", data)
                    results.append({
                        "source": "ECMWF",
                        "filename": fn,
                        "type": "wave_model",
                        "region": region_name,
                        "priority": 1,
                        "timestamp": utils.utcnow(),
                        "north_facing": region_name == "north_pacific",
                        "south_facing": region_name == "south_pacific"
                    })
                    # No need to try other endpoints for this region
                    break
                    
            except Exception as e:
                logger.warning(f"Failed to fetch ECMWF wave data for {region_name} from {endpoint}: {e}")
                # Continue to next endpoint
    
    if not results:
        logger.error(f"All ECMWF endpoints failed for {region_name}: {e}")
    
    return results

# External model agents from various weather centers
# Add more functions for other model sources as needed