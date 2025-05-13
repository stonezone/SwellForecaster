#!/usr/bin/env python3
# pacific_forecast_analyzer.py – turn image bundle → surf forecast via GPT‑4.1
from __future__ import annotations
import base64, json, logging, sys, utils, configparser, os, httpx, re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from io import BytesIO
from PIL import Image
from openai import OpenAI
import requests
import markdown
from weasyprint import HTML, CSS

# Import the new North Pacific analysis module
import north_pacific_analysis

log = utils.log_init("analyzer")

# Load prompt templates from the JSON file
def load_prompts(prompts_file="prompts.json"):
    """
    Load prompt templates from the JSON configuration file.

    This function provides a flexible prompt templating system that allows
    customization of AI-generated forecasts without modifying code. The templates
    use string interpolation with {variable} syntax to inject dynamic data at runtime.

    Structure of prompts.json:
    - forecast: Main forecasting prompts
      - intro: System instruction for the AI forecaster
      - emphasis: Configurable emphasis sections (north/south/both shores)
      - data_sources: Template for listing available data sources
      - structure: Templates for different forecast structural components
      - specialized: Domain-specific analysis templates
    - chart_generation: Templates for generating visual charts

    Args:
        prompts_file (str): Path to the JSON file containing prompt templates.
                           Defaults to "prompts.json" in the current directory.

    Returns:
        dict: A nested dictionary of prompt templates, or None if loading fails.
              When None is returned, the code falls back to hardcoded prompts.

    See also:
        /docs/prompt_templates.md for a comprehensive guide to the templating system.
    """
    try:
        with open(prompts_file, 'r') as f:
            templates = json.load(f)
            log.info(f"Successfully loaded prompt templates from {prompts_file}")
            return templates
    except Exception as e:
        log.warning(f"Failed to load prompts from {prompts_file}: {e}")
        log.warning("Using default hardcoded prompts")
        return None

# Load the prompts at module initialization
PROMPTS = load_prompts()

def prepare(img_path: Path, max_bytes=200_000) -> tuple[str,str]:
    """return (format, base64str) <= 200 kB"""
    im = Image.open(img_path)
    # Convert palette mode (P) to RGB before saving as JPEG
    if im.mode == 'P':
        im = im.convert('RGB')
    buf = BytesIO()
    im.save(buf, format="JPEG", quality=85, optimize=True)
    data = buf.getvalue()
    if len(data) > max_bytes:
        im.thumbnail((1024,1024)); buf = BytesIO(); im.save(buf, "JPEG", quality=80)
        data = buf.getvalue()
    return "jpeg", base64.b64encode(data).decode()

def load_bundle(data_dir: Path, bid: str | None):
    if not bid:
        bid = Path(data_dir/"latest_bundle.txt").read_text().strip()
    bdir = data_dir / bid
    meta = json.loads((bdir/"metadata.json").read_text())
    for r in meta["results"]:
        r["path"] = bdir / r["filename"]
    return meta, bdir

def path_to_str(obj):
    """Convert Path objects to strings for JSON serialization"""
    if isinstance(obj, dict):
        return {k: path_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [path_to_str(item) for item in obj]
    elif isinstance(obj, Path):
        return str(obj)
    else:
        return obj

def parse_buoy_data(bundle_dir: Path, meta):
    """Extract and structure NDBC buoy data for easier forecasting"""
    buoy_data = {}
    
    for r in meta["results"]:
        if r["source"] == "NDBC" and r["type"] == "realtime":
            try:
                buoy_id = r.get("buoy", r.get("station", "unknown"))
                content = Path(bundle_dir / r["filename"]).read_text()
                
                # Parse the NDBC text format (skip header lines)
                lines = content.strip().split('\n')
                if len(lines) < 2:
                    continue
                    
                headers = lines[0].split()
                data_line = lines[1].split()
                
                data = {}
                for i, header in enumerate(headers):
                    if i < len(data_line):
                        data[header] = data_line[i]
                
                # Add south-facing flag
                data["south_facing"] = r.get("south_facing", False)
                
                # Add trend data if we have more than one data line
                if len(lines) > 2:
                    try:
                        prev_line = lines[2].split()
                        if len(prev_line) >= len(headers):
                            trends = {}
                            for i, header in enumerate(headers):
                                if i < len(prev_line) and i < len(data_line):
                                    try:
                                        # Only process numeric data
                                        curr_val = float(data_line[i]) if data_line[i].replace('.', '', 1).isdigit() else None
                                        prev_val = float(prev_line[i]) if prev_line[i].replace('.', '', 1).isdigit() else None
                                        if curr_val is not None and prev_val is not None:
                                            trends[header] = "up" if curr_val > prev_val else "down" if curr_val < prev_val else "steady"
                                    except (ValueError, TypeError):
                                        # Skip non-numeric values
                                        pass
                            data["trends"] = trends
                    except Exception as e:
                        log.warning(f"Failed to parse trend data for buoy {buoy_id}: {e}")
                
                buoy_data[buoy_id] = data
            except Exception as e:
                log.warning(f"Failed to parse buoy data for {buoy_id}: {e}")
    
    return buoy_data

def parse_wind_data(bundle_dir: Path, meta):
    """Extract NOAA CO-OPS station wind data"""
    wind_data = {}
    
    for r in meta["results"]:
        if r["source"] == "NOAA-COOPS" and r["type"] == "wind_observation":
            try:
                location = r["location"]
                content = json.loads(Path(bundle_dir / r["filename"]).read_text())
                
                if "data" in content and len(content["data"]) > 0:
                    # Get the most recent observation
                    latest = content["data"][0]
                    wind_data[location] = {
                        "speed": latest.get("s", "N/A"),
                        "direction": latest.get("d", "N/A"),
                        "direction_text": latest.get("dr", "N/A"),
                        "time": latest.get("t", "N/A"),
                        "gust": latest.get("g", "N/A")
                    }
                    
                    # Add trend data if we have multiple observations
                    if len(content["data"]) > 1:
                        prev = content["data"][1]
                        try:
                            curr_speed = float(latest.get("s", "0")) if latest.get("s", "").replace('.', '', 1).isdigit() else 0
                            prev_speed = float(prev.get("s", "0")) if prev.get("s", "").replace('.', '', 1).isdigit() else 0
                            wind_data[location]["speed_trend"] = "up" if curr_speed > prev_speed else "down" if curr_speed < prev_speed else "steady"
                        except (ValueError, TypeError):
                            wind_data[location]["speed_trend"] = "unknown"
            except Exception as e:
                log.warning(f"Failed to parse wind data for {location}: {e}")
    
    return wind_data

def parse_model_data(bundle_dir: Path, meta):
    """Extract and structure wave model data from WW3 and other sources"""
    model_data = {
        "hawaii": {},
        "north_pacific": {},
        "south_pacific": {}
    }

    # Process WW3 model data
    for r in meta["results"]:
        if r.get("source") in ["WW3", "WW3-Fallback"] and r.get("type") == "model":
            try:
                region = r.get("region", "hawaii")
                # Read the WW3 JSON data
                content = Path(bundle_dir / r["filename"]).read_text()
                try:
                    json_data = json.loads(content)

                    # Extract significant wave heights and periods
                    wave_data = {}
                    for record in json_data:
                        param_name = record.get("header", {}).get("parameterName", "")
                        if param_name in ["HTSGW", "PERPW", "DIRPW"]:  # Height, Period, Direction
                            wave_data[param_name] = record.get("data", [])

                    # Add processed data to the appropriate region
                    if region == "hawaii":
                        model_data["hawaii"] = wave_data
                    elif region == "south_pacific":
                        model_data["south_pacific"] = wave_data
                    else:
                        model_data["north_pacific"] = wave_data

                except json.JSONDecodeError:
                    log.warning(f"Failed to parse WW3 JSON data for {region}")
            except Exception as e:
                log.warning(f"Failed to process model data: {e}")

    return model_data

def parse_southern_hemisphere_data(bundle_dir: Path, meta):
    """Extract and structure Southern Hemisphere data for South Shore forecasting"""
    southern_data = {
        "charts": [],
        "forecasts": [],
        "historical_analogs": [],
        "storms": [],  # New section for detected storm systems
        "caldwell_analysis": None,  # Special section for Pat Caldwell's analysis
        "surfline_region_analysis": None  # Surfline South Pacific regional analysis
    }

    # Track which data sources we successfully processed
    processed_sources = set()

    for r in meta["results"]:
        if r.get("source") == "SouthernHemisphere":
            try:
                data_type = r.get("type", "unknown")
                processed_sources.add(data_type)

                if data_type == "chart":
                    southern_data["charts"].append({
                        "subtype": r.get("subtype", "unknown"),
                        "url": r.get("url", ""),
                        "filename": r.get("filename", ""),
                        "path_str": str(bundle_dir / r.get("filename", ""))
                    })
                elif data_type == "forecast":
                    # Try to parse JSON data if available
                    try:
                        content = json.loads(Path(bundle_dir / r["filename"]).read_text())
                        provider = r.get("provider", "unknown")

                        # Extract storm data from Surfline if available
                        if provider == "surfline" and "forecast" in content:
                            try:
                                # Extract storm data from surfline format
                                for day in content.get("forecast", {}).get("wave", []):
                                    if "surf" in day and day.get("surf", {}).get("max") > 2:  # Lowered threshold from 3 to 2
                                        storm_data = {
                                            "date": day.get("timestamp", ""),
                                            "height": day.get("surf", {}).get("max"),
                                            "period": day.get("swells", [{}])[0].get("period"),
                                            "direction": day.get("swells", [{}])[0].get("direction"),
                                            "source": "surfline"
                                        }
                                        southern_data["storms"].append(storm_data)
                            except Exception as e:
                                log.warning(f"Failed to extract storm data from Surfline: {e}")

                        southern_data["forecasts"].append({
                            "provider": provider,
                            "data": content
                        })
                    except Exception as e:
                        log.warning(f"Failed to parse Southern Hemisphere forecast JSON: {e}")
                elif data_type == "historical_analog":
                    southern_data["historical_analogs"].append({
                        "date": r.get("date", "unknown"),
                        "url": r.get("url", ""),
                        "filename": r.get("filename", "")
                    })
                elif data_type == "text_forecast":
                    # Process text forecasts
                    try:
                        if bundle_dir is not None:
                            content = Path(bundle_dir / r["filename"]).read_text()
                        else:
                            log.warning("Cannot analyze Caldwell forecast: bundle_dir is None")
                            continue
                        provider = r.get("provider", "unknown")

                        # Special handling for Pat Caldwell's forecast
                        if provider == "caldwell":
                            # Extract the South Pacific section from Caldwell's text
                            import re
                            # Pat's forecasts follow a consistent format with sections
                            south_pacific_section = None
                            south_regex = re.search(r'SOUTH\s+PACIFIC.*?(?=NORTH PACIFIC|WIND AND SEA STATE|$)',
                                                  content, re.DOTALL | re.IGNORECASE)

                            if south_regex:
                                south_pacific_section = south_regex.group(0).strip()

                                # Store the complete South Pacific analysis
                                southern_data["caldwell_analysis"] = south_pacific_section

                                # Enhanced extraction of multi-component south swell information
                                # Check for Caldwell's table format first (most precise source)
                                table_pattern = r"(\d+\.?\d?)\s+(S|SSW|SW|SSE)\s+(\d+)\s+\d+\s+\d+"
                                table_matches = re.finditer(table_pattern, content, re.IGNORECASE)

                                # Process tabular swell data for most precise information
                                south_components = []
                                for match in table_matches:
                                    groups = match.groups()
                                    if len(groups) >= 3:
                                        component = {
                                            "source": "caldwell_table",
                                            "height": float(groups[0]),
                                            "direction": groups[1].upper(),
                                            "period": int(groups[2]),
                                            "match_text": match.group(0)
                                        }
                                        south_components.append(component)

                                # Now look for detailed storm descriptions and fetch information
                                # Patterns for storm systems and fetch geography
                                storm_patterns = [
                                    # Pattern for New Zealand area storms with timing
                                    r"((?:Low|Gale|Storm)[^.]*?(?:New Zealand|NZ|Tasman)[^.]*?)(\d+[/-]\d+)(?:-\d+[/-]\d+)?",

                                    # Pattern for specific storm phases with dates
                                    r"Phase\s+(\d+)[^.]*?((?:New Zealand|NZ|Tasman|E of NZ)[^.]*?)(\d+[/-]\d+)(?:-\d+[/-]\d+)?",

                                    # Pattern for satellite/instrument validation
                                    r"(ASCAT|JASON)[^.]*?(validated|showed)[^.]*?(\d+)(?:'|\s*ft)",

                                    # Pattern for arrival predictions
                                    r"((?:average|notch above|overhead|peaking)[^.]*?)(\d+[/-]\d+)(?:-\d+[/-]\d+)?[^.]*?((?:from|degrees)[^.]*?(\d+)(?:-(\d+))?)",
                                ]

                                # Extract detailed storm information
                                storm_descriptions = []
                                for pattern in storm_patterns:
                                    matches = re.finditer(pattern, south_pacific_section, re.IGNORECASE)
                                    for match in matches:
                                        groups = match.groups()
                                        storm_info = {
                                            "match_text": match.group(0),
                                            "pattern_type": "storm_system"
                                        }

                                        # Extract available information based on pattern
                                        if "Phase" in pattern:
                                            storm_info["phase"] = groups[0] if len(groups) > 0 else None
                                            storm_info["location"] = groups[1] if len(groups) > 1 else None
                                            storm_info["date"] = groups[2] if len(groups) > 2 else None
                                        elif "ASCAT" in pattern or "JASON" in pattern:
                                            storm_info["instrument"] = groups[0] if len(groups) > 0 else None
                                            storm_info["observation"] = groups[1] if len(groups) > 1 else None
                                            storm_info["seas_height"] = groups[2] if len(groups) > 2 else None
                                        elif "average" in pattern or "notch above" in pattern or "overhead" in pattern:
                                            storm_info["description"] = groups[0] if len(groups) > 0 else None
                                            storm_info["arrival_date"] = groups[1] if len(groups) > 1 else None
                                            storm_info["direction_desc"] = groups[2] if len(groups) > 2 else None
                                            storm_info["direction_min"] = groups[3] if len(groups) > 3 else None
                                            storm_info["direction_max"] = groups[4] if len(groups) > 4 else None
                                        else:
                                            storm_info["description"] = groups[0] if len(groups) > 0 else None
                                            storm_info["date"] = groups[1] if len(groups) > 1 else None

                                        storm_descriptions.append(storm_info)

                                # Also check for Caldwell's typical precise directional bands
                                direction_pattern = r"(\d+)(?:-(\d+))?\s*degrees"
                                dir_matches = re.finditer(direction_pattern, south_pacific_section, re.IGNORECASE)

                                for match in dir_matches:
                                    groups = match.groups()
                                    direction_info = {
                                        "direction_min": groups[0] if len(groups) > 0 else None,
                                        "direction_max": groups[1] if len(groups) > 1 else None,
                                        "match_text": match.group(0),
                                        "pattern_type": "precise_direction"
                                    }
                                    storm_descriptions.append(direction_info)

                                # Process all the extracted information into well-structured storm components
                                if south_components or storm_descriptions:
                                    # First add the table-derived components (highest precision)
                                    for component in south_components:
                                        southern_data["storms"].append({
                                            "source": "text_forecast",
                                            "provider": "caldwell",
                                            "indication": f"{component['direction']} swell",
                                            "period": component["period"],
                                            "height": component["height"],
                                            "direction": component["direction"],
                                            "content_excerpt": component["match_text"],
                                            "component_type": "precise_tabular"
                                        })

                                    # Then add the narrative storm descriptions for context
                                    for description in storm_descriptions:
                                        storm_data = {
                                            "source": "text_forecast",
                                            "provider": "caldwell",
                                            "indication": "Detailed storm system",
                                            "content_excerpt": description["match_text"],
                                            "component_type": "storm_narrative"
                                        }

                                        # Add any extracted specific attributes
                                        for key, value in description.items():
                                            if key not in ["match_text", "pattern_type"] and value is not None:
                                                storm_data[key] = value

                                        southern_data["storms"].append(storm_data)

                                # If no components found through detailed parsing, fall back to simple indicators
                                if not (south_components or storm_descriptions):
                                    swell_indicators = ["SSW", "S swell", "south swell", "southern hemisphere"]
                                    found_indicators = []

                                    for indicator in swell_indicators:
                                        if indicator.lower() in south_pacific_section.lower():
                                            found_indicators.append(indicator)

                                    if found_indicators:
                                        # Try to extract period and height information
                                        period_match = re.search(r'(\d+)(?:-(\d+))?\s*(?:s(?:ec)?|second)', south_pacific_section, re.IGNORECASE)
                                        height_match = re.search(r'(\d+)(?:-(\d+))?\s*(?:ft|foot|feet)', south_pacific_section, re.IGNORECASE)

                                        period = int(period_match.group(1)) if period_match else None
                                        height = float(height_match.group(1)) if height_match else None

                                        # Add to storms if we found good indicators
                                        southern_data["storms"].append({
                                            "source": "text_forecast",
                                            "provider": "caldwell",
                                            "indication": ", ".join(found_indicators),
                                            "period": period,
                                            "height": height,
                                            "component_type": "basic_indicators",
                                            "content_excerpt": south_pacific_section[:300] + "..." if len(south_pacific_section) > 300 else south_pacific_section
                                        })

                        # Process SNN forecasts
                        elif provider == "snn":
                            # Look for south swell mentions
                            if any(term in content for term in ["SSW", "South swell", "South Shore"]):
                                southern_data["storms"].append({
                                    "source": "text_forecast",
                                    "provider": "snn",
                                    "indication": "South swell mentioned",
                                    "content_sample": content[:200] + "..."  # Just a small sample
                                })

                        # Process Surfline South Pacific region forecasts
                        elif provider == "surfline_region":
                            # Store the whole content for reference
                            southern_data["surfline_region_analysis"] = content[:1000] + "..." if len(content) > 1000 else content

                            # Look for storm mentions
                            storm_indicators = ["storm", "gale", "low pressure", "fetch", "significant swell"]
                            for indicator in storm_indicators:
                                if indicator in content.lower():
                                    southern_data["storms"].append({
                                        "source": "text_forecast",
                                        "provider": "surfline_region",
                                        "indication": f"South Pacific {indicator} mentioned",
                                        "content_sample": content[:200] + "..."
                                    })
                                    break

                    except Exception as e:
                        log.warning(f"Failed to parse text forecast: {e}")
            except Exception as e:
                log.warning(f"Failed to process Southern Hemisphere data: {e}")

    # Log which data sources we processed successfully
    log.info(f"Processed Southern Hemisphere data sources: {processed_sources}")

    return southern_data

def process_ecmwf_data(bundle_dir: Path, meta):
    """Extract and structure ECMWF wave model data"""
    ecmwf_data = {
        "hawaii": {},
        "north_pacific": {},
        "south_pacific": {}
    }
    
    for r in meta["results"]:
        if r["source"] == "ECMWF" and r["type"] == "wave_model":
            try:
                region = r.get("subtype", "").split("_")[0]
                if region in ecmwf_data:
                    # For GRIB files, we'd need to parse them
                    # For now, just record their presence
                    ecmwf_data[region][r.get("description", "unknown")] = {
                        "filename": r["filename"],
                        "timestamp": r["timestamp"],
                        "north_facing": r.get("north_facing", False),
                        "south_facing": r.get("south_facing", False)
                    }
            except Exception as e:
                log.warning(f"Failed to process ECMWF data: {e}")
    
    return ecmwf_data

def process_bom_data(bundle_dir: Path, meta):
    """Extract and structure Australian BOM data"""
    bom_data = {
        "forecasts": [],
        "charts": [],
        "observations": []
    }
    
    for r in meta["results"]:
        if r["source"] == "BOM":
            try:
                data_type = r["type"]
                
                if data_type == "text_forecast":
                    content = json.loads(Path(bundle_dir / r["filename"]).read_text())
                    bom_data["forecasts"].append({
                        "district": r.get("district", "unknown"),
                        "description": r.get("description", "unknown"),
                        "data": content
                    })
                elif data_type == "chart":
                    bom_data["charts"].append({
                        "subtype": r.get("subtype", "unknown"),
                        "forecast_hour": r.get("forecast_hour", 0),
                        "filename": r["filename"],
                        "path": str(bundle_dir / r["filename"])
                    })
                elif data_type == "observations":
                    content = json.loads(Path(bundle_dir / r["filename"]).read_text())
                    bom_data["observations"].append({
                        "subtype": r.get("subtype", "unknown"),
                        "data": content
                    })
            except Exception as e:
                log.warning(f"Failed to process BOM data: {e}")
    
    return bom_data

def select(meta, n=12, bundle_dir=None):  # Increased from 10 to 12 to include more data
    """Select and prioritize images based on relevance"""
    # Categorize images by type
    buoy_charts = []
    opc_charts = []
    wpc_charts = []
    pacioos_charts = []
    southern_hemisphere_charts = []  # New category for Southern Hemisphere
    north_pacific_charts = []        # New category for North Pacific
    ecmwf_charts = []                # New category for ECMWF
    bom_charts = []                  # New category for BOM
    other_charts = []

    # Track if we have Pat Caldwell forecast data
    has_caldwell_forecast = False

    # Find bundle directory from metadata if not provided
    if bundle_dir is None and "results" in meta and len(meta["results"]) > 0:
        # Extract bundle directory from the first result's path
        for r in meta["results"]:
            if "path" in r and isinstance(r["path"], Path):
                bundle_dir = r["path"].parent
                break

    for r in meta["results"]:
        if r.get("provider") == "caldwell" or r.get("source") == "PatCaldwell":
            has_caldwell_forecast = True

        if any(r.get("filename", "").lower().endswith(x) for x in (".png",".gif",".jpg",".tif")):
            if r.get("isobars", False) or r.get("subtype") in ["pacific_surface", "pacific_surface_24hr", "pacific_surface_48hr"]:
                # Surface analysis/forecast charts with isobars should be prioritized highest
                opc_charts.insert(0, r)  # Insert at beginning to prioritize
            elif r["source"] == "OPC":
                opc_charts.append(r)
            elif r["source"] == "WPC":
                wpc_charts.append(r)
            # Filter out PacIOOS error images
            elif r["source"] == "PacIOOS" and not any(error_img in r.get("filename", "") for error_img in
                                                 ["pacioos_ns_now.png", "pacioos_24h.png", "pacioos_48h.png",
                                                  "pacioos_now.png", "pacioos_ns_24h.png"]):
                pacioos_charts.append(r)
            elif r["source"] == "NDBC":
                buoy_charts.append(r)
            elif r["source"] == "SouthernHemisphere":
                southern_hemisphere_charts.append(r)
            elif r["source"] == "NorthPacific":
                north_pacific_charts.append(r)
            elif r["source"] == "ECMWF":
                ecmwf_charts.append(r)
            elif r["source"] == "BOM":
                bom_charts.append(r)
            elif r["source"] == "WW3-Fallback":
                # Prioritize WW3 data if available
                other_charts.insert(0, r)
            else:
                other_charts.append(r)

    # Sort each category by priority
    for charts in [opc_charts, wpc_charts, pacioos_charts, buoy_charts,
                  southern_hemisphere_charts, north_pacific_charts,
                  ecmwf_charts, bom_charts, other_charts]:
        charts.sort(key=lambda r: r.get("priority", 9))

    # Check if we should prioritize Southern Hemisphere data
    south_emphasis = False
    north_emphasis = False

    # Check for significant south swells - only if we have a bundle directory
    significant_swells = []
    has_significant_south_swells = False
    if bundle_dir is not None:
        try:
            significant_swells = extract_significant_south_swells(meta, bundle_dir)
            has_significant_south_swells = bool(significant_swells)
        except Exception as e:
            log.warning(f"Failed to extract significant south swells: {e}")

    try:
        # Check if there's a south_swell_emphasis flag in config
        cfg = configparser.ConfigParser()
        cfg.read("config.ini")
        south_emphasis = cfg["FORECAST"].getboolean("south_swell_emphasis", False)
        north_emphasis = cfg["FORECAST"].getboolean("north_swell_emphasis", False)
    except:
        # Auto-detect based on data sources, significant swells, and Caldwell forecast
        if has_significant_south_swells:
            south_emphasis = True
            log.info("Auto-enabling south swell emphasis due to detected significant swells")
        elif has_caldwell_forecast:
            # Check if Caldwell mentions significant south swells
            for r in meta["results"]:
                if r.get("provider") == "caldwell" and r.get("type") == "text_forecast":
                    try:
                        if bundle_dir is not None:
                            content = Path(bundle_dir / r["filename"]).read_text()
                        else:
                            log.warning("Cannot analyze Caldwell forecast: bundle_dir is None")
                            continue
                        # Simple pattern matching for south swell indicators
                        if re.search(r"(significant|building)\s+\w+\s+s(outh)?\s+swell", content, re.I):
                            south_emphasis = True
                            log.info("Auto-enabling south swell emphasis based on Pat Caldwell's forecast")
                            break
                    except Exception as e:
                        log.warning(f"Failed to analyze Caldwell forecast: {e}")

        # If still no detection, use original fallback
        if not south_emphasis:
            south_emphasis = len(southern_hemisphere_charts) >= 2 or len(bom_charts) >= 2
        if not north_emphasis:
            north_emphasis = len(north_pacific_charts) >= 2 or len(ecmwf_charts) >= 2
    
    # Combine in order of importance
    selected = []
    
    if south_emphasis and north_emphasis:
        # Both shores are important
        selected.extend(pacioos_charts[:2])             # PacIOOS first
        selected.extend(southern_hemisphere_charts[:1]) # Southern Hemisphere
        selected.extend(bom_charts[:1])                 # Australian BOM
        selected.extend(north_pacific_charts[:1])       # North Pacific
        selected.extend(ecmwf_charts[:1])               # ECMWF
        selected.extend(opc_charts[:2])                 # OPC
        selected.extend(wpc_charts[:1])                 # WPC
    elif south_emphasis:
        # Prioritize Southern Hemisphere charts
        selected.extend(southern_hemisphere_charts[:2])  # More Southern Hemisphere
        selected.extend(bom_charts[:1])                 # Australian BOM
        selected.extend(pacioos_charts[:2])             # PacIOOS next
        selected.extend(opc_charts[:2])                 # OPC charts 
        selected.extend(north_pacific_charts[:1])       # Some North Pacific
        selected.extend(wpc_charts[:1])                 # Fewer WPC charts
    elif north_emphasis:
        # Prioritize North Pacific charts
        selected.extend(north_pacific_charts[:2])       # More North Pacific
        selected.extend(ecmwf_charts[:1])               # ECMWF
        selected.extend(pacioos_charts[:2])             # PacIOOS next
        selected.extend(opc_charts[:2])                 # OPC charts
        selected.extend(southern_hemisphere_charts[:1]) # Some Southern Hemisphere
        selected.extend(wpc_charts[:1])                 # Fewer WPC charts
    else:
        # Original prioritization
        selected.extend(pacioos_charts[:2])             # PacIOOS first
        selected.extend(southern_hemisphere_charts[:1]) # Southern Hemisphere
        selected.extend(bom_charts[:1])                 # Australian BOM
        selected.extend(north_pacific_charts[:1])       # North Pacific
        selected.extend(ecmwf_charts[:1])               # ECMWF
        selected.extend(opc_charts[:2])                 # OPC charts
        selected.extend(wpc_charts[:1])                 # WPC charts
    
    # Fill the rest with fallbacks and other sources
    remaining_slots = n - len(selected)
    if remaining_slots > 0:
        selected.extend(other_charts[:remaining_slots])
    
    return selected[:n]  # Limit to n images

def extract_model_json(bundle_dir: Path, meta, source_type):
    """Extract data from JSON model sources"""
    data = {}
    
    for r in meta["results"]:
        if source_type in r.get("type", "") and r.get("filename", "").endswith(".json"):
            try:
                source_name = r.get("source", "Unknown")
                if isinstance(r.get("location"), dict):
                    location = r.get("location", {}).get("name", "Unknown")
                else:
                    location = r.get("location", "Unknown") 
                
                key = f"{source_name}_{location}"
                content = json.loads(Path(bundle_dir / r["filename"]).read_text())
                
                data[key] = {
                    "source": source_name,
                    "location": location,
                    "data": content,
                    "south_facing": r.get("south_facing", False),
                    "north_facing": r.get("north_facing", False)
                }
            except Exception as e:
                log.warning(f"Failed to parse JSON data for {source_type}: {e}")
    
    return data

def extract_multi_component_swells(southern_data):
    """
    Enhanced function to extract and separate multiple overlapping swell components.
    This is particularly important for South Shore forecasts where Pat Caldwell often
    identifies multiple overlapping swell events with precise directional bands.

    Args:
        southern_data: Dictionary of Southern Hemisphere data containing storm information

    Returns:
        List of separated swell components with timing, direction, height, and period
    """
    swell_components = []

    # Process Caldwell's precise tabular data first (most accurate)
    for storm in southern_data.get("storms", []):
        if storm.get("component_type") == "precise_tabular" and storm.get("provider") == "caldwell":
            component = {
                "direction": storm.get("direction"),
                "height": storm.get("height"),
                "period": storm.get("period"),
                "arrival_date": None,  # Will try to fill this in from narrative data
                "peak_date": None,     # Will try to fill this in from narrative data
                "source": "caldwell_table",
                "confidence": "high",
                "precise_direction": True
            }
            swell_components.append(component)

    # Process narrative data to extract arrival and peak timing information
    arrival_info = {}
    for storm in southern_data.get("storms", []):
        if storm.get("component_type") == "storm_narrative" and storm.get("provider") == "caldwell":
            # Look for specific timing information
            if storm.get("arrival_date"):
                direction_min = storm.get("direction_min")
                direction_max = storm.get("direction_max")
                if direction_min or direction_max:
                    # Create a directional key
                    if direction_min and direction_max:
                        dir_key = f"{direction_min}-{direction_max}"
                    else:
                        dir_key = direction_min or "south"

                    arrival_info[dir_key] = {
                        "arrival_date": storm.get("arrival_date"),
                        "peak_date": None,  # Often in a separate description
                        "description": storm.get("description", "")
                    }

            # Check for peak information
            if "peaking" in storm.get("match_text", "").lower():
                # Try to extract direction and date
                for dir_key, info in arrival_info.items():
                    if info["arrival_date"] and storm.get("arrival_date"):
                        if storm.get("arrival_date") >= info["arrival_date"]:
                            info["peak_date"] = storm.get("arrival_date")

    # Now match timing information with the precise tabular components
    for component in swell_components:
        direction = component.get("direction")
        if direction:
            # Look for matching arrival info
            for dir_key, info in arrival_info.items():
                if direction in dir_key or (direction == "S" and "south" in dir_key.lower()):
                    component["arrival_date"] = info.get("arrival_date")
                    component["peak_date"] = info.get("peak_date")
                    component["description"] = info.get("description", "")
                    break

    # If we don't have tabular data, create components from narrative data
    if not swell_components:
        # Extract from storm narratives
        unique_directions = set()
        for storm in southern_data.get("storms", []):
            if storm.get("component_type") == "storm_narrative" and storm.get("provider") == "caldwell":
                direction_desc = storm.get("direction_desc", "")
                direction_min = storm.get("direction_min")
                direction_max = storm.get("direction_max")

                if direction_min or direction_max or "south" in direction_desc.lower():
                    # Create a unique direction key
                    if direction_min and direction_max:
                        dir_key = f"{direction_min}-{direction_max}"
                    elif direction_min:
                        dir_key = direction_min
                    else:
                        dir_key = "south"

                    if dir_key not in unique_directions:
                        unique_directions.add(dir_key)
                        # Try to extract height and period from the description
                        height = None
                        period = None
                        match_text = storm.get("match_text", "")

                        # Extract height if available
                        height_match = re.search(r'(\d+(?:\.\d+)?)(?:-\d+(?:\.\d+)?)?\s*(?:ft|foot|feet)', match_text, re.IGNORECASE)
                        if height_match:
                            try:
                                height = float(height_match.group(1))
                            except (ValueError, TypeError):
                                pass

                        # Extract period if available
                        period_match = re.search(r'(\d+)(?:-\d+)?\s*(?:s(?:ec)?|second)', match_text, re.IGNORECASE)
                        if period_match:
                            try:
                                period = int(period_match.group(1))
                            except (ValueError, TypeError):
                                pass

                        # Create a component
                        if dir_key != "south" or height or period:  # Require some specific info
                            component = {
                                "direction": dir_key,
                                "height": height,
                                "period": period,
                                "arrival_date": storm.get("arrival_date") or storm.get("date"),
                                "source": "caldwell_narrative",
                                "confidence": "medium",
                                "description": match_text[:100] + "..." if len(match_text) > 100 else match_text,
                                "precise_direction": bool(direction_min or direction_max)
                            }
                            swell_components.append(component)

    # Add components from other sources (surfline, etc.) for comparison
    for storm in southern_data.get("storms", []):
        if storm.get("source") == "surfline" and storm.get("height") and storm.get("period"):
            # Convert surfline direction to text
            direction = storm.get("direction")
            direction_text = "S"
            if direction:
                if 170 <= direction <= 190:
                    direction_text = "S"
                elif 190 < direction <= 210:
                    direction_text = "SSW"
                elif 210 < direction <= 230:
                    direction_text = "SW"
                elif 150 <= direction < 170:
                    direction_text = "SSE"

            component = {
                "direction": direction_text,
                "direction_degrees": direction,
                "height": storm.get("height"),
                "period": storm.get("period"),
                "arrival_date": storm.get("date"),
                "source": "surfline",
                "confidence": "medium",
                "precise_direction": True
            }
            swell_components.append(component)

    # Sort by direction, period, and confidence
    swell_components.sort(key=lambda x: (
        0 if x.get("direction") == "S" else
        1 if x.get("direction") == "SSW" else
        2 if x.get("direction") == "SW" else 3,
        -1 * (x.get("period") or 0),
        0 if x.get("confidence") == "high" else
        1 if x.get("confidence") == "medium" else 2
    ))

    return swell_components

def extract_significant_south_swells(meta, bundle_dir):
    """Analyze all data sources to detect significant south swells"""
    significant_swells = []

    # Check buoy data for south-facing buoys
    for r in meta["results"]:
        if r["source"] == "NDBC" and r.get("south_facing", False):
            try:
                buoy_id = r.get("buoy", r.get("station", "unknown"))
                content = Path(bundle_dir / r["filename"]).read_text()

                lines = content.strip().split('\n')
                if len(lines) < 2:
                    continue

                headers = lines[0].split()
                data_line = lines[1].split()

                # Extract wave height and period
                wvht_idx = next((i for i, h in enumerate(headers) if h == "WVHT"), None)
                dpd_idx = next((i for i, h in enumerate(headers) if h == "DPD"), None)
                mwd_idx = next((i for i, h in enumerate(headers) if h == "MWD"), None)

                if all(idx is not None for idx in [wvht_idx, dpd_idx, mwd_idx]) and len(data_line) > max(wvht_idx, dpd_idx, mwd_idx):
                    try:
                        # Only process if data is numeric
                        if (data_line[wvht_idx].replace('.', '', 1).isdigit() and
                            data_line[dpd_idx].replace('.', '', 1).isdigit() and
                            data_line[mwd_idx].replace('.', '', 1).isdigit()):

                            wvht = float(data_line[wvht_idx])
                            dpd = float(data_line[dpd_idx])
                            mwd = float(data_line[mwd_idx])

                            # Check if this indicates a significant south swell
                            # Widened the south direction range to 150-220 degrees for better detection
                            if 150 <= mwd <= 220 and dpd >= 12 and wvht >= 1.5:
                                significant_swells.append({
                                    "source": "buoy",
                                    "buoy_id": buoy_id,
                                    "height": wvht,
                                    "period": dpd,
                                    "direction": mwd,
                                    "confidence": "high"
                                })
                    except (ValueError, IndexError) as e:
                        log.warning(f"Failed to process buoy data for south swells: {e}")
            except Exception as e:
                log.warning(f"Failed to analyze buoy data for south swells: {e}")

    # Check model data from SWAN, Open-Meteo, etc.
    for r in meta["results"]:
        if r.get("south_facing", False) and r.get("type", "").startswith(("marine_forecast", "swan_south_shore")):
            try:
                if r["filename"].endswith(".json"):
                    content = json.loads(Path(bundle_dir / r["filename"]).read_text())

                    # Process Open-Meteo marine data
                    if r["source"] == "Open-Meteo" and "hourly" in content:
                        if all(k in content["hourly"] for k in ["wave_height", "wave_direction", "wave_period"]):
                            heights = content["hourly"]["wave_height"][:24]  # First 24 hours
                            directions = content["hourly"]["wave_direction"][:24]
                            periods = content["hourly"]["wave_period"][:24]

                            # Find max height in the next 24 hours
                            for i in range(len(heights)):
                                if isinstance(heights[i], (int, float)) and heights[i] >= 1.0:
                                    if 150 <= directions[i] <= 220 and periods[i] >= 12:
                                        significant_swells.append({
                                            "source": "model",
                                            "model": "Open-Meteo",
                                            "height": heights[i],
                                            "period": periods[i],
                                            "direction": directions[i],
                                            "confidence": "medium",
                                            "location": r.get("location", {}).get("name", "Unknown")
                                        })
                                        break  # Only add one entry per location
            except Exception as e:
                log.warning(f"Failed to analyze model data for south swells: {e}")

    # Check Windy API data
    for r in meta["results"]:
        if r["source"] == "Windy" and r.get("south_facing", False):
            try:
                content = json.loads(Path(bundle_dir / r["filename"]).read_text())
                if "swell1" in content and "swell2" in content:
                    # Extract all swell components for the first 72 hours
                    for swell_key in ["swell1", "swell2", "swell3"]:
                        if swell_key in content:
                            heights = content[swell_key].get("height", [])[:24]
                            directions = content[swell_key].get("direction", [])[:24]
                            periods = content[swell_key].get("period", [])[:24]

                            for i in range(min(len(heights), len(directions), len(periods))):
                                # Check south swell parameters
                                if (heights[i] >= 1.0 and
                                    150 <= directions[i] <= 220 and
                                    periods[i] >= 12):
                                    significant_swells.append({
                                        "source": "windy",
                                        "component": swell_key,
                                        "height": heights[i],
                                        "period": periods[i],
                                        "direction": directions[i],
                                        "confidence": "medium",
                                        "location": r.get("location", {}).get("name", "south_shore")
                                    })
                                    break  # Only add one entry per swell component
            except Exception as e:
                log.warning(f"Failed to analyze Windy data for south swells: {e}")

    # Check Southern Hemisphere data
    southern_data = parse_southern_hemisphere_data(bundle_dir, meta)
    if "storms" in southern_data and southern_data["storms"]:
        for storm in southern_data["storms"]:
            significant_swells.append({
                "source": "southern_hemisphere",
                "details": storm,
                "confidence": "medium"
            })

    # Check text forecasts for mentions of south swell
    for r in meta["results"]:
        if r.get("type") == "text_forecast" and r.get("provider") in ["caldwell", "snn", "surfline_region"]:
            try:
                content = Path(bundle_dir / r["filename"]).read_text().lower()

                # Look for specific south swell indicators in text forecasts
                south_swell_indicators = [
                    "south swell", "s swell", "ssw swell", "sse swell",
                    "south pacific storm", "southern hemisphere", "south shore"
                ]

                for indicator in south_swell_indicators:
                    if indicator in content:
                        # Try to extract period and height using regex patterns
                        import re
                        # Pattern for period (e.g., "14-16 second")
                        period_match = re.search(r'(\d+)[-]?(\d+)?\s*(?:second|s|sec|period)', content)
                        period = int(period_match.group(1)) if period_match else None

                        # Pattern for height (e.g., "2-3 feet")
                        height_match = re.search(r'(\d+)[-]?(\d+)?\s*(?:foot|ft|feet)', content)
                        height = float(height_match.group(1)) if height_match else None

                        significant_swells.append({
                            "source": "text_forecast",
                            "provider": r.get("provider"),
                            "indicator": indicator,
                            "period": period,
                            "height": height,
                            "confidence": "medium" if (period or height) else "low"
                        })
                        break  # Only add one entry per forecast source
            except Exception as e:
                log.warning(f"Failed to analyze text forecast for south swells: {e}")

    # Check BOM data for south swell indicators
    bom_data = process_bom_data(bundle_dir, meta)
    for forecast in bom_data.get("forecasts", []):
        # Look for indications of south swells in forecast data
        if "data" in forecast:
            forecast_data = forecast["data"]
            if isinstance(forecast_data, dict) and "forecast" in forecast_data:
                for period in forecast_data.get("forecast", []):
                    if isinstance(period, dict) and "swell" in period:
                        swell_info = period.get("swell", {})
                        direction = swell_info.get("direction")
                        height = swell_info.get("height")
                        period_value = swell_info.get("period")

                        # Check if this is a significant south swell (widened range)
                        if direction and height and period_value:
                            if 150 <= int(direction) <= 220 and float(height) >= 1.5 and float(period_value) >= 12:
                                significant_swells.append({
                                    "source": "bom",
                                    "height": float(height),
                                    "period": float(period_value),
                                    "direction": int(direction),
                                    "confidence": "medium"
                                })

    # Look at any detected swell from forecast models
    if significant_swells:
        log.info(f"Detected {len(significant_swells)} significant south swells")
        for i, swell in enumerate(significant_swells[:3]):  # Log the first 3
            log.info(f"South swell #{i+1}: {swell}")
    else:
        log.info("No significant south swells detected")

    return significant_swells

def forecast(cfg, meta, bundle_dir, imgs):
    client = OpenAI(api_key=cfg["API"]["OPENAI_KEY"])
    
    # Process specialized data sources
    buoy_data = parse_buoy_data(bundle_dir, meta)
    wind_data = parse_wind_data(bundle_dir, meta)
    
    # Extract model forecast data
    marine_forecasts = extract_model_json(bundle_dir, meta, "marine_forecast")
    wind_forecasts = extract_model_json(bundle_dir, meta, "wind_forecast")
    
    # Process Southern Hemisphere data
    southern_hemisphere_data = parse_southern_hemisphere_data(bundle_dir, meta)
    
    # Process ECMWF and BOM data
    ecmwf_data = process_ecmwf_data(bundle_dir, meta)
    bom_data = process_bom_data(bundle_dir, meta)

    # Process WW3 model data
    model_data = parse_model_data(bundle_dir, meta)

    # Get advanced North Shore analysis
    north_shore_analysis = north_pacific_analysis.get_north_shore_analysis(meta, bundle_dir)
    
    # Check for significant south swells
    significant_swells = extract_significant_south_swells(meta, bundle_dir)

    # Extract multi-component south swells using enhanced pattern recognition
    # This allows us to separate overlapping south swell components like Pat Caldwell does
    multi_component_swells = extract_multi_component_swells(southern_hemisphere_data)

    # Log the detected multi-component swells
    if multi_component_swells:
        log.info(f"Detected {len(multi_component_swells)} multi-component south swells using Caldwell-style analysis")
        for i, swell in enumerate(multi_component_swells[:3]):  # Log the first 3
            log.info(f"Component #{i+1}: {swell.get('direction', 'unknown')} @ {swell.get('period', 'unknown')}s, height: {swell.get('height', 'unknown')}ft")

    # Prepare structured data summaries for the prompt
    buoy_summary = json.dumps(buoy_data, indent=2)
    wind_summary = json.dumps(wind_data, indent=2)

    # Create a summary of Southern Hemisphere data - making sure to convert Path objects
    # Include Caldwell analysis if available
    caldwell_analysis = southern_hemisphere_data.get("caldwell_analysis")
    surfline_analysis = southern_hemisphere_data.get("surfline_region_analysis")

    southern_summary = json.dumps({
        "chart_count": len(southern_hemisphere_data["charts"]),
        "forecast_providers": [f["provider"] for f in southern_hemisphere_data["forecasts"]] if "forecasts" in southern_hemisphere_data else [],
        "historical_analogs": [a["date"] for a in southern_hemisphere_data["historical_analogs"]] if "historical_analogs" in southern_hemisphere_data else [],
        "detected_storms": southern_hemisphere_data.get("storms", []),
        "significant_swells": significant_swells,
        "multi_component_swells": multi_component_swells,  # Add the multi-component swell analysis
        "caldwell_analysis": caldwell_analysis,
        "surfline_analysis": surfline_analysis
    }, indent=2)
    
    # Create a summary of North Shore analysis - making sure to convert Path objects
    north_shore_summary = json.dumps({
        "buoy_count": len(north_shore_analysis["buoy_data"]),
        "break_forecasts": path_to_str(north_shore_analysis["break_forecasts"]),
        "storm_phases": path_to_str(north_shore_analysis["storm_phases"]),
        "historical_analogs": path_to_str(north_shore_analysis["historical_analogs"])
    }, indent=2)
    
    # Create summaries for ECMWF and BOM data
    ecmwf_summary = json.dumps(path_to_str(ecmwf_data), indent=2)
    bom_summary = json.dumps(path_to_str(bom_data), indent=2)
    model_summary = json.dumps(path_to_str(model_data), indent=2)
    
    # Check if we need to emphasize south swell
    south_swell_emphasis = cfg["FORECAST"].getboolean("south_swell_emphasis", False)
    
    # Auto-detect south swell emphasis if needed
    if not south_swell_emphasis and significant_swells:
        log.info("Auto-enabling south swell emphasis due to detected significant swells")
        south_swell_emphasis = True
    
    # Check for north swell emphasis
    north_swell_emphasis = cfg["FORECAST"].getboolean("north_swell_emphasis", False)
    
    # Auto-detect north swell emphasis if we have significant data
    if not north_swell_emphasis and len(north_shore_analysis["storm_phases"]) > 1:
        log.info("Auto-enabling north swell emphasis due to detected storm phases")
        north_swell_emphasis = True
    
    mdl = cfg["GENERAL"]["agent_model"]

    # Build prompt using templates from prompts.json if available, otherwise use defaults
    if PROMPTS and "forecast" in PROMPTS:
        # Start with the intro
        prompt = PROMPTS["forecast"]["intro"].format(timestamp=meta['timestamp'])
        prompt += "\n\n"

        # Add emphasis flags if configured or auto-detected
        if south_swell_emphasis and north_swell_emphasis:
            prompt += PROMPTS["forecast"]["emphasis"]["both"] + "\n\n"
        elif south_swell_emphasis:
            prompt += PROMPTS["forecast"]["emphasis"]["south"] + "\n\n"
        elif north_swell_emphasis:
            prompt += PROMPTS["forecast"]["emphasis"]["north"] + "\n\n"

        # Add data sources section
        prompt += PROMPTS["forecast"]["data_sources"].format(
            buoy_summary=buoy_summary,
            wind_summary=wind_summary,
            southern_summary=southern_summary,
            north_shore_summary=north_shore_summary,
            ecmwf_summary=ecmwf_summary,
            bom_summary=bom_summary,
            model_summary=model_summary
        ) + "\n\n"

        # Add structure section intro
        prompt += PROMPTS["forecast"]["structure"]["intro"] + "\n\n"

        # Add nowcast section
        prompt += PROMPTS["forecast"]["structure"]["nowcast"] + "\n\n"
    else:
        # Use hardcoded prompts as fallback
        prompt = (
            f"You are a veteran Hawaiian surf forecaster with over 30 years "
            f"of experience analyzing Pacific storm systems and delivering detailed, educational surf forecasts for Hawaii.\n\n"

            f"Use your deep expertise in swell mechanics, Pacific climatology, and historical analogs to analyze the following "
            f"marine data (surf, wind, swell) collected {meta['timestamp']} and generate a 10-day surf forecast for Oʻahu.\n\n"
        )

        # Add emphasis flags if configured or auto-detected
        if south_swell_emphasis and north_swell_emphasis:
            prompt += (
                f"IMPORTANT: Both North Pacific and South Pacific storm activity are significant right now. "
                f"Your forecast should provide detailed multi-phase analysis for both North Shore and South Shore conditions, "
                f"with comprehensive storm tracking for both hemispheres.\n\n"
            )

            # Add special section for multi-component swell analysis when both shores are active
            if multi_component_swells:
                prompt += (
                    f"MULTI-COMPONENT SOUTH SWELL ANALYSIS (Pat Caldwell style):\n"
                    f"I've detected {len(multi_component_swells)} distinct overlapping south swell components using Pat Caldwell's methodology. "
                    f"Please incorporate these components in your South Shore forecast:\n"
                )

                for idx, component in enumerate(multi_component_swells):
                    component_desc = (
                        f"- Component #{idx+1}: {component.get('direction', 'unknown')} swell @ "
                        f"{component.get('period', 'unknown')}s, height: {component.get('height', 'unknown')}ft"
                    )

                    if component.get('arrival_date'):
                        component_desc += f", arriving/peaking: {component.get('arrival_date')}"
                    if component.get('description'):
                        component_desc += f" - {component.get('description', '')}"

                    prompt += component_desc + "\n"

                prompt += (
                    f"\nYour South Shore forecast should analyze these distinct overlapping swell components similarly to "
                    f"how you handle the North Shore's multi-phase storm systems. Separate your analysis of different "
                    f"directional bands and periods, following Pat Caldwell's approach.\n\n"
                )
        elif south_swell_emphasis:
            prompt += (
                f"IMPORTANT: Currently there is significant South Pacific storm activity generating large south swells. "
                f"Your South Shore forecast section should receive extra attention and detail in this report.\n\n"
            )

            # Add special section for multi-component swell analysis like Pat Caldwell does
            if multi_component_swells:
                prompt += (
                    f"MULTI-COMPONENT SOUTH SWELL ANALYSIS (Pat Caldwell style):\n"
                    f"I've detected {len(multi_component_swells)} distinct overlapping south swell components using Pat Caldwell's methodology. "
                    f"Please incorporate these components in your South Shore forecast:\n"
                )

                for idx, component in enumerate(multi_component_swells):
                    component_desc = (
                        f"- Component #{idx+1}: {component.get('direction', 'unknown')} swell @ "
                        f"{component.get('period', 'unknown')}s, height: {component.get('height', 'unknown')}ft"
                    )

                    if component.get('arrival_date'):
                        component_desc += f", arriving/peaking: {component.get('arrival_date')}"
                    if component.get('description'):
                        component_desc += f" - {component.get('description', '')}"

                    prompt += component_desc + "\n"

                prompt += (
                    f"\nYour South Shore forecast should analyze these distinct overlapping swell components "
                    f"separately, following Pat Caldwell's approach. Explain how each component affects "
                    f"different south-facing breaks based on their distinct periods and directional bands.\n\n"
                )
        elif north_swell_emphasis:
            prompt += (
                f"IMPORTANT: Current North Pacific data shows a multi-phase storm system with distinct components. "
                f"Your North Shore forecast should break down this system into its separate phases (similar to Pat Caldwell's approach) "
                f"and track how each phase affects swell arrival timing and characteristics.\n\n"
            )

        prompt += (
            f"AVAILABLE DATA SOURCES:\n"
            f"1. NDBC Buoy Readings: {buoy_summary}\n"
            f"2. NOAA CO-OPS Wind Observations: {wind_summary}\n"
            f"3. Southern Hemisphere Data: {southern_summary}\n"
            f"4. North Shore Analysis: {north_shore_summary}\n"
            f"5. ECMWF Wave Model Data: {ecmwf_summary}\n"
            f"6. Australian BOM Data: {bom_summary}\n"
            f"7. WW3 Wave Model Data: {model_summary}\n"
            f"8. Multiple marine charts and forecast images will be provided\n"
            f"9. Windy.com API forecast data for North and South Shore\n"
            f"10. PacIOOS SWAN nearshore wave model data\n"
            f"11. Open-Meteo wave height and wind forecasts\n\n"

            f"Structure the forecast in this style:\n"
            f"1. Start with a short title and opening paragraph summarizing current surf conditions and the broader meteorological setup.\n\n"

            f"2. Include a detailed NOWCAST section that:\n"
            f"   - Uses the most recent buoy readings to describe what's happening RIGHT NOW\n"
            f"   - Tracks swell propagation (e.g., '2.8 ft @ 15s from 310° hit Buoy 51001 at 8AM, arriving at North Shore around 11AM')\n"
            f"   - Translates buoy data to actual surf heights at specific breaks (e.g., 'translating to 5-7 ft faces at exposed spots')\n"
            f"   - Notes whether swell is building, holding, or dropping based on buoy trends\n\n"
        )
    
    # Adjust North Shore vs South Shore section ordering based on emphasis
    if PROMPTS and "forecast" in PROMPTS:
        if south_swell_emphasis and not north_swell_emphasis:
            # Prioritize South Shore
            prompt += PROMPTS["forecast"]["structure"]["south_shore_priority"]

            # Add special section for multi-component swell analysis
            if multi_component_swells:
                prompt += "\n\n# MULTI-COMPONENT SOUTH SWELL ANALYSIS (Pat Caldwell style):\n"
                prompt += f"I've detected {len(multi_component_swells)} distinct overlapping south swell components using Pat Caldwell's methodology. "
                prompt += "Please incorporate these components in your South Shore forecast:\n"

                for idx, component in enumerate(multi_component_swells):
                    component_desc = (
                        f"- Component #{idx+1}: {component.get('direction', 'unknown')} swell @ "
                        f"{component.get('period', 'unknown')}s, height: {component.get('height', 'unknown')}ft"
                    )

                    if component.get('arrival_date'):
                        component_desc += f", arriving/peaking: {component.get('arrival_date')}"
                    if component.get('description'):
                        component_desc += f" - {component.get('description', '')}"

                    prompt += component_desc + "\n"

                prompt += "\nYour South Shore forecast should analyze these distinct overlapping swell components "
                prompt += "separately, following Pat Caldwell's approach. Explain how each component affects "
                prompt += "different south-facing breaks based on their distinct periods and directional bands."

        elif north_swell_emphasis and not south_swell_emphasis:
            # Prioritize North Shore
            prompt += PROMPTS["forecast"]["structure"]["north_shore_priority"]
        else:
            # Both shores are equally important
            prompt += PROMPTS["forecast"]["structure"]["balanced"]

            # Add multi-component analysis for balanced forecast
            if multi_component_swells:
                prompt += "\n\n# MULTI-COMPONENT SOUTH SWELL ANALYSIS:\n"
                prompt += f"I've detected {len(multi_component_swells)} distinct overlapping south swell components. "
                prompt += "Please incorporate these components in your South Shore forecast:\n"

                for idx, component in enumerate(multi_component_swells):
                    component_desc = (
                        f"- Component #{idx+1}: {component.get('direction', 'unknown')} swell @ "
                        f"{component.get('period', 'unknown')}s, height: {component.get('height', 'unknown')}ft"
                    )

                    if component.get('arrival_date'):
                        component_desc += f", arriving/peaking: {component.get('arrival_date')}"

                    prompt += component_desc + "\n"

                prompt += "\nFollow Pat Caldwell's approach of analyzing distinct directional bands separately."

        # Add wingfoiling section
        prompt += "\n\n" + PROMPTS["forecast"]["structure"]["wingfoiling"]

        # Add conclusion and style sections
        prompt += "\n\n" + PROMPTS["forecast"]["structure"]["conclusion"]
        prompt += "\n\n" + PROMPTS["forecast"]["structure"]["style"]
    else:
        # Fallback to hardcoded prompts
        if south_swell_emphasis and not north_swell_emphasis:
            # Prioritize South Shore
            prompt += (
                f"3. For the South Shore:\n"
                f"   - Begin with a summary of current southern hemisphere swell energy (direction in °, period in s, height in ft)\n"
                f"   - Break down each Southern Hemisphere storm system in detail, linking them to expected swell arrival\n"
                f"   - Analyze Southern Ocean storm development near New Zealand and off Antarctica\n"
                f"   - Provide swell travel time estimates based on distance (typically 5-8 days from South Pacific)\n"
                f"   - Create a detailed day-by-day forecast with expected size, direction, period, and conditions for 7–10 days\n"
                f"   - Include technical commentary on model agreement or uncertainties\n"
                f"   - Compare current patterns to past significant south swells when relevant\n\n"

                f"4. For the North Shore:\n"
                f"   - Begin with a summary of current swell energy (direction in °, period in s, height in ft) and wind conditions\n"
                f"   - Use a multi-phase approach to track NPAC storm systems like professional forecasters (similar to Pat Caldwell)\n"
                f"   - Break down each storm system into distinct phases as it moves across the Pacific\n"
                f"   - Consider buoy data trends (rising vs falling) when forecasting swell arrival and timing\n"
                f"   - Include specific break forecasts for Pipeline, Sunset, Waimea, and Haleiwa when relevant\n"
                f"   - Provide a day-by-day forecast with expected size, direction, period, and conditions for 7–10 days\n"
                f"   - Include technical commentary on model agreement or uncertainties\n\n"
            )
        elif north_swell_emphasis and not south_swell_emphasis:
            # Prioritize North Shore
            prompt += (
                f"3. For the North Shore:\n"
                f"   - Begin with a summary of current swell energy (direction in °, period in s, height in ft) and wind conditions\n"
                f"   - Use a multi-phase approach to track NPAC storm systems like professional forecasters (similar to Pat Caldwell)\n"
                f"   - Break down each storm system into distinct phases (fetch development, peak intensity, decay) as it moves across the Pacific\n"
                f"   - Consider buoy data trends (rising vs falling) when forecasting swell arrival and timing\n"
                f"   - Don't overlook smaller secondary swells from different directions (e.g., NNW vs NW)\n"
                f"   - Include specific break forecasts for Pipeline, Sunset, Waimea, and Haleiwa\n"
                f"   - Provide a day-by-day forecast with expected size, direction, period, and conditions for 7–10 days\n"
                f"   - Include technical commentary on model agreement or uncertainties\n\n"

                f"4. For the South Shore:\n"
                f"   - Begin with a summary of current southern hemisphere swell energy\n"
                f"   - Break down any relevant Southern Hemisphere swell sources\n"
                f"   - Provide a day-by-day forecast with expected size, direction, period, and conditions for 7–10 days\n"
                f"   - Include technical commentary on model agreement or uncertainties\n\n"
            )
        else:
            # Both shores are equally important
            prompt += (
                f"3. For the North Shore:\n"
                f"   - Begin with a summary of current swell energy (direction in °, period in s, height in ft) and wind conditions\n"
                f"   - Use a multi-phase approach to track NPAC storm systems like professional forecasters (similar to Pat Caldwell)\n"
                f"   - Break down each storm system into distinct phases (fetch development, peak intensity, decay) as it moves across the Pacific\n"
                f"   - Consider buoy data trends (rising vs falling) when forecasting swell arrival and timing\n"
                f"   - Include specific break forecasts for Pipeline, Sunset, Waimea, and Haleiwa\n"
                f"   - Provide a day-by-day forecast with expected size, direction, period, and conditions for 7–10 days\n"
                f"   - Include technical commentary on model agreement or uncertainties\n\n"

                f"4. For the South Shore:\n"
                f"   - Begin with a summary of current southern hemisphere swell energy\n"
                f"   - Break down each Southern Hemisphere storm system in detail, linking them to expected swell arrival\n"
                f"   - Analyze Southern Ocean storm development near New Zealand and off Antarctica\n"
                f"   - Provide swell travel time estimates based on distance (typically 5-8 days from South Pacific)\n"
                f"   - Create a detailed day-by-day forecast with expected size, direction, period, and conditions for 7–10 days\n"
                f"   - Include technical commentary on model agreement or uncertainties\n\n"
            )

        prompt += (
            f"5. WING FOILING Forecast:\n"
            f"   - Analyze wind patterns specifically for wing foiling potential (12-25 knots is ideal)\n"
            f"   - Note daily time windows with best wind (e.g., 'Tuesday 1-4PM: NE 15-18 knots, ideal for Kailua and Kahana Bay')\n"
            f"   - Include specific locations that will have appropriate wind given daily patterns\n"
            f"   - Consider sea state/chop from wind-against-swell scenarios\n\n"

            f"6. Use buoy readings to support forecast confidence (e.g., '51001 showing 14s energy from 310° at 3 ft')\n"
            f"7. Discuss local wind patterns (trades, Kona, diurnal land/sea breezes) and their timing/impact on surf quality\n"
            f"8. End with a confidence score (1–10), and a brief summary - neutral, factual, and occasionally dry-humored\n\n"

            f"Key style points:\n"
            f"- Use precise terms: 'dominant period,' 'long-period forerunners,' 'shadowing from Kauai,' etc.\n"
            f"- Use markdown formatting: section headings, bullet points, and a day-by-day forecast table per coast\n"
            f"- Include a markdown table with these columns: Date, Primary Swell (ft), Direction (°), Period (s), Wind/Conditions, Notes\n"
            f"- Use a calm, technical, and educational tone — avoid hype, be honest about uncertainties\n"
            f"- Sign the forecast at the end with 'Your Benevolent AI Overlords'\n"
        )
    
    # Add specialized sections based on configuration
    if PROMPTS and "forecast" in PROMPTS:
        # Add specialized Northern Pacific analysis tips when needed
        if north_swell_emphasis or len(north_shore_analysis["storm_phases"]) > 1:
            prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["north_pacific"]

            # Add break-specific forecasts if available
            if north_shore_analysis["break_forecasts"]:
                prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["break_specific"]

        # Add Southern Hemisphere expertise if configured
        if south_swell_emphasis or significant_swells:
            prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["southern_hemisphere"]

            # Add Pat Caldwell's analysis if available
            if caldwell_analysis:
                prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["caldwell_analysis"].format(
                    caldwell_analysis=caldwell_analysis
                )

            # Add Surfline analysis if available
            if surfline_analysis:
                prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["surfline_analysis"].format(
                    surfline_analysis=surfline_analysis[:500]
                )

        # Add specific south swell instruction if we detected significant south swells
        if significant_swells:
            swell_details = "\n".join([
                f"- South Swell #{i+1}: {swell.get('height', 'unknown')}ft @ {swell.get('period', 'unknown')}s from {swell.get('direction', 'unknown')}°"
                for i, swell in enumerate(significant_swells[:3]) if 'height' in swell
            ])

            if swell_details:
                prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["south_swell_details"].format(
                    swell_details=swell_details
                )

        # Add ECMWF and BOM specific instructions
        if len(ecmwf_data.get("hawaii", {})) > 0 or len(ecmwf_data.get("north_pacific", {})) > 0 or len(ecmwf_data.get("south_pacific", {})) > 0:
            prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["ecmwf"]

        if len(bom_data.get("forecasts", [])) > 0 or len(bom_data.get("charts", [])) > 0:
            prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["bom"]
    else:
        # Fallback to hardcoded prompts
        # Add specialized Northern Pacific analysis tips when needed
        if north_swell_emphasis or len(north_shore_analysis["storm_phases"]) > 1:
            prompt += (
                f"\nNorth Pacific Storm Analysis (Pat Caldwell style):\n"
                f"- Use a multi-phase approach like Pat Caldwell does, tracking systems as they move across the Pacific\n"
                f"- For each phase, note the location (e.g., 'off Kurils,' 'Date Line,' 'central/E Aleutians')\n"
                f"- Include specific fetch details for each phase (e.g., 'compact area of gales,' 'narrow fetch centered near 310°')\n"
                f"- Track pattern evolution with timing (e.g., 'It settled near 55N, 155-170W 5/3-5 with a wide, long fetch')\n"
                f"- When forecasting swell arrival, be specific about timing (e.g., 'slow rise Tuesday morning, filled in by PM')\n"
                f"- Be optimistic but realistic about swell size, especially when data shows rising vs falling trends\n"
            )

            # Add break-specific forecasts if available
            if north_shore_analysis["break_forecasts"]:
                prompt += (
                    f"\nNorth Shore Break-Specific Forecasts:\n"
                    f"- Include break-specific forecasts for Pipeline, Sunset, Waimea and other key spots\n"
                    f"- Note how different swell directions and periods affect each spot differently\n"
                    f"- Mention shadowing from Kauai and refraction around Kaena Point when relevant\n"
                    f"- Provide quality assessments for each major break (e.g., 'Pipeline: Good, 5-7 ft, best on incoming tide')\n"
                )

        # Add Southern Hemisphere expertise if configured
        if south_swell_emphasis or significant_swells:
            prompt += (
                f"\nSouthern Hemisphere Storm Analysis:\n"
                f"- Draw on your expertise of South Pacific storm systems near New Zealand, Australia, and Antarctica\n"
                f"- Note that Southern Hemisphere storms are often more consistent and organized than North Pacific systems\n"
                f"- South swells typically have longer periods (14-20s) and longer travel distances (5,000+ km)\n"
                f"- Consider seasonal patterns - Southern Hemisphere winter (June-September) produces larger south swells\n"
                f"- Look for consistency in multiple Southern Hemisphere data sources to gauge confidence\n"
                f"- Use BOM data to gain insights into storms near Australia and New Zealand\n"
            )

            # Add Pat Caldwell's analysis if available
            if caldwell_analysis:
                prompt += (
                    f"\nSpecial South Pacific Analysis from Pat Caldwell (NOAA):\n"
                    f"The following is an excerpt from Pat Caldwell's official NOAA surf forecast regarding the Southern Hemisphere:\n"
                    f"\"{caldwell_analysis}\"\n"
                    f"- Pat Caldwell is considered the authority on Hawaiian surf forecasting\n"
                    f"- Use his analysis to enhance your South Shore forecast section\n"
                    f"- Pay special attention to his observations about storm systems and timing\n"
                )

            # Add Surfline analysis if available
            if surfline_analysis:
                prompt += (
                    f"\nSurfline South Pacific Regional Analysis:\n"
                    f"The following is an excerpt from Surfline's South Pacific regional forecast:\n"
                    f"\"{surfline_analysis[:500]}...\"\n"
                    f"- Consider this commercial forecast but prioritize your own analysis\n"
                )

        # Add specific south swell instruction if we detected significant south swells
        if significant_swells:
            swell_details = "\n".join([
                f"- South Swell #{i+1}: {swell.get('height', 'unknown')}ft @ {swell.get('period', 'unknown')}s from {swell.get('direction', 'unknown')}°"
                for i, swell in enumerate(significant_swells[:3]) if 'height' in swell
            ])

            if swell_details:
                prompt += (
                    f"\nSpecific South Swell Information:\n"
                    f"Our automated analysis has detected significant south swells:\n"
                    f"{swell_details}\n"
                    f"Please incorporate this data in your South Shore forecast for accuracy.\n"
                )

        # Add ECMWF and BOM specific instructions
        if len(ecmwf_data.get("hawaii", {})) > 0 or len(ecmwf_data.get("north_pacific", {})) > 0 or len(ecmwf_data.get("south_pacific", {})) > 0:
            prompt += (
                f"\nECMWF Wave Model Analysis:\n"
                f"- ECMWF wave models are considered the most accurate in the world\n"
                f"- Pay special attention to their forecasts, particularly for significant wave height and direction\n"
                f"- Use ECMWF data to refine your forecast confidence and accuracy\n"
                f"- Note any discrepancies between ECMWF forecasts and other models\n"
            )

        if len(bom_data.get("forecasts", [])) > 0 or len(bom_data.get("charts", [])) > 0:
            prompt += (
                f"\nAustralian BOM Data Analysis:\n"
                f"- Australian BOM provides excellent coverage of Southern Ocean storm development\n"
                f"- Use their marine forecasts to identify potential south swell sources\n"
                f"- Pay attention to storm intensity and fetch location from BOM charts\n"
                f"- Consider how these Southern Hemisphere systems will translate to Hawaii surf conditions\n"
            )
    
    content=[{"type":"text","text":prompt}]
    for r in imgs:
        fmt, b64 = prepare(r["path"])
        content.append({"type":"image_url",
                        "image_url":{"url":f"data:image/{fmt};base64,{b64}"}})
    
    try:
        res = client.chat.completions.create(
            model=mdl,
            messages=[{"role":"user","content":content}],
            max_tokens=int(cfg["GENERAL"]["max_tokens"]),
            temperature=float(cfg["GENERAL"]["temperature"]))
        
        forecast_text = res.choices[0].message.content
        return forecast_text
    except Exception as e:
        log.error(f"Error generating forecast: {e}")
        return f"# Error Generating Forecast\n\nThere was an error connecting to the OpenAI API: {e}\n\nPlease try again later."

def extract_table(forecast_text, shore_type):
    """
    Extract the markdown table for a specific shore from the forecast text.

    Args:
        forecast_text (str): The complete forecast text in markdown format
        shore_type (str): The shore type to look for (e.g., "North", "South")

    Returns:
        str: The extracted table as a string, or None if no table found
    """
    lines = forecast_text.split('\n')
    table_lines = []
    capture = False

    # Try different heading formats (case insensitive)
    heading_patterns = [
        f"{shore_type} Shore",
        f"{shore_type.upper()} SHORE",
        f"{shore_type} Shore Day-by-Day Forecast Table",
        f"{shore_type.upper()} SHORE DAY-BY-DAY FORECAST"
    ]

    # Look for the table header with shore_type
    for i, line in enumerate(lines):
        # Check if this line contains any of our heading patterns
        if any(pattern in line for pattern in heading_patterns) and i < len(lines) - 1:
            # Skip this header line and look for the actual table
            for j in range(i+1, min(i+20, len(lines))):  # Look up to 20 lines ahead
                if '|' in lines[j]:  # Found the start of a table
                    # Start capturing from the table header row
                    capture = True
                    table_start = j
                    break

            if capture:
                # Found a table, start capturing from the header row
                table_lines = [lines[table_start]]
                for k in range(table_start+1, len(lines)):
                    if lines[k].strip() and '|' in lines[k]:
                        table_lines.append(lines[k])
                    elif len(table_lines) > 2 and not ('|' in lines[k]):
                        # We've reached the end of the table if we have header + separator + data rows
                        # and this line doesn't contain pipe character
                        break
                break  # Found our table, exit the outer loop

    if table_lines:
        return '\n'.join(table_lines)

    # Fallback table extraction if we don't find the heading but there's a table
    # This looks for tables with Date and the shore type in them
    for i, line in enumerate(lines):
        if '|' in line and 'Date' in line and shore_type in line:
            table_lines = [line]
            for j in range(i+1, len(lines)):
                if lines[j].strip() and '|' in lines[j]:
                    table_lines.append(lines[j])
                elif len(table_lines) > 2 and not ('|' in lines[j]):
                    break
            break

    if table_lines:
        return '\n'.join(table_lines)

    return None

def generate_forecast_chart(forecast_text, shore_type, out_dir):
    """Generate a forecast chart image using OpenAI's gpt-image-1 model"""
    try:
        # Extract API key from config
        cfg = configparser.ConfigParser()
        cfg.read("config.ini")
        api_key = cfg["API"]["OPENAI_KEY"]

        if not api_key:
            log.warning("OPENAI_API_KEY not found in config")
            return None, None

        # Format the prompt for the image generation using template if available
        if PROMPTS and "chart_generation" in PROMPTS and "surf_chart" in PROMPTS["chart_generation"]:
            prompt = PROMPTS["chart_generation"]["surf_chart"].format(forecast_text=forecast_text)
        else:
            # Fallback to hardcoded prompt
            prompt = f"Create a clear, professional surf forecast chart showing the following data as a table. Format with columns for Date, Primary Swell (ft), Direction (°), Period (s), Wind/Conditions, and Notes. Make text large and readable.\n\n{forecast_text}"

        # Headers for API request
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        # Get image generation settings from config
        image_model = cfg["GENERAL"].get("image_model", "gpt-image-1")
        image_size = cfg["FORECAST"].get("chart_image_size", "1024x1024")
        image_quality = cfg["FORECAST"].get("chart_image_quality", "standard")

        # Validate size parameter for gpt-image-1
        valid_sizes = ["1024x1024", "1024x1536", "1536x1024", "auto"]
        if image_size not in valid_sizes:
            log.warning(f"Invalid image size: {image_size}, defaulting to 1024x1024")
            image_size = "1024x1024"

        # Create payload based on model
        json_payload = {
            "model": image_model,
            "prompt": prompt,
            "n": 1,
            "size": image_size
        }

        # gpt-image-1 doesn't use the quality parameter, so we don't need to add it

        # Make request with longer timeout for image generation
        timeout = httpx.Timeout(120.0)

        with httpx.Client(timeout=timeout) as client:
            response = client.post(
                "https://api.openai.com/v1/images/generations",
                headers=headers,
                json=json_payload
            )

        if response.status_code != 200:
            log.error(f"Error generating image: {response.text}")
            return None, None

        response_json = response.json()
        log.debug(f"Image API response: {response_json}")

        # Extract data from the response
        data = response_json.get("data", [])
        if not data:
            log.error("No data in response")
            return None, None

        # Log the structure to debug
        if data:
            log.info(f"Response keys: {list(data[0].keys())}")

        # gpt-image-1 can return either URL or base64
        img_data = None
        if "url" in data[0]:
            # Get image URL
            image_url = data[0]["url"]
            log.info(f"Image URL received, downloading...")

            # Download the image from the URL
            img_response = httpx.get(image_url)
            if img_response.status_code != 200:
                log.error(f"Failed to download image from URL: {image_url}")
                return None, None

            img_data = img_response.content
        elif "b64_json" in data[0]:
            # Get base64 encoded image
            log.info(f"Base64 image received, decoding...")
            img_data = base64.b64decode(data[0]["b64_json"])
        else:
            log.error(f"No image URL or base64 in response: {data[0]}")
            return None, None
        
        # Save the image
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        image_filename = f"forecast_chart_{shore_type.lower().replace(' ', '_')}_{timestamp}.png"
        image_path = out_dir / "images" / image_filename
        
        # Ensure the directory exists
        (out_dir / "images").mkdir(exist_ok=True)
        
        with open(image_path, "wb") as f:
            f.write(img_data)
            
        log.info(f"Forecast chart saved as: {image_path}")
        
        # Convert to base64 for HTML embedding
        with open(image_path, "rb") as f:
            b64_data = base64.b64encode(f.read()).decode()
            
        return image_filename, b64_data
        
    except Exception as e:
        log.error(f"Exception in chart generation: {e}", exc_info=True)
        return None, None

def create_html_report(forecast_text, timestamp, north_chart_b64=None, south_chart_b64=None):
    """Create HTML report from Markdown text and include base64 encoded images"""
    # Convert Markdown to HTML
    html_content = markdown.markdown(forecast_text, extensions=['tables', 'fenced_code'])
    
    # Add CSS for styling
    css = """
    <style>
        body { 
            font-family: 'Helvetica Neue', Arial, sans-serif; 
            line-height: 1.6; 
            color: #333; 
            max-width: 1000px; 
            margin: 0 auto; 
            padding: 20px;
        }
        table { 
            border-collapse: collapse; 
            width: 100%; 
            margin: 20px 0; 
        }
        th, td { 
            border: 1px solid #ddd; 
            padding: 8px; 
            text-align: left; 
        }
        th { 
            background-color: #f2f2f2; 
        }
        tr:nth-child(even) { 
            background-color: #f9f9f9; 
        }
        h1, h2, h3 { color: #205493; }
        .forecast-image { 
            max-width: 100%; 
            margin: 20px 0; 
            border: 1px solid #ddd; 
        }
        .header {
            border-bottom: 2px solid #205493;
            margin-bottom: 20px;
            padding-bottom: 10px;
        }
        .footer {
            margin-top: 40px;
            padding-top: 10px;
            border-top: 1px solid #ddd;
            font-size: 0.8em;
            color: #666;
        }
    </style>
    """
    
    # Create the header
    header = f"""
    <div class="header">
        <h1>Hawaii Surf Forecast</h1>
        <p>Generated {utils.utcnow()}</p>
    </div>
    """
    
    # Insert chart images if available
    if north_chart_b64:
        north_chart_html = f"""
        <div id="north-shore-chart">
            <h2>North Shore Forecast Chart</h2>
            <img src="data:image/png;base64,{north_chart_b64}" alt="North Shore Forecast Chart" class="forecast-image">
        </div>
        """
        # Insert after North Shore section header but before the next heading
        north_idx = html_content.find("<h2>NORTH SHORE")
        if north_idx == -1:  # Try alternative heading formats
            north_idx = html_content.find("<h2>North Shore")

        if north_idx != -1:
            next_h2 = html_content.find("<h2>", north_idx + 1)
            if next_h2 != -1:
                # Find the first paragraph or list after the heading to insert after that
                first_para = html_content.find("<p>", north_idx, next_h2)
                first_list = html_content.find("<ul>", north_idx, next_h2)
                insert_pos = next_h2

                if first_para != -1 and (first_list == -1 or first_para < first_list):
                    # Find end of paragraph
                    para_end = html_content.find("</p>", first_para)
                    if para_end != -1:
                        insert_pos = para_end + 4  # Length of </p>
                elif first_list != -1:
                    # Find end of list
                    list_end = html_content.find("</ul>", first_list)
                    if list_end != -1:
                        insert_pos = list_end + 5  # Length of </ul>

                html_content = html_content[:insert_pos] + north_chart_html + html_content[insert_pos:]
            else:
                html_content += north_chart_html

    if south_chart_b64:
        south_chart_html = f"""
        <div id="south-shore-chart">
            <h2>South Shore Forecast Chart</h2>
            <img src="data:image/png;base64,{south_chart_b64}" alt="South Shore Forecast Chart" class="forecast-image">
        </div>
        """
        # Insert after South Shore section
        south_idx = html_content.find("<h2>SOUTH SHORE")
        if south_idx == -1:  # Try alternative heading formats
            south_idx = html_content.find("<h2>South Shore")

        if south_idx != -1:
            next_h2 = html_content.find("<h2>", south_idx + 1)
            if next_h2 != -1:
                # Find the first paragraph or list after the heading to insert after that
                first_para = html_content.find("<p>", south_idx, next_h2)
                first_list = html_content.find("<ul>", south_idx, next_h2)
                insert_pos = next_h2

                if first_para != -1 and (first_list == -1 or first_para < first_list):
                    # Find end of paragraph
                    para_end = html_content.find("</p>", first_para)
                    if para_end != -1:
                        insert_pos = para_end + 4  # Length of </p>
                elif first_list != -1:
                    # Find end of list
                    list_end = html_content.find("</ul>", first_list)
                    if list_end != -1:
                        insert_pos = list_end + 5  # Length of </ul>

                html_content = html_content[:insert_pos] + south_chart_html + html_content[insert_pos:]
            else:
                html_content += south_chart_html
    
    # Create the footer
    footer = f"""
    <div class="footer">
        <p>Generated by Lord GPT-4.1 | {timestamp}</p>
    </div>
    """
    
    # Combine all parts
    full_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Hawaii Surf Forecast - {timestamp}</title>
        {css}
    </head>
    <body>
        {header}
        {html_content}
        {footer}
    </body>
    </html>
    """
    
    return full_html

def main():
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--config",default="config.ini")
    p.add_argument("--bundle-id")
    p.add_argument("--max-img",type=int,default=12)  # Increased from 10 to 12
    p.add_argument("--south-swell-emphasis", action="store_true", 
                   help="Emphasize South Shore forecast in the output")
    p.add_argument("--north-swell-emphasis", action="store_true", 
                   help="Emphasize North Shore forecast in the output")
    args=p.parse_args()
    cfg=configparser.ConfigParser(); cfg.read(args.config)
    data_dir = Path(cfg["GENERAL"]["data_dir"])
    meta, bundle_dir = load_bundle(data_dir,args.bundle_id)
    
    # Auto-detect significant south swells
    significant_swells = extract_significant_south_swells(meta, bundle_dir)
    
    # Get North Shore analysis
    north_shore_analysis = north_pacific_analysis.get_north_shore_analysis(meta, bundle_dir)

    # Handle "auto" settings properly
    south_emphasis = cfg["FORECAST"].get("south_swell_emphasis", "").lower()
    if args.south_swell_emphasis or significant_swells:
        south_swell_emphasis = True
    elif south_emphasis == "auto":
        south_swell_emphasis = bool(significant_swells)
    else:
        south_swell_emphasis = south_emphasis == "true"
    
    # Update config with resolved value
    if "FORECAST" not in cfg:
        cfg.add_section("FORECAST")
    cfg["FORECAST"]["south_swell_emphasis"] = str(south_swell_emphasis).lower()
    
    # Handle North Shore emphasis similarly
    north_emphasis = cfg["FORECAST"].get("north_swell_emphasis", "").lower()
    if args.north_swell_emphasis or (north_shore_analysis and len(north_shore_analysis["storm_phases"]) > 1):
        north_swell_emphasis = True
    elif north_emphasis == "auto":
        north_swell_emphasis = bool(north_shore_analysis and len(north_shore_analysis["storm_phases"]) > 1)
    else:
        north_swell_emphasis = north_emphasis == "true"
    
    # Update config with resolved value
    cfg["FORECAST"]["north_swell_emphasis"] = str(north_swell_emphasis).lower()
    
    # Select images
    imgs=select(meta, args.max_img, bundle_dir)
    
    # Generate the forecast text
    text=forecast(cfg, meta, bundle_dir, imgs)
    
    # Create output directory and images subdirectory
    out_dir = Path(cfg["FORECAST"].get("output_dir", "forecasts"))
    out_dir.mkdir(exist_ok=True)
    image_dir = out_dir / "images"
    image_dir.mkdir(exist_ok=True)
    
    # Timestamp for file naming
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    
    # Generate chart images if specified in config
    include_charts = cfg["FORECAST"].getboolean("include_charts", False)
    north_chart_file = None
    south_chart_file = None
    north_chart_b64 = None
    south_chart_b64 = None
    
    if include_charts:
        try:
            # Extract tables and generate charts
            north_table = extract_table(text, "North")
            south_table = extract_table(text, "South")

            if north_table:
                log.info("North Shore table extracted, generating chart...")
                north_chart_file, north_chart_b64 = generate_forecast_chart(north_table, "North", out_dir)
                if not north_chart_file:
                    log.warning("Failed to generate North Shore chart")
            else:
                log.warning("Could not extract North Shore table from forecast text")

            if south_table:
                log.info("South Shore table extracted, generating chart...")
                south_chart_file, south_chart_b64 = generate_forecast_chart(south_table, "South", out_dir)
                if not south_chart_file:
                    log.warning("Failed to generate South Shore chart")
            else:
                log.warning("Could not extract South Shore table from forecast text")
        except Exception as e:
            log.error(f"Error in chart generation process: {e}", exc_info=True)
            # Continue with the process even if chart generation fails
    
    # Save the Markdown version
    md_path = out_dir / f"forecast_{timestamp}.md"
    md_content = f"# Hawaii Surf Forecast\n\nGenerated {utils.utcnow()}\n\n{text}"
    
    # Add image references to markdown if generated
    if include_charts and (north_chart_file or south_chart_file):
        lines = md_content.split('\n')
        modified_lines = []

        i = 0
        while i < len(lines):
            line = lines[i]
            modified_lines.append(line)

            # Insert North Shore chart after North Shore heading and intro content
            if north_chart_file and (line.startswith("# North Shore") or line.startswith("## NORTH SHORE")):
                # Find the end of the section intro (before a table or next heading)
                section_end = i + 1
                while section_end < len(lines):
                    if (lines[section_end].startswith('#') and lines[section_end] != line) or \
                       lines[section_end].startswith('|') or \
                       lines[section_end].startswith('---'):
                        break
                    section_end += 1

                # Check if image is already there
                image_already_exists = False
                for j in range(i+1, section_end):
                    if "![" in lines[j] and "North Shore" in lines[j]:
                        image_already_exists = True
                        break

                if not image_already_exists:
                    # Insert after introduction paragraphs but before table
                    modified_lines.append(f"\n![North Shore Forecast Chart](images/{north_chart_file})\n")

            # Insert South Shore chart after South Shore heading and intro content
            if south_chart_file and (line.startswith("# South Shore") or line.startswith("## SOUTH SHORE")):
                # Find the end of the section intro (before a table or next heading)
                section_end = i + 1
                while section_end < len(lines):
                    if (lines[section_end].startswith('#') and lines[section_end] != line) or \
                       lines[section_end].startswith('|') or \
                       lines[section_end].startswith('---'):
                        break
                    section_end += 1

                # Check if image is already there
                image_already_exists = False
                for j in range(i+1, section_end):
                    if "![" in lines[j] and "South Shore" in lines[j]:
                        image_already_exists = True
                        break

                if not image_already_exists:
                    # Insert after introduction paragraphs but before table
                    modified_lines.append(f"\n![South Shore Forecast Chart](images/{south_chart_file})\n")

            i += 1

        md_content = '\n'.join(modified_lines)
    
    # Write markdown file
    md_path.write_text(md_content)
    log.info(f"Markdown forecast saved -> {md_path}")
    
    # Create and save HTML version
    html_content = create_html_report(text, timestamp, north_chart_b64, south_chart_b64)
    html_path = out_dir / f"forecast_{timestamp}.html"
    html_path.write_text(html_content)
    log.info(f"HTML forecast saved -> {html_path}")
    
    # Generate PDF from HTML
    try:
        from weasyprint import HTML, CSS
        
        pdf_path = out_dir / f"forecast_{timestamp}.pdf"
        HTML(string=html_content).write_pdf(pdf_path)
        log.info(f"PDF forecast saved -> {pdf_path}")
    except Exception as e:
        log.error(f"Error generating PDF: {e}")
    
    print(f"Forecasts generated: {timestamp}")
    print(f"  Markdown: {md_path}")
    print(f"  HTML:     {html_path}")
    print(f"  PDF:      {pdf_path}")

if __name__=="__main__":
    main()