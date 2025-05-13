#!/usr/bin/env python3
# agents/region_agents.py - Regional data collection agents
from __future__ import annotations
import logging
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
import utils

log = logging.getLogger("region_agents")

async def southern_hemisphere(ctx, session):
    """Track Southern Hemisphere storm systems for south swell forecasting"""
    out = []

    # Southern Ocean and South Pacific data sources - ENHANCED for better detection
    south_sources = [
        # Australian Bureau of Meteorology - Southern Ocean Analysis
        ("https://tgftp.nws.noaa.gov/fax/PYFE10.gif", "noaa_south_pacific_surface.gif", "surface_analysis"),
        ("https://tgftp.nws.noaa.gov/fax/PWFA11.gif", "noaa_south_pacific_wave_24h.gif", "wave_24h"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE11.gif", "noaa_south_pacific_wave_48h.gif", "wave_48h"),

        # South Pacific Wave Height Analysis - critical for south swell detection
        ("https://tgftp.nws.noaa.gov/fax/PJFA90.gif", "south_pacific_wave_analysis.gif", "wave_analysis"),

        # NOAA South Pacific Streamlines and SST data - updated URL
        ("https://www.ospo.noaa.gov/data/sst/contour/global.c.gif", "south_pacific_sst.jpg", "sst"),
        # Alternative SST source
        ("https://www.ospo.noaa.gov/data/sst/contour/southpac.c.gif", "south_pacific_sst_alt.jpg", "sst_alt"),

        # New Zealand MetService - excellent for tracking Southern Ocean storms
        ("https://www.metservice.com/publicData/marineSST", "nz_metservice_sst.png", "nz_sst"),
        ("https://www.metservice.com/publicData/marineSfc1", "nz_metservice_surface_1.png", "nz_surface_1"),
        ("https://www.metservice.com/publicData/marineSfc2", "nz_metservice_surface_2.png", "nz_surface_2"),
        ("https://www.metservice.com/publicData/marineSfc3", "nz_metservice_surface_3.png", "nz_surface_3"),

        # Australia BOM - essential for tracking systems near New Zealand
        ("http://www.bom.gov.au/marine/wind.shtml?unit=p0&location=wa1&tz=AEDT", "bom_australia_wind.html", "australia_wind"),
        ("http://www.bom.gov.au/australia/charts/synoptic_col.shtml", "bom_australia_charts.html", "australia_charts"),

        # Global wave models - great for tracking South Pacific swell patterns
        ("https://polar.ncep.noaa.gov/waves/WEB/gfswave.latest/plots/Global.small/Global.small.whs_withsnow.jpeg", "global_wave_heights.jpg", "global_waves"),

        # Satellite imagery - for visually tracking South Pacific storms
        ("https://www.goes.noaa.gov/dml/south/nhem/ssa/vis.jpg", "south_pacific_vis.jpg", "satellite_vis"),
        ("https://www.oceanweather.com/data/SPAC-WAM/WAVE.GIF", "oceanweather_south_pacific.gif", "oceanweather"),

        # Wave period forecasts - critical for south swell quality
        ("https://www.surf-forecast.com/maps/New-Zealand/six_day/wave-energy-period", "nz_wave_period.jpg", "wave_period"),
        ("https://www.surf-forecast.com/maps/New-Zealand/six_day/swell-height", "nz_swell_height.jpg", "swell_height"),

        # Additional Southern Ocean Storm Tracks - Nullschool visualization
        ("https://earth.nullschool.net/#current/ocean/primary/waves/overlay=primary_waves/orthographic=-180,-30,293", "nullschool_southern_ocean.png", "nullschool"),
    ]

    # Fetch standard Southern Hemisphere charts
    for url, filename, subtype in south_sources:
        try:
            data = await ctx.fetch(session, url)
            if data:
                fn = ctx.save(filename, data)
                out.append({
                    "source": "SouthernHemisphere",
                    "type": "chart",
                    "subtype": subtype,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                })
                log.info(f"Successfully fetched Southern Hemisphere data: {subtype}")
        except Exception as e:
            log.warning(f"Failed to fetch Southern Hemisphere data from {url}: {e}")

    # Surfline South Pacific Swell tracking - critical for south swell detection
    surfline_url = "https://services.surfline.com/kbyg/regions/south-pacific?subregionId=58581a836630e24c44878fd3"
    try:
        data = await ctx.fetch(session, surfline_url)
        if data:
            fn = ctx.save("surfline_south_pacific.json", data)
            out.append({
                "source": "SouthernHemisphere",
                "type": "forecast",
                "provider": "surfline",
                "filename": fn,
                "url": surfline_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch Surfline data: {e}")

    # Surfline individual spots for South Shore (Ala Moana)
    spot_url = "https://services.surfline.com/kbyg/spots/forecasts?spotId=5842041f4e65fad6a7708890&days=16"
    try:
        data = await ctx.fetch(session, spot_url)
        if data:
            fn = ctx.save("surfline_ala_moana.json", data)
            out.append({
                "source": "SouthernHemisphere",
                "type": "forecast",
                "provider": "surfline_spot",
                "spot_name": "ala_moana",
                "filename": fn,
                "url": spot_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch Surfline spot data: {e}")

    # Surf News Network - text forecast for South Shore
    snn_url = "https://www.surfnewsnetwork.com/forecast/"
    try:
        data = await ctx.fetch(session, snn_url)
        if data:
            fn = ctx.save("snn_forecast.html", data)
            out.append({
                "source": "SouthernHemisphere",
                "type": "text_forecast",
                "provider": "snn",
                "filename": fn,
                "url": snn_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch SNN forecast: {e}")

    # MOST IMPORTANT: Pat Caldwell's forecast which includes detailed South Pacific analysis
    # This is critical as Pat Caldwell has the best insights about Southern Hemisphere swells
    caldwell_urls = [
        ("https://www.weather.gov/hfo/SurfDiscussion", "caldwell_forecast.html", "discussion"),
        ("https://www.weather.gov/hfo/SRF", "caldwell_srf.html", "srf")  # SRF page often has more detailed analysis
    ]

    for url, filename, subtype in caldwell_urls:
        try:
            data = await ctx.fetch(session, url)
            if data:
                fn = ctx.save(filename, data)
                out.append({
                    "source": "SouthernHemisphere",
                    "type": "text_forecast",
                    "subtype": subtype,
                    "provider": "caldwell",
                    "filename": fn,
                    "url": url,
                    "priority": 1,  # Highest priority for Pat Caldwell's forecasts
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                })

                # Try to extract and analyze South Shore content from Pat Caldwell
                if isinstance(data, bytes):
                    text_content = data.decode('utf-8', errors='ignore')
                else:
                    text_content = str(data)

                # Enhanced pattern matching for south swell indicators with more precise Caldwell-style extraction
                south_patterns = [
                    # Pattern for height-direction-period with precise angles
                    r"(\d+\.?\d?)(?:-(\d+\.?\d?))?\s*(?:ft|foot|feet).*?(?:SSW|S|SW|South|South-southwest|Southwest|Southerly).*?(\d+)(?:-(\d+))?\s*(?:s(?:ec)?|second)",

                    # Pattern for direction-height-period
                    r"(?:SSW|S|SW|South|South-southwest|Southwest|Southerly).*?(\d+\.?\d?)(?:-(\d+\.?\d?))?\s*(?:ft|foot|feet).*?(\d+)(?:-(\d+))?\s*(?:s(?:ec)?|second)",

                    # Pattern for Caldwell's tabular format (swell height, direction, period)
                    r"(\d+\.?\d?)\s+((?:SSW|S|SW|SSE|SE|NNW|NW|N|NNE|NE|ENE|E))\s+(\d+)\s+\d+\s+\d+",

                    # Pattern for forecast arrival statements
                    r"(?:peaking|building).*?(\d+\/\d+).*?(?:SSW|S|SW|South|South-southwest|Southwest|Southerly)",
                ]

                # Extract all detailed information about south swells
                south_swell_components = []

                for pattern in south_patterns:
                    matches = re.finditer(pattern, text_content, re.IGNORECASE)
                    for match in matches:
                        match_groups = match.groups()
                        match_text = match.group(0)
                        log.info(f"Found swell indicator in Pat Caldwell's forecast: {match_text}")

                        # Extract different information based on the pattern matched
                        if "peaking" in pattern or "building" in pattern:
                            # Arrival timing pattern
                            south_swell_components.append({
                                "arrival_day": match_groups[0] if match_groups and len(match_groups) > 0 else None,
                                "match_text": match_text,
                                "pattern_type": "arrival_timing"
                            })
                        elif pattern.startswith(r"(\d+\.?\d?)\s+((?:SSW|S|SW|SSE|SE|NNW|NW|N|NNE|NE|ENE|E))"):
                            # Caldwell's tabular format
                            direction = match_groups[1].upper() if match_groups and len(match_groups) > 1 else None
                            if direction in ["S", "SSW", "SW", "SSE", "SE"]:
                                south_swell_components.append({
                                    "height": match_groups[0] if match_groups and len(match_groups) > 0 else None,
                                    "direction": direction,
                                    "period": match_groups[2] if match_groups and len(match_groups) > 2 else None,
                                    "match_text": match_text,
                                    "pattern_type": "caldwell_table",
                                    "is_south": True
                                })
                        elif "SSW" in pattern or "S " in pattern or "SW" in pattern:
                            # Direction-height-period or height-direction-period patterns
                            component = {
                                "match_text": match_text,
                                "pattern_type": "detailed_swell"
                            }

                            # Populate the component with available data
                            if match_groups:
                                if len(match_groups) >= 1:
                                    component["height"] = match_groups[0]
                                if len(match_groups) >= 2:
                                    component["height_range_max"] = match_groups[1]
                                if len(match_groups) >= 3:
                                    component["period"] = match_groups[2]
                                if len(match_groups) >= 4:
                                    component["period_range_max"] = match_groups[3]

                            south_swell_components.append(component)

                # Look for specific fetch geography and storm statements
                fetch_patterns = [
                    r"(?:Classic|Favorable|captured|fetch)(?:[^.]*?)(?:New Zealand|NZ|Tasman)(?:[^.]*?)(?:low|storm|gale)",
                    r"(?:Low|Storm|Gale)(?:[^.]*?)(?:east|west|south|north)(?:[^.]*?)(?:New Zealand|NZ|Tasmania|Australia)",
                    r"(?:ASCAT|JASON)(?:[^.]*?)(?:validated|confirmed|showed)(?:[^.]*?)(?:winds|seas)(?:[^.]*?)(\d+)(?:\'|\-foot|ft)"
                ]

                for pattern in fetch_patterns:
                    fetch_matches = re.finditer(pattern, text_content, re.IGNORECASE)
                    for match in fetch_matches:
                        match_text = match.group(0)
                        match_groups = match.groups()

                        # Extract sea height if available
                        sea_height = None
                        if match_groups and len(match_groups) > 0 and match_groups[0]:
                            try:
                                sea_height = float(match_groups[0])
                            except ValueError:
                                pass

                        south_swell_components.append({
                            "fetch_description": match_text,
                            "sea_height": sea_height,
                            "pattern_type": "fetch_geography"
                        })

                # Look for specific directional information in degrees
                direction_patterns = [
                    r"(\d+)(?:-(\d+))?\s*degrees?",
                    r"from\s+(\d+)(?:-(\d+))?\s*degrees?"
                ]

                for pattern in direction_patterns:
                    dir_matches = re.finditer(pattern, text_content, re.IGNORECASE)
                    for match in dir_matches:
                        match_groups = match.groups()
                        match_text = match.group(0)

                        # Check if this is in a south swell context
                        prev_context = text_content[max(0, match.start() - 100):match.start()]
                        if any(term in prev_context.lower() for term in ["south", "ssw", "sw", "s swell"]):
                            south_swell_components.append({
                                "direction_degrees_min": match_groups[0] if match_groups and len(match_groups) > 0 else None,
                                "direction_degrees_max": match_groups[1] if match_groups and len(match_groups) > 1 else None,
                                "match_text": match_text,
                                "pattern_type": "precise_direction"
                            })

                # After collecting all components, add comprehensive data
                if south_swell_components:
                    # Create a master record with all detailed components
                    out.append({
                        "source": "PatCaldwell",
                        "type": "detected_swell",
                        "subtype": "detailed_swell_analysis",
                        "swell_components": south_swell_components,
                        "filename": fn,
                        "priority": 1,
                        "timestamp": utils.utcnow(),
                        "south_facing": True,
                        "component_count": len(south_swell_components)
                    })
        except Exception as e:
            log.warning(f"Failed to fetch Pat Caldwell forecast from {url}: {e}")

    # Add Surfline South Pacific region forecast
    surfline_south_pacific_url = "https://www.surfline.com/surf-forecasts/south-pacific/3679"
    try:
        data = await ctx.fetch(session, surfline_south_pacific_url)
        if data:
            fn = ctx.save("surfline_south_pacific_forecast.html", data)
            out.append({
                "source": "SouthernHemisphere",
                "type": "text_forecast",
                "provider": "surfline_region",
                "filename": fn,
                "url": surfline_south_pacific_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch Surfline South Pacific forecast: {e}")

    # Add MagicSeaweed South Pacific forecast
    magicseaweed_url = "https://magicseaweed.com/Southern-Pacific-Ocean-Surf-Forecast/10/"
    try:
        data = await ctx.fetch(session, magicseaweed_url)
        if data:
            fn = ctx.save("magicseaweed_south_pacific.html", data)
            out.append({
                "source": "SouthernHemisphere",
                "type": "text_forecast",
                "provider": "magicseaweed",
                "filename": fn,
                "url": magicseaweed_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch MagicSeaweed South Pacific forecast: {e}")

    # Historical South Pacific storm analogs for current date period
    # This helps identify seasonal patterns by comparing with past years
    current_month = datetime.now().month
    current_day = datetime.now().day

    # Calculate dates for previous years in same season
    date_analogs = []
    for year in range(2023, 2025):  # Look at last 2 years
        analog_date = datetime(year, current_month, current_day).strftime("%Y-%m-%d")
        date_analogs.append(analog_date)

        # Also check +/- 7 days for better coverage
        for offset in [-7, 7]:
            offset_date = (datetime(year, current_month, current_day) + timedelta(days=offset)).strftime("%Y-%m-%d")
            date_analogs.append(offset_date)

    # Save historical analog information
    historical_data = {
        "current_date": datetime.now().strftime("%Y-%m-%d"),
        "analogs": date_analogs,
        "notes": "Historical analogs for South Pacific storm patterns"
    }

    fn = ctx.save("southern_hemisphere_analogs.json", json.dumps(historical_data))
    out.append({
        "source": "SouthernHemisphere",
        "type": "historical_analogs",
        "filename": fn,
        "priority": 3,
        "timestamp": utils.utcnow(),
        "south_facing": True
    })

    return out

async def north_pacific_enhanced(ctx, session):
    """Enhanced North Pacific data collection for detailed storm tracking"""
    out = []
    
    # FNMOC wave model charts
    fnmoc_urls = [
        ("https://www.fnmoc.navy.mil/wxmap_cgi/cgi-bin/wxmap_single.cgi?area=npac_swh&dtg=current&type=gift", "fnmoc_npac_wave_height.gif"),
        ("https://www.fnmoc.navy.mil/wxmap_cgi/cgi-bin/wxmap_single.cgi?area=npac_wind&dtg=current&type=gift", "fnmoc_npac_wind.gif"),
        ("https://www.fnmoc.navy.mil/wxmap_cgi/cgi-bin/wxmap_single.cgi?area=npac_mslp&dtg=current&type=gift", "fnmoc_npac_pressure.gif"),
    ]
    
    for url, filename in fnmoc_urls:
        data = await ctx.fetch(session, url)
        if data:
            fn = ctx.save(filename, data)
            out.append({
                "source": "NorthPacific",
                "type": "chart",
                "subtype": Path(filename).stem,
                "filename": fn,
                "url": url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
    
    # NOAA OPC North Pacific Sea State Analysis
    opc_urls = [
        "https://ocean.weather.gov/grids/images/neast_latest.gif",
        "https://ocean.weather.gov/grids/images/neast_024.gif",
        "https://ocean.weather.gov/grids/images/neast_048.gif",
        "https://ocean.weather.gov/grids/images/neast_072.gif"
    ]
    
    for idx, url in enumerate(opc_urls):
        hours = ["latest", "24h", "48h", "72h"][idx]
        data = await ctx.fetch(session, url)
        if data:
            fn = ctx.save(f"opc_npac_seastate_{hours}.gif", data)
            out.append({
                "source": "NorthPacific",
                "type": "seastate",
                "subtype": hours,
                "filename": fn,
                "url": url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
    
    # Historical analog data (fetch from local database or external source)
    # This would reference actual data in production
    historical_data = {
        "analogs": [
            {"date": "2020-12-02", "storm_location": "Off Kurils", "fetch_direction": 310},
            {"date": "2019-01-15", "storm_location": "Date Line", "fetch_direction": 320}
        ]
    }
    
    if historical_data:
        fn = ctx.save("historical_analogs.json", json.dumps(historical_data))
        out.append({
            "source": "NorthPacific",
            "type": "historical",
            "filename": fn,
            "priority": 3,
            "timestamp": utils.utcnow(),
            "north_facing": True
        })
    
    return out