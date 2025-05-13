#!/usr/bin/env python3
# agents/model_agents.py - Wave model data collection agents
from __future__ import annotations
import logging
import json
from pathlib import Path
import utils

log = logging.getLogger("model_agents")

async def pacioos(ctx, session):
    """Fetch PacIOOS WW3 Hawaii data"""
    # Updated variable name based on current PacIOOS API documentation
    # For ww3_hawaii dataset, the correct variable name is 'Thgt'
    VAR = "Thgt"
    base = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_hawaii.png"

    # Fix the depth parameter issue - use default depth (0.0) instead of using 18-25 range
    bbox = "[(0.0)][(18.0):(25.0)][(-162.0):(-151.0)]"

    urls = {
        "now": f"{base}?{VAR}[(last)]{bbox}&.draw=surface&.vars=longitude|latitude|{VAR}",
        "24h": f"{base}?{VAR}[(last-24)]{bbox}&.draw=surface&.vars=longitude|latitude|{VAR}",
        "48h": f"{base}?{VAR}[(last-48)]{bbox}&.draw=surface&.vars=longitude|latitude|{VAR}",
    }

    log.info(f"PacIOOS WW3 URL: {urls['now']}")

    # Create placeholder generator function
    def create_placeholder_image(tag):
        import numpy as np
        from PIL import Image, ImageDraw, ImageFont
        import io

        # Create a basic image with text indicating it's a placeholder
        width, height = 800, 600
        image = Image.new('RGB', (width, height), color=(240, 240, 240))
        draw = ImageDraw.Draw(image)

        # Add text
        try:
            # Try to load a font, use default if not available
            font = ImageFont.truetype("Arial", 36)
        except IOError:
            font = ImageFont.load_default()

        # Draw the placeholder text
        text = f"PacIOOS {tag} Data Unavailable"
        draw.text(
            (width/2, height/2),
            text,
            fill=(0, 0, 0),
            font=font,
            anchor="mm"
        )

        # Add timestamp
        timestamp = utils.utcnow()
        draw.text(
            (width/2, height/2 + 50),
            f"Generated: {timestamp}",
            fill=(100, 100, 100),
            font=font,
            anchor="mm"
        )

        # Save to bytes
        img_byte_array = io.BytesIO()
        image.save(img_byte_array, format='PNG')
        return img_byte_array.getvalue()

    out = []
    for tag, u in urls.items():
        try:
            log.info(f"Fetching PacIOOS {tag} from URL: {u}")
            d = await ctx.fetch(session, u)
            if d and not (b"Error:" in d and b"wasn't found in datasetID" in d):
                fn = ctx.save(f"pacioos_{tag}.png", d)
                out.append({
                    "source": "PacIOOS",
                    "type": f"wave_{tag}",
                    "filename": fn,
                    "url": u,
                    "priority": 1,
                    "timestamp": utils.utcnow()
                })
            else:
                log.error(f"Failed to fetch PacIOOS {tag}, using placeholder")
                # Generate placeholder and save it
                placeholder = create_placeholder_image(tag)
                fn = ctx.save(f"pacioos_{tag}.png", placeholder)
                out.append({
                    "source": "PacIOOS",
                    "type": f"wave_{tag}",
                    "filename": fn,
                    "url": u,
                    "priority": 2,  # Lower priority since it's a placeholder
                    "timestamp": utils.utcnow(),
                    "is_placeholder": True
                })
        except Exception as e:
            log.error(f"Exception fetching PacIOOS {tag}: {e}")
            # Generate placeholder on exception as well
            try:
                placeholder = create_placeholder_image(tag)
                fn = ctx.save(f"pacioos_{tag}.png", placeholder)
                out.append({
                    "source": "PacIOOS",
                    "type": f"wave_{tag}",
                    "filename": fn,
                    "url": u,
                    "priority": 2,  # Lower priority since it's a placeholder
                    "timestamp": utils.utcnow(),
                    "is_placeholder": True,
                    "error": str(e)
                })
            except Exception as placeholder_err:
                log.error(f"Failed to create placeholder for PacIOOS {tag}: {placeholder_err}")

    # Add high-resolution North Shore model with corrected variable name and parameters
    ns_base = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png"

    # Correct variable name for SWAN dataset (swan_oahu)
    # For swan_oahu dataset, the correct variable name is 'shgt'
    swan_var = "shgt"
    # Fixed coordinates for North Shore area
    ns_urls = {
        "ns_now": f"{ns_base}?{swan_var}[(last)][(21.5):(21.7)][(-158.2):(-158.0)]&.draw=surface&.vars=longitude|latitude|{swan_var}",
        "ns_24h": f"{ns_base}?{swan_var}[(last-24)][(21.5):(21.7)][(-158.2):(-158.0)]&.draw=surface&.vars=longitude|latitude|{swan_var}",
    }

    for tag, u in ns_urls.items():
        try:
            log.info(f"Fetching PacIOOS NS {tag} from URL: {u}")
            d = await ctx.fetch(session, u)
            if d and not (b"Error:" in d and b"wasn't found in datasetID" in d):
                fn = ctx.save(f"pacioos_{tag}.png", d)
                out.append({
                    "source": "PacIOOS",
                    "type": f"wave_{tag}",
                    "filename": fn,
                    "url": u,
                    "priority": 1,
                    "timestamp": utils.utcnow()
                })
            else:
                log.error(f"Failed to fetch PacIOOS NS {tag}, using placeholder")
                # Generate placeholder and save it
                placeholder = create_placeholder_image(f"North Shore {tag}")
                fn = ctx.save(f"pacioos_{tag}.png", placeholder)
                out.append({
                    "source": "PacIOOS",
                    "type": f"wave_{tag}",
                    "filename": fn,
                    "url": u,
                    "priority": 2,  # Lower priority since it's a placeholder
                    "timestamp": utils.utcnow(),
                    "is_placeholder": True
                })
        except Exception as e:
            log.error(f"Exception fetching PacIOOS NS {tag}: {e}")
            # Generate placeholder on exception as well
            try:
                placeholder = create_placeholder_image(f"North Shore {tag}")
                fn = ctx.save(f"pacioos_{tag}.png", placeholder)
                out.append({
                    "source": "PacIOOS",
                    "type": f"wave_{tag}",
                    "filename": fn,
                    "url": u,
                    "priority": 2,
                    "timestamp": utils.utcnow(),
                    "is_placeholder": True,
                    "error": str(e)
                })
            except Exception as placeholder_err:
                log.error(f"Failed to create placeholder for PacIOOS NS {tag}: {placeholder_err}")

    return out

async def pacioos_swan(ctx, session):
    """Fetch PacIOOS SWAN Oahu nearshore wave model data"""
    out = []

    # Create placeholder generator function
    def create_placeholder_image(tag):
        import numpy as np
        from PIL import Image, ImageDraw, ImageFont
        import io

        # Create a basic image with text indicating it's a placeholder
        width, height = 800, 600
        image = Image.new('RGB', (width, height), color=(240, 240, 240))
        draw = ImageDraw.Draw(image)

        # Add text
        try:
            # Try to load a font, use default if not available
            font = ImageFont.truetype("Arial", 36)
        except IOError:
            font = ImageFont.load_default()

        # Draw the placeholder text
        text = f"PacIOOS SWAN {tag} Data Unavailable"
        draw.text(
            (width/2, height/2),
            text,
            fill=(0, 0, 0),
            font=font,
            anchor="mm"
        )

        # Add timestamp
        timestamp = utils.utcnow()
        draw.text(
            (width/2, height/2 + 50),
            f"Generated: {timestamp}",
            fill=(100, 100, 100),
            font=font,
            anchor="mm"
        )

        # Save to bytes
        img_byte_array = io.BytesIO()
        image.save(img_byte_array, format='PNG')
        return img_byte_array.getvalue()

    # Get metadata for ww3_hawaii to confirm available variables
    try:
        ww3_info_url = "https://pae-paha.pacioos.hawaii.edu/erddap/info/ww3_hawaii/index.json"
        log.info(f"Fetching WW3 Hawaii dataset info: {ww3_info_url}")
        ww3_info = await ctx.fetch(session, ww3_info_url)
        if ww3_info:
            fn = ctx.save("ww3_hawaii_info.json", ww3_info)
            out.append({
                "source": "PacIOOS",
                "type": "ww3_info",
                "filename": fn,
                "url": ww3_info_url,
                "priority": 0,
                "timestamp": utils.utcnow()
            })
    except Exception as e:
        log.error(f"Failed to fetch WW3 Hawaii info: {e}")

    # For swan_oahu dataset, the correct variable name is 'shgt' (not significant_wave_height)
    swan_var = "shgt"

    # Visualization image using correct variable name
    # For swan_oahu dataset, the correct variable name is 'shgt'
    viz_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface"

    log.info(f"PacIOOS SWAN viz URL: {viz_url}")
    try:
        viz_data = await ctx.fetch(session, viz_url)
        if viz_data and not (b"Error:" in viz_data and b"wasn't found in datasetID" in viz_data):
            fn = ctx.save("pacioos_swan_viz.png", viz_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_viz",
                "filename": fn,
                "url": viz_url,
                "priority": 1,
                "timestamp": utils.utcnow()
            })
        else:
            log.error("Failed to fetch PacIOOS SWAN viz, using placeholder")
            # Generate placeholder and save it
            placeholder = create_placeholder_image("Visualization")
            fn = ctx.save("pacioos_swan_viz.png", placeholder)
            out.append({
                "source": "PacIOOS",
                "type": "swan_viz",
                "filename": fn,
                "url": viz_url,
                "priority": 2,  # Lower priority since it's a placeholder
                "timestamp": utils.utcnow(),
                "is_placeholder": True
            })
    except Exception as e:
        log.error(f"Exception fetching PacIOOS SWAN viz: {e}")
        # Generate placeholder on exception
        try:
            placeholder = create_placeholder_image("Visualization")
            fn = ctx.save("pacioos_swan_viz.png", placeholder)
            out.append({
                "source": "PacIOOS",
                "type": "swan_viz",
                "filename": fn,
                "url": viz_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "is_placeholder": True,
                "error": str(e)
            })
        except Exception as placeholder_err:
            log.error(f"Failed to create placeholder for PacIOOS SWAN viz: {placeholder_err}")

    # Metadata JSON - works well
    info_url = "https://pae-paha.pacioos.hawaii.edu/erddap/info/swan_oahu/index.json"
    try:
        log.info(f"Fetching PacIOOS SWAN info from URL: {info_url}")
        info_data = await ctx.fetch(session, info_url)
        if info_data:
            fn = ctx.save("pacioos_swan_info.json", info_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_info",
                "filename": fn,
                "url": info_url,
                "priority": 0,
                "timestamp": utils.utcnow()
            })
        else:
            log.error("Failed to fetch PacIOOS SWAN info: Empty response")
    except Exception as e:
        log.error(f"Exception fetching PacIOOS SWAN info: {e}")

    # South Shore specific wave models with correct variable name
    south_shore_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=|||||&.bgColor=0xffccccff&.land=under&.lat=21.25:21.30&.lon=-157.85:-157.80"
    try:
        log.info(f"Fetching PacIOOS South Shore from URL: {south_shore_url}")
        south_data = await ctx.fetch(session, south_shore_url)
        if south_data and not (b"Error:" in south_data and b"wasn't found in datasetID" in south_data):
            fn = ctx.save("pacioos_swan_south_shore.png", south_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_south_shore",
                "filename": fn,
                "url": south_shore_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
        else:
            log.error("Failed to fetch PacIOOS South Shore, using placeholder")
            # Generate placeholder and save it
            placeholder = create_placeholder_image("South Shore")
            fn = ctx.save("pacioos_swan_south_shore.png", placeholder)
            out.append({
                "source": "PacIOOS",
                "type": "swan_south_shore",
                "filename": fn,
                "url": south_shore_url,
                "priority": 2,  # Lower priority since it's a placeholder
                "timestamp": utils.utcnow(),
                "south_facing": True,
                "is_placeholder": True
            })
    except Exception as e:
        log.error(f"Exception fetching PacIOOS South Shore: {e}")
        # Generate placeholder on exception
        try:
            placeholder = create_placeholder_image("South Shore")
            fn = ctx.save("pacioos_swan_south_shore.png", placeholder)
            out.append({
                "source": "PacIOOS",
                "type": "swan_south_shore",
                "filename": fn,
                "url": south_shore_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True,
                "is_placeholder": True,
                "error": str(e)
            })
        except Exception as placeholder_err:
            log.error(f"Failed to create placeholder for PacIOOS South Shore: {placeholder_err}")

    # North Shore specific high-resolution SWAN model with correct variable name
    ns_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=|||||&.bgColor=0xffccccff&.land=under&.lat=21.55:21.75&.lon=-158.20:-158.00"
    try:
        log.info(f"Fetching PacIOOS North Shore from URL: {ns_url}")
        ns_data = await ctx.fetch(session, ns_url)
        if ns_data and not (b"Error:" in ns_data and b"wasn't found in datasetID" in ns_data):
            fn = ctx.save("pacioos_swan_north_shore.png", ns_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_north_shore",
                "filename": fn,
                "url": ns_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
        else:
            log.error("Failed to fetch PacIOOS North Shore, using placeholder")
            # Generate placeholder and save it
            placeholder = create_placeholder_image("North Shore")
            fn = ctx.save("pacioos_swan_north_shore.png", placeholder)
            out.append({
                "source": "PacIOOS",
                "type": "swan_north_shore",
                "filename": fn,
                "url": ns_url,
                "priority": 2,  # Lower priority since it's a placeholder
                "timestamp": utils.utcnow(),
                "north_facing": True,
                "is_placeholder": True
            })
    except Exception as e:
        log.error(f"Exception fetching PacIOOS North Shore: {e}")
        # Generate placeholder on exception
        try:
            placeholder = create_placeholder_image("North Shore")
            fn = ctx.save("pacioos_swan_north_shore.png", placeholder)
            out.append({
                "source": "PacIOOS",
                "type": "swan_north_shore",
                "filename": fn,
                "url": ns_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "north_facing": True,
                "is_placeholder": True,
                "error": str(e)
            })
        except Exception as placeholder_err:
            log.error(f"Failed to create placeholder for PacIOOS North Shore: {placeholder_err}")

    # Add the period and direction placeholders if requested in the original query
    for img_type in ["period", "direction"]:
        try:
            # Create the URLs for these types
            if img_type == "period":
                var = "tps"
                url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{var}&.colorBar=|||||&.bgColor=0xffccccff&.land=under"
            else:  # direction
                var = "mpd"
                url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{var}&.colorBar=|||||&.bgColor=0xffccccff&.land=under"

            log.info(f"Fetching PacIOOS SWAN {img_type} from URL: {url}")
            img_data = await ctx.fetch(session, url)

            if img_data and not (b"Error:" in img_data and b"wasn't found in datasetID" in img_data):
                fn = ctx.save(f"pacioos_swan_{img_type}.png", img_data)
                out.append({
                    "source": "PacIOOS",
                    "type": f"swan_{img_type}",
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "south_facing": True
                })
            else:
                log.error(f"Failed to fetch PacIOOS SWAN {img_type}, using placeholder")
                # Generate placeholder and save it
                placeholder = create_placeholder_image(f"Wave {img_type.capitalize()}")
                fn = ctx.save(f"pacioos_swan_{img_type}.png", placeholder)
                out.append({
                    "source": "PacIOOS",
                    "type": f"swan_{img_type}",
                    "filename": fn,
                    "url": url,
                    "priority": 2,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "south_facing": True,
                    "is_placeholder": True
                })
        except Exception as e:
            log.error(f"Exception fetching PacIOOS SWAN {img_type}: {e}")
            # Generate placeholder on exception
            try:
                placeholder = create_placeholder_image(f"Wave {img_type.capitalize()}")
                fn = ctx.save(f"pacioos_swan_{img_type}.png", placeholder)
                out.append({
                    "source": "PacIOOS",
                    "type": f"swan_{img_type}",
                    "filename": fn,
                    "url": url,
                    "priority": 2,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "south_facing": True,
                    "is_placeholder": True,
                    "error": str(e)
                })
            except Exception as placeholder_err:
                log.error(f"Failed to create placeholder for PacIOOS SWAN {img_type}: {placeholder_err}")

    return out

async def ww3_model_fallback(ctx, session):
    """
    Fallback for WW3 data if the main model agent fails.

    This function gets NCEP WW3 global overview images as a fallback
    when the main WW3 model data collection fails.

    Args:
        ctx: Context object with fetch and save methods
        session: aiohttp ClientSession object for making HTTP requests

    Returns:
        List of dictionaries with metadata about fetched files
    """
    log.info("Using WW3 model fallback sources")

    # Use the provided session with ctx.fetch and ctx.save directly
    # No need for wrappers since the signature was fixed
    
    # Try NCEP WW3 global overview image (less detailed but still useful)
    overview_url = "https://polar.ncep.noaa.gov/waves/latest_run/pac-hs.latest_run.gif"

    try:
        log.info(f"Fetching WW3 fallback overview: {overview_url}")
        data = await ctx.fetch(session, overview_url)
        if not data:
            log.warning("No data returned from WW3 overview URL")
            return []

        filename = ctx.save("ww3_pacific_overview.gif", data)

        results = [{
            "source": "WW3-Fallback",
            "type": "wave_overview",
            "filename": filename,
            "url": overview_url,
            "priority": 2,
            "timestamp": utils.utcnow()
        }]

        # Try to get higher resolution North Pacific data
        npac_url = "https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.north_pacific.latest.gif"
        log.info(f"Fetching WW3 fallback North Pacific detail: {npac_url}")
        npac_data = await ctx.fetch(session, npac_url)

        if npac_data:
            npac_filename = ctx.save("ww3_north_pacific_detail.gif", npac_data)
            results.append({
                "source": "WW3-Fallback",
                "type": "wave_npac_detail",
                "filename": npac_filename,
                "url": npac_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
        else:
            log.warning("No data returned from WW3 North Pacific detail URL")

        return results

    except Exception as e:
        log.error(f"Error in WW3 fallback: {e}")
        return []

async def ecmwf_wave(ctx, session):
    """Fetch ECMWF wave model data if API key is available"""
    api_key = ctx.cfg["API"].get("ECMWF_KEY", "").strip()
    if not api_key:
        return []
    
    out = []
    
    # Example ECMWF API call - would need to be adapted to their actual API
    url = "https://api.ecmwf.int/v1/wave"
    body = {
        "area": [30, -170, 15, -150],  # Area around Hawaii
        "params": ["swh", "mwd", "pp1d"],  # Significant wave height, mean wave direction, peak period
        "apikey": api_key
    }
    
    try:
        data = await ctx.fetch(session, url, method="POST", json_body=body)
        if data:
            fn = ctx.save("ecmwf_wave.json", data)
            out.append({
                "source": "ECMWF",
                "type": "wave_model",
                "filename": fn,
                "url": url,
                "priority": 1,
                "timestamp": utils.utcnow()
            })
    except Exception as e:
        log.warning(f"Failed to fetch ECMWF wave data: {e}")
    
    return out