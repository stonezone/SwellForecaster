#!/usr/bin/env python3
# agents/chart_agents.py - OPC, WPC and other chart collection agents
from __future__ import annotations
import logging
import re
from pathlib import Path
import utils
from bs4 import BeautifulSoup

log = logging.getLogger("chart_agents")

# Regular expression for excluded chart types
EXCL = re.compile(r"(logo|header|thumb_|usa_gov|twitter)", re.I)

async def opc(ctx, session):
    """Fetch Ocean Prediction Center charts"""
    # CRITICAL: These surface models with isobars are essential for forecasting
    # Explicit list of critical Pacific surface analysis and forecast charts
    critical_urls = [
        # Current surface analysis - most important for current conditions
        ("https://ocean.weather.gov/P_sfc_full_ocean_color.png", "opc_P_sfc_full_ocean_color.png", "pacific_surface", 1),
        ("https://ocean.weather.gov/P_w_sfc_color.png", "opc_P_w_sfc_color.png", "west_pacific_surface", 1),
        ("https://ocean.weather.gov/P_e_sfc_color.png", "opc_P_e_sfc_color.png", "east_pacific_surface", 1),

        # Surface forecasts - critical for future storm development
        ("https://ocean.weather.gov/shtml/P_24hrsfc.gif", "opc_P_24hrsfc.gif", "pacific_surface_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrsfc.gif", "opc_P_48hrsfc.gif", "pacific_surface_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrsfc.gif", "opc_P_72hrsfc.gif", "pacific_surface_72hr", 1),
        ("https://ocean.weather.gov/shtml/P_96hrsfc.gif", "opc_P_96hrsfc.gif", "pacific_surface_96hr", 1),

        # Wave height forecasts - essential for detecting significant swells
        ("https://ocean.weather.gov/shtml/P_24hrwhs.gif", "opc_P_24hrwhs.gif", "pacific_wave_height_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrwhs.gif", "opc_P_48hrwhs.gif", "pacific_wave_height_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrwhs.gif", "opc_P_72hrwhs.gif", "pacific_wave_height_72hr", 1),

        # Wave period forecasts - critical for swell quality assessment
        ("https://ocean.weather.gov/shtml/P_24hrwper.gif", "opc_P_24hrwper.gif", "pacific_wave_period_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrwper.gif", "opc_P_48hrwper.gif", "pacific_wave_period_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrwper.gif", "opc_P_72hrwper.gif", "pacific_wave_period_72hr", 1),
    ]

    out = []

    # First fetch all the critical forecast models with isobars
    for url, filename, subtype, priority in critical_urls:
        data = await ctx.fetch(session, url)
        if data:
            fn = ctx.save(filename, data)
            out.append({
                "source": "OPC",
                "type": "chart",
                "subtype": subtype,
                "filename": fn,
                "url": url,
                "priority": priority,  # Priority 1 for critical surface models
                "timestamp": utils.utcnow(),
                "isobars": True  # Flag to indicate this chart has isobars
            })
            log.info(f"Fetched critical OPC chart: {filename}")
        else:
            log.warning(f"Failed to fetch critical OPC chart: {url}")

    # Then also fetch additional charts from the web page
    html = await ctx.fetch(session, "https://ocean.weather.gov/Pac_tab.php")
    if not html:
        return out  # Return what we already have if page fetch fails

    soup = BeautifulSoup(html, "html.parser")

    for img in soup.find_all("img"):
        src = img.get("src", "")
        if not src or not re.search(r"\.(png|gif)$", src, re.I):
            continue

        if EXCL.search(src):
            continue

        # Skip files we've already fetched explicitly
        if any(src.endswith(Path(u[0]).name) for u in critical_urls):
            continue

        url = "https://ocean.weather.gov/" + src.lstrip("/")
        d = await ctx.fetch(session, url)

        if not d or len(d) < 25_000:
            continue

        fn = ctx.save(f"opc_{Path(url).name}", d)
        out.append({
            "source": "OPC",
            "filename": fn,
            "url": url,
            "type": "chart",
            "subtype": "supplementary",
            "priority": 2,  # Lower priority for supplementary charts
            "timestamp": utils.utcnow()
        })

    return out

async def wpc(ctx, session):
    """Fetch Weather Prediction Center charts"""
    out = []
    
    # Try original URLs first
    charts = [
        "https://tgftp.nws.noaa.gov/fax/PWFA12.TIF",  # 24h wave
        "https://tgftp.nws.noaa.gov/fax/PWFE12.TIF"   # 48h wave
    ]
    
    for url in charts:
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save("wpc_" + Path(url).name.lower(), d)
            out.append({
                "source": "WPC", 
                "filename": fn, 
                "url": url,
                "type": "forecast", 
                "priority": 2, 
                "timestamp": utils.utcnow()
            })
    
    # Try alternate URL (from older collector)
    if not out:
        url = "https://tgftp.nws.noaa.gov/fax/PWFA11.TIF"   # 24h surface
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save("wpc_24hr.tif", d)
            out.append({
                "source": "WPC", 
                "type": "surface_24hr",
                "filename": fn, 
                "url": url, 
                "priority": 2, 
                "timestamp": utils.utcnow()
            })
    
    # Add more North Pacific charts
    npac_urls = [
        "https://tgftp.nws.noaa.gov/fax/PPAE10.gif",  # N Pacific Surface Analysis
        "https://tgftp.nws.noaa.gov/fax/PPAE11.gif",  # N Pacific 24hr Surface Forecast
        "https://tgftp.nws.noaa.gov/fax/PPAE12.gif",  # N Pacific 48hr Surface Forecast
        "https://tgftp.nws.noaa.gov/fax/PJFA10.gif",  # Pacific Wave Analysis
    ]
    
    for idx, url in enumerate(npac_urls):
        hours = ["latest", "24h", "48h", "72h"][min(idx, 3)]
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"npac_{Path(url).name.lower()}", d)
            out.append({
                "source": "WPC", 
                "filename": fn, 
                "url": url,
                "type": "npac_chart", 
                "priority": 1, 
                "timestamp": utils.utcnow()
            })
    
    return out

async def nws(ctx, session):
    """Fetch National Weather Service forecasts and headlines"""
    out = []
    
    # Get Office Headlines
    for office in ("HFO", "GUM"):
        url = f"https://api.weather.gov/offices/{office}/headlines"
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"nws_{office.lower()}.json", d)
            out.append({
                "source": "NWS", 
                "type": "forecast_headlines",
                "filename": fn, 
                "url": url, 
                "priority": 3, 
                "timestamp": utils.utcnow()
            })
    
    # Get marine forecasts for zones
    zones = ["PHZ116", "PHZ117", "PHZ118"]  # North Shore, Windward, and Leeward Oahu
    for zone in zones:
        url = f"https://api.weather.gov/zones/forecast/{zone}/forecast"
        d = await ctx.fetch(session, url)
        if d:
            fn = ctx.save(f"nws_{zone.lower()}_forecast.json", d)
            out.append({
                "source": "NWS", 
                "type": "zone_forecast",
                "filename": fn, 
                "url": url, 
                "priority": 2, 
                "timestamp": utils.utcnow()
            })
    
    return out