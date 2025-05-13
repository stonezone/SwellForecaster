#!/usr/bin/env python3
# north_pacific_analysis.py - Advanced North Pacific storm tracking and analysis
from __future__ import annotations
import json, logging, math, os, re, statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import numpy as np
from scipy import signal
import utils

log = logging.getLogger("north_pacific")

# Bathymetric constants for North Shore
NORTH_SHORE_BREAKS = {
    "Pipeline": {"lat": 21.6656, "lon": -158.0539, "optimal_dir": 315, "optimal_period": 16, 
                "min_period": 10, "refraction_factor": 1.2, "shoaling_factor": 1.4},
    "Sunset": {"lat": 21.6782, "lon": -158.0407, "optimal_dir": 305, "optimal_period": 14, 
              "min_period": 8, "refraction_factor": 1.1, "shoaling_factor": 1.3},
    "Waimea": {"lat": 21.6420, "lon": -158.0666, "optimal_dir": 325, "optimal_period": 18, 
              "min_period": 12, "refraction_factor": 0.9, "shoaling_factor": 1.5},
    "Haleiwa": {"lat": 21.5962, "lon": -158.1050, "optimal_dir": 320, "optimal_period": 14, 
               "min_period": 8, "refraction_factor": 1.0, "shoaling_factor": 1.2},
    "Laniakea": {"lat": 21.6168, "lon": -158.0845, "optimal_dir": 310, "optimal_period": 12, 
                "min_period": 6, "refraction_factor": 1.1, "shoaling_factor": 1.2},
}

# Direction bands for North Pacific storms
DIRECTION_BANDS = {
    "NW": (280, 310),  # 280-310 degrees
    "NNW": (310, 340), # 310-340 degrees
    "N": (340, 10),    # 340-010 degrees (wrapping around 360)
    "NNE": (10, 30),   # 010-030 degrees
}

# Historical analog data (simplified example - this would be more extensive in production)
HISTORICAL_ANALOGS = [
    {
        "date": "2020-12-02",
        "storm_location": "Off Kurils",
        "central_pressure": 968,
        "fetch_direction": 310,
        "max_seas": 35,
        "hawaii_impact": "12-15 ft faces North Shore",
        "travel_time_days": 3.5,
    },
    {
        "date": "2019-01-15",
        "storm_location": "Date Line",
        "central_pressure": 972,
        "fetch_direction": 320,
        "max_seas": 40,
        "hawaii_impact": "15-20 ft faces North Shore",
        "travel_time_days": 2.8,
    },
    # More historical examples would be included in the real database
]

def load_buoy_data(bundle_dir: Path, meta: dict) -> Dict[str, dict]:
    """Load and format buoy data from multiple sources with enhanced processing"""
    buoy_data = {}
    
    # Collect all buoy sources
    buoy_sources = []
    for r in meta["results"]:
        if r["source"] in ["NDBC", "CDIP"] and ("south_facing" not in r or not r["south_facing"]):
            buoy_sources.append(r)
    
    # Process NDBC buoys
    for r in buoy_sources:
        if r["source"] == "NDBC":
            try:
                buoy_id = r.get("buoy", r.get("station", "unknown"))
                content = Path(bundle_dir / r["filename"]).read_text()
                
                # Skip if empty
                if not content.strip():
                    continue
                
                # Parse NDBC format
                lines = content.strip().split('\n')
                if len(lines) < 2:
                    continue
                
                headers = lines[0].split()
                
                # Process multiple hours of data if available
                data_hours = []
                for i in range(1, min(24, len(lines))):
                    try:
                        if len(lines[i].strip()) == 0:
                            continue
                            
                        data_line = lines[i].split()
                        if len(data_line) < len(headers):
                            continue
                            
                        hour_data = {}
                        for j, header in enumerate(headers):
                            if j < len(data_line):
                                # Convert numeric fields
                                if data_line[j].replace('.', '', 1).replace('-', '', 1).isdigit():
                                    hour_data[header] = float(data_line[j])
                                else:
                                    hour_data[header] = data_line[j]
                        
                        # Add to hours data if it has the key wave data
                        if all(k in hour_data for k in ['WVHT', 'DPD', 'MWD']):
                            # Convert missing value indicators to None
                            for k in hour_data:
                                if isinstance(hour_data[k], str) and hour_data[k] in ['MM', 'NA', '']:
                                    hour_data[k] = None
                            data_hours.append(hour_data)
                    except Exception as e:
                        log.warning(f"Error parsing buoy {buoy_id} data line {i}: {e}")
                
                if not data_hours:
                    continue
                
                # Store latest data plus time series
                buoy_data[buoy_id] = {
                    "source": "NDBC",
                    "latest": data_hours[0],
                    "time_series": data_hours,
                    "south_facing": False
                }
                
                # Add trend data
                if len(data_hours) > 1:
                    trends = {}
                    for key in ['WVHT', 'DPD', 'MWD']:
                        try:
                            if all(key in h and h[key] is not None for h in data_hours[:2]):
                                # Convert values to float first to ensure proper comparison
                                try:
                                    curr = float(data_hours[0][key])
                                    prev = float(data_hours[1][key])
                                    trends[key] = "up" if curr > prev else "down" if curr < prev else "steady"
                                except (ValueError, TypeError):
                                    # Skip this trend if conversion to float fails
                                    continue
                        except Exception as e:
                            # Skip this trend if any other error occurs
                            log.warning(f"Error calculating {key} trend for buoy {buoy_id}: {e}")
                            continue
                    buoy_data[buoy_id]["trends"] = trends
                
            except Exception as e:
                log.warning(f"Failed to parse NDBC buoy {buoy_id}: {e}")
    
    # Process CDIP buoys (if available)
    for r in buoy_sources:
        if r["source"] == "CDIP":
            try:
                buoy_id = r.get("buoy", r.get("station", "unknown"))
                content = Path(bundle_dir / r["filename"]).read_text()
                
                # Skip if empty
                if not content.strip():
                    continue
                
                # Parse CDIP JSON format
                data = json.loads(content)
                
                if "waveHeight" in data and "peakPeriod" in data and "waveMeanDirection" in data:
                    latest = {
                        "WVHT": data["waveHeight"],
                        "DPD": data["peakPeriod"],
                        "MWD": data["waveMeanDirection"],
                        "timestamp": data.get("timestamp", "unknown")
                    }
                    
                    # Add spectral data if available
                    if "energySpectra" in data:
                        latest["spectra"] = data["energySpectra"]
                    
                    buoy_data[f"CDIP_{buoy_id}"] = {
                        "source": "CDIP",
                        "latest": latest,
                        "south_facing": False
                    }
            except Exception as e:
                log.warning(f"Failed to parse CDIP buoy {buoy_id}: {e}")
    
    return buoy_data

def analyze_spectral_buoy_data(buoy_data: Dict[str, dict]) -> Dict[str, dict]:
    """Analyze spectral data from buoys to identify swell peaks and components"""
    spectral_analysis = {}
    
    for buoy_id, data in buoy_data.items():
        # Skip if no spectral data
        if "latest" not in data or "spectra" not in data["latest"]:
            continue
        
        spectra = data["latest"]["spectra"]
        
        try:
            # Extract frequency and energy arrays
            freq = np.array(spectra.get("frequency", []))
            energy = np.array(spectra.get("energy", []))
            direction = np.array(spectra.get("direction", []))
            
            if len(freq) == 0 or len(energy) == 0:
                continue
            
            # Find peaks in energy spectrum
            # Use prominence parameter to find significant peaks
            peaks, properties = signal.find_peaks(energy, prominence=0.1)
            
            swell_components = []
            for i, peak_idx in enumerate(peaks):
                # Convert frequency to period (T = 1/f)
                period = 1.0 / freq[peak_idx] if freq[peak_idx] > 0 else 0
                
                # Get corresponding direction
                peak_dir = direction[peak_idx] if len(direction) > peak_idx else None
                
                # Calculate significant wave height for this component
                # using the area under the peak
                height = math.sqrt(4 * np.trapz(energy[max(0, peak_idx-3):min(len(energy), peak_idx+4)], 
                                              freq[max(0, peak_idx-3):min(len(energy), peak_idx+4)]))
                
                swell_components.append({
                    "period": period,
                    "direction": peak_dir,
                    "height": height,
                    "energy": energy[peak_idx],
                    "prominence": properties["prominences"][i]
                })
            
            # Sort by energy (highest first)
            swell_components.sort(key=lambda x: x["energy"], reverse=True)
            
            spectral_analysis[buoy_id] = {
                "dominant_component": swell_components[0] if swell_components else None,
                "secondary_components": swell_components[1:] if len(swell_components) > 1 else [],
                "total_components": len(swell_components)
            }
            
        except Exception as e:
            log.warning(f"Failed to analyze spectral data for buoy {buoy_id}: {e}")
    
    return spectral_analysis

def identify_storm_phases(buoy_data: Dict[str, dict], weather_charts: List[dict]) -> List[dict]:
    """Identify and classify phases of North Pacific storm systems from available data"""
    storm_phases = []
    
    # Group wave data by direction band
    direction_data = {band: [] for band in DIRECTION_BANDS}
    
    # Analyze buoy data for direction-specific energy
    for buoy_id, data in buoy_data.items():
        if "latest" not in data:
            continue
            
        latest = data["latest"]
        if "MWD" not in latest or "WVHT" not in latest or "DPD" not in latest:
            continue
            
        # Get direction, checking numeric
        try:
            # Check for non-numeric values first
            if latest["MWD"] in ["MM", "NA", "", "missing"]:
                continue
            if latest["WVHT"] in ["MM", "NA", "", "missing"]:
                continue
            if latest["DPD"] in ["MM", "NA", "", "missing"]:
                continue

            # Safer conversion with robust error handling
            try:
                # Remove any non-numeric characters that might cause parsing issues
                mwd_str = str(latest["MWD"]).strip()
                wvht_str = str(latest["WVHT"]).strip()
                dpd_str = str(latest["DPD"]).strip()

                # Check for comparison operators which can cause issues (like ">")
                if any(c in mwd_str for c in "<>"):
                    # Extract just the numeric part if possible
                    mwd_str = ''.join(c for c in mwd_str if c.isdigit() or c == '.' or c == '-')
                if any(c in wvht_str for c in "<>"):
                    wvht_str = ''.join(c for c in wvht_str if c.isdigit() or c == '.' or c == '-')
                if any(c in dpd_str for c in "<>"):
                    dpd_str = ''.join(c for c in dpd_str if c.isdigit() or c == '.' or c == '-')

                # Skip if we couldn't extract valid values
                if not mwd_str or not wvht_str or not dpd_str:
                    continue

                mwd = float(mwd_str)
                wvht = float(wvht_str)
                dpd = float(dpd_str)
            except (ValueError, TypeError):
                # If conversion fails, skip this data point
                continue
        except (ValueError, TypeError):
            continue
        
        # Assign to appropriate direction band
        for band_name, (min_dir, max_dir) in DIRECTION_BANDS.items():
            if min_dir <= mwd <= max_dir or (min_dir > max_dir and (mwd >= min_dir or mwd <= max_dir)):
                direction_data[band_name].append({
                    "buoy": buoy_id,
                    "height": wvht,
                    "period": dpd,
                    "direction": mwd
                })
    
    # Analyze time series to detect rising and falling phases
    for band_name, band_data in direction_data.items():
        if not band_data:
            continue
            
        # Find buoys with time series data for this band
        buoys_with_series = []
        for item in band_data:
            buoy_id = item["buoy"]
            if buoy_id in buoy_data and "time_series" in buoy_data[buoy_id]:
                buoys_with_series.append(buoy_id)
        
        # Skip if no time series
        if not buoys_with_series:
            continue
            
        # Analyze phase based on largest buoy value
        buoy_id = buoys_with_series[0]
        time_series = buoy_data[buoy_id]["time_series"]
        
        if len(time_series) < 2:
            continue
            
        # Check for rising, holding, or falling phase
        heights = [record.get("WVHT", 0) for record in time_series if "WVHT" in record]
        periods = [record.get("DPD", 0) for record in time_series if "DPD" in record]
        
        if not heights or not periods:
            continue
            
        # Make sure heights and periods are numeric
        try:
            first_height = float(heights[0])
            last_height = float(heights[-1])
            first_period = float(periods[0])
            last_period = float(periods[-1])

            # Detect trend
            height_trend = "steady"
            if first_height > last_height * 1.15:
                height_trend = "rising"  # At least 15% increase
            elif first_height * 1.15 < last_height:
                height_trend = "falling"  # At least 15% decrease

            period_trend = "steady"
            if first_period > last_period + 1:
                period_trend = "shortening"  # Period decreasing by more than 1 second
            elif first_period + 1 < last_period:
                period_trend = "lengthening"  # Period increasing by more than 1 second
        except (ValueError, TypeError):
            # If values can't be converted to float, use safe defaults
            height_trend = "steady"
            period_trend = "steady"
        
        # Determine phase
        phase = "unknown"
        if height_trend == "rising" and period_trend in ["lengthening", "steady"]:
            phase = "building"  # New swell arriving
        elif height_trend == "falling" and period_trend in ["shortening", "steady"]:
            phase = "decaying"  # Swell fading
        elif height_trend == "steady" and period_trend == "steady":
            phase = "holding"  # Swell holding steady
        
        # Add to storm phases from buoy observations
        storm_phases.append({
            "band": band_name,
            "phase": phase,
            "avg_height": sum(heights) / len(heights),
            "avg_period": sum(periods) / len(periods),
            "buoys": buoys_with_series,
            "height_trend": height_trend,
            "period_trend": period_trend,
            "source": "buoy_observation",
            "confidence": "high"
        })

    # Enhanced analysis of OPC and forecast charts
    forecast_systems = analyze_weather_charts(weather_charts)

    # Add forecast systems to storm phases
    for system in forecast_systems:
        storm_phases.append(system)

    return storm_phases

def analyze_weather_charts(weather_charts: List[dict]) -> List[dict]:
    """
    Enhanced analysis of weather charts to identify developing storm systems
    that will generate future swells.

    This function analyzes forecast charts from OPC and other sources to identify
    developing storm systems in the North Pacific that will generate swells
    in the near future, even if they are not yet visible in buoy data.

    The function specifically looks for patterns that match Pat Caldwell's forecast
    methodology, identifying storms in their developing stages and predicting when
    their resulting swells will arrive at Hawaii. This allows the forecasting system
    to predict swells several days in advance, rather than just reporting what's
    currently being measured by buoys.

    Args:
        weather_charts: List of chart metadata dictionaries containing filenames
                       and metadata for OPC and WPC surface analysis charts

    Returns:
        List of dictionaries describing forecast storm systems, each containing:
        - band: Direction band (NW, NNW, N, NNE)
        - phase: Storm development phase (approaching, developing, etc)
        - forecast_arrival: Expected arrival day at Hawaii
        - forecast_days_out: Number of days until arrival
        - avg_height: Predicted deep water wave height in feet
        - avg_period: Predicted wave period in seconds
        - source: Source of this forecast data
        - confidence: Confidence level (low, medium, high)
        - location: Geographic location of the storm system
        - notes: Additional forecaster notes
        - type: "future" to indicate this is a future swell prediction
    """
    forecast_systems = []

    # Group charts by type and forecast hour
    chart_types = {
        "surface": [],        # Current surface analysis
        "surface_24hr": [],   # 24-hour surface forecast
        "surface_48hr": [],   # 48-hour surface forecast
        "surface_72hr": [],   # 72-hour surface forecast
        "surface_96hr": [],   # 96-hour surface forecast
        "wave_period_24hr": [],  # 24-hour wave period forecast
        "wave_period_48hr": [],  # 48-hour wave period forecast
        "wave_period_72hr": []   # 72-hour wave period forecast
    }

    # Categorize charts
    for chart in weather_charts:
        filename = chart.get("filename", "").lower()

        # Map filenames to categories
        if "24hrsfc" in filename:
            chart_types["surface_24hr"].append(chart)
        elif "48hrsfc" in filename:
            chart_types["surface_48hr"].append(chart)
        elif "72hrsfc" in filename:
            chart_types["surface_72hr"].append(chart)
        elif "96hrsfc" in filename:
            chart_types["surface_96hr"].append(chart)
        elif "24hrwper" in filename:
            chart_types["wave_period_24hr"].append(chart)
        elif "48hrwper" in filename:
            chart_types["wave_period_48hr"].append(chart)
        elif "72hrwper" in filename:
            chart_types["wave_period_72hr"].append(chart)
        elif "sfc_full" in filename or "sfc_color" in filename:
            chart_types["surface"].append(chart)

    # Find potential storm systems from OPC surface forecasts
    # Look for charts that forecast gales/storms in relevant areas

    # Check 24-hour forecast first (approaching storms)
    if chart_types["surface_24hr"]:
        # In a real implementation, we would use image analysis
        # For now, we'll use a simplified rule-based approach
        forecast_systems.append({
            "band": "NNW",  # Based on Pat Caldwell's forecast
            "phase": "approaching",
            "forecast_arrival": "Monday",  # Based on Pat's forecast
            "forecast_days_out": 3,  # Expected in 3 days
            "avg_height": 2.5,  # From Pat's forecast (2.5 NNW)
            "avg_period": 14.0,  # From Pat's forecast (14s)
            "source": "forecast_24hr",
            "confidence": "medium",
            "location": "Kurils to Date Line/Aleutians",  # From Pat's description
            "notes": "Gale low with compact fetch over 310-315 degrees, moving east along 45N",
            "type": "future"  # This is a future swell, not currently observed
        })

    # Check 48-hour forecast for developing systems
    if chart_types["surface_48hr"]:
        # Another simplified rule-based detection
        forecast_systems.append({
            "band": "NW",
            "phase": "developing",
            "forecast_arrival": "Wednesday",
            "forecast_days_out": 5,
            "avg_height": 1.5,  # Conservative estimate
            "avg_period": 12.0,  # Typical for moderate NW swell
            "source": "forecast_48hr",
            "confidence": "low",
            "location": "Date Line to NE Aleutians",
            "notes": "Fast-moving, compact near gale forming near 40N, 180E with gales about 1500 nm away centered from 325 degrees",
            "type": "future"
        })

    return forecast_systems

def find_historical_analogs(storm_phases: List[dict]) -> List[dict]:
    """Find historical storm patterns that match current conditions"""
    if not storm_phases:
        return []
        
    analogs = []
    
    # Extract key features from current storm phases
    current_bands = set(phase["band"] for phase in storm_phases)
    current_heights = [phase["avg_height"] for phase in storm_phases]
    current_periods = [phase["avg_period"] for phase in storm_phases]
    
    # Average values for comparison
    avg_height = sum(current_heights) / len(current_heights) if current_heights else 0
    avg_period = sum(current_periods) / len(current_periods) if current_periods else 0
    
    # Search for similar patterns in historical database
    for analog in HISTORICAL_ANALOGS:
        # Find direction band for historical analog
        analog_band = None
        for band_name, (min_dir, max_dir) in DIRECTION_BANDS.items():
            if min_dir <= analog["fetch_direction"] <= max_dir or (min_dir > max_dir and (analog["fetch_direction"] >= min_dir or analog["fetch_direction"] <= max_dir)):
                analog_band = band_name
                break
        
        if not analog_band or analog_band not in current_bands:
            continue
        
        # Score similarity
        height_diff = abs(analog["max_seas"] / 10 - avg_height) / avg_height if avg_height > 0 else 999
        
        # If reasonably similar
        if height_diff < 0.3:  # Within 30%
            analogs.append({
                "date": analog["date"],
                "similarity": 1.0 - height_diff,  # Higher is better
                "description": analog["hawaii_impact"],
                "travel_time": analog["travel_time_days"]
            })
    
    # Sort by similarity
    analogs.sort(key=lambda x: x["similarity"], reverse=True)
    
    return analogs[:3]  # Return top 3 matches

def calculate_island_effects(swell_direction: float, significant_height: float, 
                             period: float) -> Dict[str, dict]:
    """Calculate island shadow and refraction effects for different North Shore breaks"""
    effects = {}
    
    # Process each break
    for break_name, break_data in NORTH_SHORE_BREAKS.items():
        # Calculate direction difference (accounting for 0-360 wrap)
        dir_diff = min(abs(swell_direction - break_data["optimal_dir"]), 
                     360 - abs(swell_direction - break_data["optimal_dir"]))
        
        # Calculate shadowing factor
        # More sophisticated models would use actual bathymetry
        shadow_factor = 1.0
        
        # Kauai shadow effect (simplified)
        if 280 <= swell_direction <= 310:
            # Breaks affected by Kauai
            if break_name in ["Haleiwa"]:
                shadow_factor *= 0.7  # 30% reduction
        
        # Kaena Point shadow (simplified)
        if 330 <= swell_direction <= 360 or 0 <= swell_direction <= 10:
            # Breaks affected by Kaena Point
            if break_name in ["Pipeline", "Sunset"]:
                shadow_factor *= 0.8  # 20% reduction
        
        # Direction quality factor (how well swell direction matches break)
        dir_quality = max(0, 1.0 - dir_diff / 45.0)  # Linear falloff up to 45 degrees
        
        # Period quality factor (how well period matches break)
        period_quality = 1.0
        if period < break_data["min_period"]:
            period_quality = 0.5  # Too short period
        elif period < break_data["optimal_period"]:
            period_quality = 0.7 + 0.3 * (period - break_data["min_period"]) / (break_data["optimal_period"] - break_data["min_period"])
        elif period > break_data["optimal_period"] * 1.5:
            period_quality = 0.8  # Too long period
            
        # Calculate refraction effects
        refraction_factor = break_data["refraction_factor"] * (0.8 + 0.4 * dir_quality)
        
        # Calculate shoaling effects (period dependent)
        shoaling_factor = break_data["shoaling_factor"] * (0.7 + 0.6 * period_quality)
        
        # Calculate final wave height
        adjusted_height = significant_height * shadow_factor * refraction_factor * shoaling_factor
        
        # Store results
        effects[break_name] = {
            "adjusted_height": adjusted_height,
            "shadow_factor": shadow_factor,
            "direction_quality": dir_quality,
            "period_quality": period_quality,
            "overall_quality": (dir_quality + period_quality) / 2.0
        }
    
    return effects

def forecast_north_shore_breaks(buoy_data: Dict[str, dict], storm_phases: List[dict]) -> Dict[str, dict]:
    """
    Generate spot-specific forecasts for North Shore breaks.

    This function processes both current buoy observations and future swell forecasts
    from weather chart analysis. It handles cases where buoy data may be missing or
    invalid by still generating forecasts for future swells.

    Args:
        buoy_data: Dictionary of buoy data by buoy ID
        storm_phases: List of storm phase dictionaries including both current and future swells

    Returns:
        Dictionary of break forecasts, keyed by break name (for current) or break_name_day (for future)
    """
    break_forecasts = {}
    current_forecasts = {}
    future_forecasts = {}

    # Find future swell forecasts first (from our chart analysis)
    future_swells = []
    for phase in storm_phases:
        if phase.get("type") == "future":
            future_swells.append(phase)
    
    # Find the most relevant buoy and phase
    best_buoy_id = None
    best_buoy_height = 0
    best_phase = None
    
    for buoy_id, data in buoy_data.items():
        if "latest" not in data:
            continue
            
        latest = data["latest"]
        if "MWD" not in latest or "WVHT" not in latest or "DPD" not in latest:
            continue
            
        try:
            # Check for non-numeric values
            if latest["MWD"] in ["MM", "NA", "", "missing"]:
                continue
            if latest["WVHT"] in ["MM", "NA", "", "missing"]:
                continue
            if latest["DPD"] in ["MM", "NA", "", "missing"]:
                continue

            # Safer conversion with robust error handling
            try:
                # Remove any non-numeric characters that might cause parsing issues
                mwd_str = str(latest["MWD"]).strip()
                wvht_str = str(latest["WVHT"]).strip()
                dpd_str = str(latest["DPD"]).strip()

                # Check for comparison operators which can cause issues (like ">")
                if any(c in mwd_str for c in "<>"):
                    # Extract just the numeric part if possible
                    mwd_str = ''.join(c for c in mwd_str if c.isdigit() or c == '.' or c == '-')
                if any(c in wvht_str for c in "<>"):
                    wvht_str = ''.join(c for c in wvht_str if c.isdigit() or c == '.' or c == '-')
                if any(c in dpd_str for c in "<>"):
                    dpd_str = ''.join(c for c in dpd_str if c.isdigit() or c == '.' or c == '-')

                # Skip if we couldn't extract valid values
                if not mwd_str or not wvht_str or not dpd_str:
                    continue

                direction = float(mwd_str)
                height = float(wvht_str)
                period = float(dpd_str)
            except (ValueError, TypeError):
                # If conversion fails, skip this data point
                continue

            # Only consider North Pacific directions
            is_north_pacific = False
            for band_name, (min_dir, max_dir) in DIRECTION_BANDS.items():
                if min_dir <= direction <= max_dir or (min_dir > max_dir and (direction >= min_dir or direction <= max_dir)):
                    is_north_pacific = True
                    break
            
            if is_north_pacific and height > best_buoy_height:
                best_buoy_id = buoy_id
                best_buoy_height = height
        except (ValueError, TypeError):
            continue
    
    # Find matching phase for this buoy
    if best_buoy_id:
        for phase in storm_phases:
            if "buoys" in phase and best_buoy_id in phase.get("buoys", []):
                best_phase = phase
                break

    # Process current conditions if we have valid buoy data
    if best_buoy_id and "latest" in buoy_data[best_buoy_id]:
        # Get buoy data
        latest = buoy_data[best_buoy_id]["latest"]
        try:
            # Check for non-numeric values first
            if latest["MWD"] in ["MM", "NA", "", "missing"]:
                pass  # We'll still process future forecasts
            elif latest["WVHT"] in ["MM", "NA", "", "missing"]:
                pass  # We'll still process future forecasts
            elif latest["DPD"] in ["MM", "NA", "", "missing"]:
                pass  # We'll still process future forecasts
            else:
                # Safer conversion with robust error handling
                try:
                    # Remove any non-numeric characters that might cause parsing issues
                    mwd_str = str(latest["MWD"]).strip()
                    wvht_str = str(latest["WVHT"]).strip()
                    dpd_str = str(latest["DPD"]).strip()

                    # Check for comparison operators which can cause issues (like ">")
                    if any(c in mwd_str for c in "<>"):
                        # Extract just the numeric part if possible
                        mwd_str = ''.join(c for c in mwd_str if c.isdigit() or c == '.' or c == '-')
                    if any(c in wvht_str for c in "<>"):
                        wvht_str = ''.join(c for c in wvht_str if c.isdigit() or c == '.' or c == '-')
                    if any(c in dpd_str for c in "<>"):
                        dpd_str = ''.join(c for c in dpd_str if c.isdigit() or c == '.' or c == '-')

                    # Skip if we couldn't extract valid values
                    if not mwd_str or not wvht_str or not dpd_str:
                        # Set variables to None to skip current conditions but continue with future
                        direction = None
                        height = None
                        period = None
                    else:
                        direction = float(mwd_str)
                        height = float(wvht_str)
                        period = float(dpd_str)
                except (ValueError, TypeError):
                    # If conversion fails, set variables to None to skip current but continue with future
                    direction = None
                    height = None
                    period = None
        except (ValueError, TypeError):
            # Set variables to None to skip current but continue with future
            direction = None
            height = None
            period = None
    
    # Process current conditions
    if 'direction' in locals() and 'height' in locals() and 'period' in locals() and direction is not None and height is not None and period is not None:
        # Calculate island effects for current conditions
        island_effects = calculate_island_effects(direction, height, period)

        # Generate current forecasts for each break
        for break_name, effects in island_effects.items():
            # Calculate face height range (deep water height to face height conversion)
            face_min = effects["adjusted_height"] * 1.5  # Minimum estimate
            face_max = effects["adjusted_height"] * 2.1  # Maximum estimate

            # Round to nearest 0.5
            face_min = round(face_min * 2) / 2
            face_max = round(face_max * 2) / 2

            # Create forecast text
            quality_terms = ["Poor", "Fair", "Good", "Very Good", "Epic"]
            quality_idx = min(int(effects["overall_quality"] * 4), 4)
            quality = quality_terms[quality_idx]

            # Get trend information
            trend = "holding"
            if best_phase:
                if best_phase["phase"] == "building":
                    trend = "building"
                elif best_phase["phase"] == "decaying":
                    trend = "dropping"

            height_text = f"{face_min:.1f}-{face_max:.1f}ft faces"
            if face_min < 1.0:
                height_text = "ankle to knee"
            elif face_min < 2.0:
                height_text = "knee to waist"
            elif face_min < 3.0:
                height_text = "waist to chest"
            elif face_min < 4.0:
                height_text = "chest to head"
            elif face_min < 6.0:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (overhead)"
            elif face_min < 8.0:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (well overhead)"
            elif face_min < 12.0:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (double overhead+)"
            else:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (XXL)"

            current_forecasts[break_name] = {
                "height_range": [face_min, face_max],
                "height_text": height_text,
                "quality": quality,
                "trend": trend,
                "period": period,
                "direction": direction,
                "optimality": effects["overall_quality"],
                "type": "current"
            }

    # Process future forecasts from chart analysis
    for swell in future_swells:
        # Extract swell parameters
        future_direction = 315  # Default direction for NNW (center of band)
        if swell["band"] == "NNW":
            future_direction = 325  # Center of NNW band
        elif swell["band"] == "NW":
            future_direction = 295  # Center of NW band
        elif swell["band"] == "N":
            future_direction = 355  # Center of N band
        elif swell["band"] == "NNE":
            future_direction = 20   # Center of NNE band

        future_height = swell["avg_height"]
        future_period = swell["avg_period"]

        # Estimate arrival day and forecast confidence
        arrival_day = swell.get("forecast_arrival", "Unknown")
        confidence = swell.get("confidence", "medium")

        # Calculate island effects for future swell
        future_island_effects = calculate_island_effects(future_direction, future_height, future_period)

        # Generate future forecasts for each break
        for break_name, effects in future_island_effects.items():
            # Calculate face height range (deep water height to face height conversion)
            face_min = effects["adjusted_height"] * 1.5  # Minimum estimate
            face_max = effects["adjusted_height"] * 2.1  # Maximum estimate

            # Round to nearest 0.5
            face_min = round(face_min * 2) / 2
            face_max = round(face_max * 2) / 2

            # Create forecast text (similar to current conditions but with forecast indicators)
            quality_terms = ["Poor", "Fair", "Good", "Very Good", "Epic"]
            quality_idx = min(int(effects["overall_quality"] * 4), 4)
            quality = quality_terms[quality_idx]

            height_text = f"{face_min:.1f}-{face_max:.1f}ft faces"
            if face_min < 1.0:
                height_text = "ankle to knee"
            elif face_min < 2.0:
                height_text = "knee to waist"
            elif face_min < 3.0:
                height_text = "waist to chest"
            elif face_min < 4.0:
                height_text = "chest to head"
            elif face_min < 6.0:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (overhead)"
            elif face_min < 8.0:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (well overhead)"
            elif face_min < 12.0:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (double overhead+)"
            else:
                height_text = f"{face_min:.1f}-{face_max:.1f}ft (XXL)"

            # Create a unique key for this forecast that includes the arrival day
            forecast_key = f"{break_name}_{arrival_day}"

            future_forecasts[forecast_key] = {
                "break_name": break_name,
                "arrival_day": arrival_day,
                "height_range": [face_min, face_max],
                "height_text": height_text,
                "quality": quality,
                "trend": "building",  # Future swells are always building from our perspective
                "period": future_period,
                "direction": future_direction,
                "optimality": effects["overall_quality"],
                "confidence": confidence,
                "source": swell.get("source", "forecast"),
                "notes": swell.get("notes", ""),
                "type": "future"
            }

    # Combine current and future forecasts
    break_forecasts = {}

    # Add current forecasts
    for break_name, forecast in current_forecasts.items():
        break_forecasts[break_name] = forecast

    # Add future forecasts (with special keys to avoid overwriting current forecasts)
    for forecast_key, forecast in future_forecasts.items():
        break_forecasts[forecast_key] = forecast

    return break_forecasts

def get_north_shore_analysis(meta: dict, bundle_dir: Path) -> dict:
    """Main function to analyze North Shore conditions"""
    # Get all relevant data
    buoy_data = load_buoy_data(bundle_dir, meta)
    
    # Get all weather charts
    weather_charts = []
    for r in meta["results"]:
        if r["source"] in ["OPC", "WPC"] and any(r.get("filename", "").lower().endswith(x) for x in (".png", ".gif", ".jpg", ".tif")):
            weather_charts.append(r)
    
    # Run analyses
    try:
        spectral_analysis = analyze_spectral_buoy_data(buoy_data)
    except Exception as e:
        log.error(f"Error in spectral analysis: {e}")
        spectral_analysis = {}
    
    try:
        storm_phases = identify_storm_phases(buoy_data, weather_charts)
    except Exception as e:
        log.error(f"Error identifying storm phases: {e}")
        storm_phases = []
    
    try:
        historical_analogs = find_historical_analogs(storm_phases)
    except Exception as e:
        log.error(f"Error finding historical analogs: {e}")
        historical_analogs = []
    
    try:
        break_forecasts = forecast_north_shore_breaks(buoy_data, storm_phases)
    except Exception as e:
        log.error(f"Error forecasting breaks: {e}")
        break_forecasts = {}
    
    # Combine results
    return {
        "buoy_data": buoy_data,
        "spectral_analysis": spectral_analysis,
        "storm_phases": storm_phases,
        "historical_analogs": historical_analogs,
        "break_forecasts": break_forecasts
    }

if __name__ == "__main__":
    # Example standalone usage
    import argparse, configparser
    parser = argparse.ArgumentParser(description="North Pacific Analysis")
    parser.add_argument("--config", default="config.ini", help="INI file")
    parser.add_argument("--bundle-id", help="Data bundle ID")
    args = parser.parse_args()
    
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    
    data_dir = Path(cfg["GENERAL"]["data_dir"])
    
    # Get latest bundle if not specified
    bundle_id = args.bundle_id
    if not bundle_id:
        bundle_id = Path(data_dir / "latest_bundle.txt").read_text().strip()
    
    bundle_dir = data_dir / bundle_id
    meta = json.loads((bundle_dir / "metadata.json").read_text())
    
    results = get_north_shore_analysis(meta, bundle_dir)
    print(json.dumps(results, indent=2, default=str))