#!/usr/bin/env python3
# utils.py – tiny helper toolkit
from __future__ import annotations
import json, logging, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

def utcnow() -> str:
    """RFC‑3339 UTC timestamp."""
    # Fixed deprecation warning: using datetime.now(timezone.utc) instead of utcnow()
    return datetime.now(timezone.utc).isoformat()

def jdump(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)

def log_init(name: str, level: str | int = "INFO") -> logging.Logger:
    log_dir = Path("logs"); log_dir.mkdir(exist_ok=True)
    # Fixed deprecation warning: using datetime.now(timezone.utc) instead of utcnow()
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_dir / f"{name}_{datetime.now(timezone.utc):%Y%m%d}.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(name)

def getint_safe(cfg, section, key, default=0):
    """Safely get integer from config with fallback."""
    try:
        return int(cfg[section][key])
    except (KeyError, ValueError):
        return default

def argparser(desc: str):
    import argparse
    p = argparse.ArgumentParser(description=desc)
    p.add_argument("--config", default="config.ini",
                   help="INI file to read")
    p.add_argument("--cache-days", type=int, default=7,
                   help="days to keep bundles")
    return p

def get_south_swell_status():
    """Check if there's a significant south swell in the forecast or currently active.
    This can be used to automatically set the south_swell_emphasis flag."""
    try:
        # You could implement a check against recent buoy data or API sources
        # For now, this is a placeholder that would need to be implemented
        # with actual data sources
        from datetime import datetime
        
        # Example implementation:
        # Check if we're in south swell season (typically April-September)
        current_month = datetime.now().month
        if 4 <= current_month <= 9:
            # Higher chance of south swells during these months
            return 0.6  # 60% chance
        else:
            return 0.3  # 30% chance during off-season
            
    except Exception as e:
        logging.getLogger("utils").warning(f"Failed to check south swell status: {e}")
        return 0.0  # Default to no south swell if we can't check