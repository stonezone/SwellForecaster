#!/usr/bin/env python3
# collector.py – fetch Marine / Surf artefacts into timestamped bundle
from __future__ import annotations
import asyncio, json, logging, sys, time, uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional

import aiohttp, configparser
import utils

# Import all agents with fallback handling
try:
    import models
    models_available = True
except ImportError as e:
    logging.getLogger("collector").warning(f"Failed to import models module: {e}")
    models_available = False

try:
    import stormglass_agent
    stormglass_available = True
except ImportError as e:
    logging.getLogger("collector").warning(f"Failed to import stormglass_agent: {e}")
    stormglass_available = False

# Import the organized agent modules
import agents

log = utils.log_init("collector")

# --------------------------------------------------------------------- #
class Ctx:
    """Context class for data collection"""
    def __init__(self, cfg: configparser.ConfigParser):
        self.cfg = cfg
        self.run_id = f"{uuid.uuid4().hex}_{int(time.time())}"
        self.base = Path(cfg["GENERAL"]["data_dir"]).expanduser()
        self.bundle = self.base / self.run_id
        self.bundle.mkdir(parents=True, exist_ok=True)
        self.headers = {"User-Agent": cfg["GENERAL"]["user_agent"]}
        self.timeout = int(cfg["GENERAL"]["timeout"])
        self.retries = int(cfg["GENERAL"]["max_retries"])
        self.throttle = int(cfg["GENERAL"]["windy_throttle_seconds"])
        self.last_call: Dict[str, float] = {}
    
    async def fetch(self, sess: aiohttp.ClientSession, url: str,
                   *, method: str = "GET", json_body=None) -> bytes | None:
        """Fetch data from a URL with retries and throttling"""
        host = url.split("/")[2]
        if "windy.com" in host:
            gap = time.time() - self.last_call.get(host, 0)
            if gap < self.throttle:
                await asyncio.sleep(self.throttle - gap)
        
        for attempt in range(self.retries):
            try:
                if method == "GET":
                    r = await sess.get(url, headers=self.headers, timeout=self.timeout)
                else:
                    r = await sess.request(method, url, headers=self.headers,
                                          timeout=self.timeout, json=json_body)
                
                if r.status == 200:
                    self.last_call[host] = time.time()
                    return await r.read()
                if r.status == 404:
                    # Downgrade to debug level for 404s - these are common and expected
                    log.debug("HTTP 404 Not Found: %s", url)
                    return None
                if r.status == 403:
                    log.warning("HTTP 403 Forbidden: %s", url)
                    return None
                if r.status == 400 and "windy" in host:
                    # Windy free tier returns 400 for too many params
                    log.debug("HTTP 400 Bad Request (Windy API limit): %s", url)
                    return None
                if r.status == 400 and "stormglass" in host:
                    # Stormglass API limit
                    log.debug("HTTP 400 Bad Request (Stormglass API limit): %s", url)
                    return None
                if r.status in (400, 429, 500):
                    log.warning("HTTP %s %s", r.status, url)
                    back = 2 ** attempt
                    if attempt < self.retries - 1:  # Don't log retry message on last attempt
                        log.debug("Retry in %ss", back)
                    await asyncio.sleep(back)
                else:
                    log.info("HTTP %s %s", r.status, url)
                    return None
            except aiohttp.ClientConnectorCertificateError as e:
                # Handle SSL certificate errors
                log.debug("SSL Certificate error for %s: %s", url, str(e))
                if attempt == self.retries - 1:
                    log.warning("SSL Certificate verification failed for %s after %d attempts",
                               url, self.retries)
                await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientConnectorError as e:
                # Handle connection errors
                log.debug("Connection error for %s: %s", url, str(e))
                if attempt == self.retries - 1:
                    log.warning("Connection failed for %s after %d attempts: %s",
                               url, self.retries, str(e))
                await asyncio.sleep(2 ** attempt)
            except asyncio.TimeoutError:
                # Handle timeouts
                log.debug("Timeout error for %s – retry in %ss", url, 2 ** attempt)
                if attempt == self.retries - 1:
                    log.warning("Timeout error for %s after %d attempts",
                               url, self.retries)
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                # Handle other errors
                log.warning("Fetch error for %s: %s – retry in %ss",
                           url, str(e), 2 ** attempt)
                await asyncio.sleep(2 ** attempt)
        return None

    def save(self, name: str, data: bytes | str):
        """Save data to the bundle directory"""
        path = self.bundle / name
        path.write_bytes(data if isinstance(data, bytes) else data.encode())
        return path.name

# --------------------------------------------------------------------- #
async def collect(cfg, args):
    """Main collection function that orchestrates all data sources"""
    ctx = Ctx(cfg)

    # Prune old bundles
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.cache_days)
    for d in ctx.base.iterdir():
        if d.is_dir() and datetime.fromtimestamp(
               d.stat().st_mtime, timezone.utc) < cutoff:
            for f in d.iterdir(): f.unlink(missing_ok=True)
            d.rmdir()

    enabled = cfg["SOURCES"]
    
    # Use a longer timeout for network operations
    timeout = aiohttp.ClientTimeout(total=120)  # 2 minutes timeout

    # Configure SSL verification exceptions
    import ssl

    # Get domains that should skip SSL verification
    ssl_exception_domains = set()
    if "SSL_EXCEPTIONS" in cfg and "disable_verification" in cfg["SSL_EXCEPTIONS"]:
        exceptions = cfg["SSL_EXCEPTIONS"]["disable_verification"].split(',')
        ssl_exception_domains = set(domain.strip() for domain in exceptions if domain.strip())
        log.info(f"SSL verification disabled for: {', '.join(ssl_exception_domains)}")

    # Create context that doesn't verify certificates
    no_verify_ssl = ssl.create_default_context()
    no_verify_ssl.check_hostname = False
    no_verify_ssl.verify_mode = ssl.CERT_NONE

    # Prepare connector with SSL settings - use ssl=False to skip all SSL verification
    # This is a simplification - in a production environment you'd want to be more selective
    connector = aiohttp.TCPConnector(ssl=False)
    log.warning("SSL verification disabled for all connections for stability")

    session = None
    try:
        session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        # Create a list of tasks that can fail independently
        all_tasks = []
        
        # === Chart Agents ===
        if enabled.getboolean("enable_opc"):
            all_tasks.append(asyncio.create_task(agents.opc(ctx, session)))
        if enabled.getboolean("enable_wpc"):
            all_tasks.append(asyncio.create_task(agents.wpc(ctx, session)))
        if enabled.getboolean("enable_nws"):
            all_tasks.append(asyncio.create_task(agents.nws(ctx, session)))

        # === Buoy Agents ===
        if enabled.getboolean("enable_buoys"):
            all_tasks.append(asyncio.create_task(agents.buoys(ctx, session)))
        if enabled.getboolean("enable_coops"):
            all_tasks.append(asyncio.create_task(agents.noaa_coops(ctx, session)))

        # === Model Agents ===
        if enabled.getboolean("enable_pacioos"):
            all_tasks.append(asyncio.create_task(agents.pacioos(ctx, session)))
        if enabled.getboolean("enable_pacioos_swan"):
            all_tasks.append(asyncio.create_task(agents.pacioos_swan(ctx, session)))
        if enabled.getboolean("enable_ecmwf") and cfg["API"].get("ECMWF_KEY", "").strip():
            all_tasks.append(asyncio.create_task(agents.ecmwf_wave(ctx, session)))

        # === API Agents ===
        if enabled.getboolean("enable_windy"):
            all_tasks.append(asyncio.create_task(agents.windy(ctx, session)))
        if enabled.getboolean("enable_open_meteo"):
            all_tasks.append(asyncio.create_task(agents.open_meteo(ctx, session)))
        if enabled.getboolean("enable_stormglass") and stormglass_available:
            all_tasks.append(asyncio.create_task(stormglass_agent.stormglass_agent(ctx, session)))

        # === Regional Agents ===
        if enabled.getboolean("enable_southern_hemisphere"):
            all_tasks.append(asyncio.create_task(agents.southern_hemisphere(ctx, session)))
        if enabled.getboolean("enable_north_pacific"):
            all_tasks.append(asyncio.create_task(agents.north_pacific_enhanced(ctx, session)))

        # === WW3 model data with fallback mechanism ===
        if enabled.getboolean("enable_models"):
            # Check if models module is available
            if models_available:
                try:
                    # Try primary WW3 source first
                    model_task = asyncio.create_task(models.model_agent(ctx, session, ctx.fetch, ctx.save))
                    # Use fallback if primary fails
                    model_task.add_done_callback(
                        lambda t: all_tasks.append(asyncio.create_task(agents.ww3_model_fallback(ctx, session)))
                        if t.exception() or not t.result() else None
                    )
                    all_tasks.append(model_task)
                except Exception as e:
                    log.error(f"Failed to create model task: {e}")
                    # Add fallback directly if model task creation fails
                    all_tasks.append(asyncio.create_task(agents.ww3_model_fallback(ctx, session)))
            else:
                # Always use fallback if models module isn't available
                log.warning("Models module not available, using fallback")
                all_tasks.append(asyncio.create_task(agents.ww3_model_fallback(ctx, session)))

        # Wait for all tasks to complete, handling errors
        results = []
        for task in asyncio.as_completed(all_tasks):
            try:
                result = await task
                if result:  # Only extend if we got actual results
                    results.extend(result)
            except Exception as e:
                log.error(f"Task failed: {str(e)}")

        # Write metadata and update latest bundle pointer
        (ctx.bundle/"metadata.json").write_text(utils.jdump(
            {"run_id": ctx.run_id, "timestamp": utils.utcnow(), "results": results}))
        (ctx.base/"latest_bundle.txt").write_text(ctx.run_id)
        log.info("Bundle %s complete (%s files)", ctx.run_id, len(results))
        return ctx.bundle

    except Exception as e:
        log.error(f"Collection process failed: {e}")
        return None

    finally:
        # Ensure all tasks are completed or cancelled before closing session
        for task in asyncio.all_tasks():
            # Skip the main collection task
            if task is asyncio.current_task():
                continue
            try:
                task.cancel()
                # Allow a brief period for tasks to clean up
                await asyncio.sleep(0.1)
            except Exception as e:
                log.warning(f"Error cancelling task: {e}")

        # Ensure session is properly closed
        if session and not session.closed:
            try:
                await session.close()
                # Wait a bit to allow the session to fully close
                await asyncio.sleep(0.25)
            except Exception as e:
                log.error(f"Error closing session: {e}")

# --------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Collector – grab latest marine artefacts")
    parser.add_argument("--config", default="config.ini",
                       help="INI file to read")
    parser.add_argument("--cache-days", type=int, default=7,
                       help="days to keep bundles")
    args = parser.parse_args()
    
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    
    try:
        result = asyncio.run(collect(cfg, args))
        if result:
            print(result)
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)