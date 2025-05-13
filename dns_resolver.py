#!/usr/bin/env python3
# dns_resolver.py - DNS resolution helpers for collector

import asyncio
import socket
import logging
import os
from typing import List, Optional, Dict

log = logging.getLogger("dns_resolver")

# Alternative DNS resolvers to try
PUBLIC_DNS = [
    "8.8.8.8",       # Google
    "1.1.1.1",       # Cloudflare
    "9.9.9.9",       # Quad9
    "208.67.222.222" # OpenDNS
]

# Cache of resolved IPs
resolved_ips: Dict[str, str] = {}

async def resolve_host_alternative(hostname: str) -> Optional[str]:
    """
    Try to resolve a hostname using alternative DNS servers if the system's DNS fails.
    
    Args:
        hostname: The hostname to resolve
        
    Returns:
        The IP address as string if resolved, None otherwise
    """
    # Check cache first
    if hostname in resolved_ips:
        return resolved_ips[hostname]
    
    # First try standard resolution
    try:
        info = await asyncio.get_event_loop().getaddrinfo(
            hostname, None, family=socket.AF_INET
        )
        if info:
            ip = info[0][4][0]  # Extract the IP address
            resolved_ips[hostname] = ip
            return ip
    except Exception as e:
        log.debug(f"Standard DNS resolution failed for {hostname}: {e}")
    
    # Try with alternative DNS servers
    for dns_server in PUBLIC_DNS:
        try:
            # Use subprocess to call dig or nslookup
            resolver = 'dig' if _command_exists('dig') else 'nslookup'
            
            if resolver == 'dig':
                cmd = f"dig @{dns_server} {hostname} +short"
            else:
                cmd = f"nslookup {hostname} {dns_server} | grep -oE '([0-9]{{1,3}}\\.){{3}}[0-9]{{1,3}}$' | head -1"
                
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            ip = stdout.decode().strip()
            if ip:
                log.info(f"Resolved {hostname} to {ip} using {dns_server}")
                resolved_ips[hostname] = ip
                return ip
        except Exception as e:
            log.debug(f"Alternative DNS resolution with {dns_server} failed for {hostname}: {e}")
    
    # If all else fails, try a hardcoded list of common hostnames
    common_hosts = {
        "www.opc.ncep.noaa.gov": "140.90.96.19",
        "ocean.weather.gov": "140.90.96.19",
        "www.cdip.ucsd.edu": "132.239.120.129",
        "tgftp.nws.noaa.gov": "140.90.96.19",
        "www.fnmoc.navy.mil": "205.128.64.16"
    }
    
    if hostname in common_hosts:
        ip = common_hosts[hostname]
        log.info(f"Using hardcoded IP {ip} for {hostname}")
        resolved_ips[hostname] = ip
        return ip
    
    return None

def _command_exists(cmd: str) -> bool:
    """Check if a command exists in the system."""
    return any(
        os.access(os.path.join(path, cmd), os.X_OK)
        for path in os.environ["PATH"].split(os.pathsep)
    )