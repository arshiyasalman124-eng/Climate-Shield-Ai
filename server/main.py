import json
import asyncio
import math
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from xml.sax.saxutils import escape
from xml.etree import ElementTree as ET
from html import unescape

import httpx
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

APP_DIR = Path(__file__).resolve().parent
load_dotenv(APP_DIR / ".env")
DB_PATH = Path(os.getenv("CLIMATESHIELD_DB", APP_DIR / "climateshield.sqlite3"))
app = FastAPI(title="ClimateShield AI", version="1.0.0", description="Local weather awareness and preparedness API")
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CLIENT_URL", "http://localhost:5173").split(","), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
_reverse_cache = {}
_reverse_lock = asyncio.Lock()
_last_reverse_request = 0.0


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.on_event("startup")
def init_db():
    with db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS report_history (id INTEGER PRIMARY KEY, location TEXT NOT NULL, latitude REAL NOT NULL, longitude REAL NOT NULL, generated_at TEXT NOT NULL, summary_json TEXT NOT NULL)")


async def weather_for(lat: float, lon: float):
    params = {"latitude": lat, "longitude": lon, "current": "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,pressure_msl,wind_speed_10m,wind_gusts_10m,wind_direction_10m", "hourly": "temperature_2m,precipitation_probability,precipitation,weather_code,wind_speed_10m,wind_gusts_10m", "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,sunrise,sunset", "timezone": "auto", "forecast_days": 7}
    async with httpx.AsyncClient(timeout=15, headers={"User-Agent": "ClimateShieldAI/1.0 (weather awareness app)"}) as client:
        response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
        response.raise_for_status()
        return response.json()


async def air_quality_for(lat: float, lon: float):
    """Fetch the current Open-Meteo AQI, particulate, and UV values; air quality is optional."""
    params = {"latitude": lat, "longitude": lon, "current": "us_aqi,european_aqi,pm2_5,pm10,uv_index", "timezone": "auto"}
    try:
        async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "ClimateShieldAI/1.0 (weather awareness app)"}) as client:
            response = await client.get("https://air-quality-api.open-meteo.com/v1/air-quality", params=params)
            response.raise_for_status()
            payload = response.json()
        current = payload.get("current", {})
        values = {key: current.get(key) for key in ("us_aqi", "european_aqi", "pm2_5", "pm10", "uv_index")}
        available = any(value is not None for value in values.values())
        return {"available": available, "time": current.get("time"), "timezone": payload.get("timezone"), "units": payload.get("current_units", {}), **values, "message": None if available else "Current air quality values are unavailable for this location."}
    except (httpx.HTTPError, ValueError, KeyError):
        return {"available": False, "time": None, "us_aqi": None, "european_aqi": None, "pm2_5": None, "pm10": None, "uv_index": None, "message": "Air quality data could not be retrieved."}


def _trigger(metric, value, unit, timestamp, detail):
    normalized_value = round(float(value), 1) if isinstance(value, (int, float)) else value
    return {"trigger_metric": metric, "trigger_value": normalized_value, "trigger_unit": unit, "trigger_time": timestamp, "trigger": {"metric": metric, "value": normalized_value, "unit": unit, "timestamp": timestamp}, "detail": detail, "reason": detail}


def assess(data, air_quality=None):
    """Apply screening thresholds and attach the exact metric and forecast timestamp for each trigger."""
    hourly, daily = data.get("hourly", {}), data.get("daily", {})
    times = hourly.get("time", [])
    next24_count = min(24, len(times))
    rain = hourly.get("precipitation", [])[:next24_count]
    rain_p = hourly.get("precipitation_probability", [])[:next24_count]
    rain_total = sum(v for v in rain if isinstance(v, (int, float)))
    rain_times = times[:len(rain)]
    rain_end = rain_times[-1] if rain_times else None
    peak_rain_probability = max(range(len(rain_p)), key=lambda i: rain_p[i] if isinstance(rain_p[i], (int, float)) else -1, default=None)
    peak_rain_prob = rain_p[peak_rain_probability] if peak_rain_probability is not None else 0
    peak_rain_time = times[peak_rain_probability] if peak_rain_probability is not None and peak_rain_probability < len(times) else rain_end
    risks = []

    if rain_total >= 30 or peak_rain_prob >= 80:
        if rain_total >= 30:
            metric, value, unit, trigger_time = "24-hour precipitation total", rain_total, "mm", rain_end
            detail = f"{rain_total:.1f} mm rainfall forecast over the next 24 hours (window through {rain_end or 'the next 24 hours'})."
        else:
            metric, value, unit, trigger_time = "precipitation probability", peak_rain_prob, "%", peak_rain_time
            detail = f"Rain probability peaks at {peak_rain_prob}% at {peak_rain_time or 'an unspecified time'} in the next 24 hours."
        level = "Severe" if rain_total >= 100 else "High" if rain_total >= 60 or peak_rain_prob >= 95 else "Moderate"
        risks.append({"type": "Heavy Rainfall", "level": level, **_trigger(metric, value, unit, trigger_time, detail), "precautions": "Carry rain protection, avoid waterlogged roads, and never walk or drive through floodwater."})

    if rain_total >= 50:
        level = "Severe" if rain_total >= 100 else "High" if rain_total >= 75 else "Moderate"
        detail = f"{rain_total:.1f} mm forecast in 24 hours through {rain_end or 'the next 24 hours'} may increase local flooding risk."
        risks.append({"type": "Flood Risk", "level": level, **_trigger("24-hour precipitation total", rain_total, "mm", rain_end, detail), "precautions": "Avoid flood-prone routes, move valuables above floor level where appropriate, and follow local evacuation instructions. Never enter floodwater."})

    codes = hourly.get("weather_code", [])[:next24_count]
    storm_index = next((i for i, code in enumerate(codes) if isinstance(code, (int, float)) and code >= 95), None)
    if storm_index is not None:
        storm_time = times[storm_index] if storm_index < len(times) else None
        code = codes[storm_index]
        detail = f"Thunderstorm forecast (weather code {code}) at {storm_time or 'an unspecified time'} in the next 24 hours."
        risks.append({"type": "Thunderstorms", "level": "Moderate", **_trigger("thunderstorm weather code", code, "WMO code", storm_time, detail), "precautions": "Move indoors, avoid open areas and tall isolated objects, and follow official local guidance."})

    temperatures = hourly.get("temperature_2m", [])
    heat_index = max(range(len(temperatures)), key=lambda i: temperatures[i] if isinstance(temperatures[i], (int, float)) else float("-inf"), default=None)
    if heat_index is not None and isinstance(temperatures[heat_index], (int, float)) and temperatures[heat_index] >= 40:
        heat = temperatures[heat_index]
        heat_time = times[heat_index] if heat_index < len(times) else None
        level = "Severe" if heat >= 45 else "High"
        detail = f"Temperature reaches {heat:.1f}°C at {heat_time or 'an unspecified forecast time'}."
        risks.append({"type": "Extreme Heat", "level": level, **_trigger("forecast air temperature", heat, "°C", heat_time, detail), "precautions": "Drink water regularly, reduce strenuous outdoor activity during the hottest hours, and check on vulnerable people."})

    gusts = hourly.get("wind_gusts_10m", [])
    wind_metric = "wind gusts"
    wind_values = gusts if any(isinstance(v, (int, float)) for v in gusts) else hourly.get("wind_speed_10m", [])
    if wind_values is not gusts:
        wind_metric = "wind speed"
    wind_index = max(range(len(wind_values)), key=lambda i: wind_values[i] if isinstance(wind_values[i], (int, float)) else float("-inf"), default=None)
    if wind_index is not None and isinstance(wind_values[wind_index], (int, float)) and wind_values[wind_index] >= 50:
        speed = wind_values[wind_index]
        wind_time = times[wind_index] if wind_index < len(times) else None
        level = "Severe" if speed >= 100 else "High" if speed >= 75 else "Moderate"
        detail = f"Peak {wind_metric} of {speed:.0f} km/h expected at {wind_time or 'an unspecified forecast time'}."
        risks.append({"type": "Strong Winds", "level": level, **_trigger(wind_metric, speed, "km/h", wind_time, detail), "precautions": "Secure loose outdoor items, stay clear of trees and power lines, and avoid exposed areas during gusts."})

    daily_rain = daily.get("precipitation_sum", [])
    daily_highs = daily.get("temperature_2m_max", [])
    seven_day_rain = sum(v for v in daily_rain[:7] if isinstance(v, (int, float)))
    seven_day_high = max((v for v in daily_highs[:7] if isinstance(v, (int, float))), default=0)
    daily_times = daily.get("time", [])
    drought_time = daily_times[min(6, len(daily_times) - 1)] if daily_times else None
    if len(daily_rain) and seven_day_rain < 5 and seven_day_high >= 35:
        detail = f"Only {seven_day_rain:.1f} mm rain forecast over 7 days through {drought_time or 'the forecast period'}, with highs up to {seven_day_high:.1f}°C."
        risks.append({"type": "Drought", "level": "Low", **_trigger("7-day precipitation total", seven_day_rain, "mm", drought_time, detail), "precautions": "Use water carefully and check local drought, fire, and water-supply advisories."})

    air_quality = air_quality or {}
    us_aqi, eu_aqi = air_quality.get("us_aqi"), air_quality.get("european_aqi")
    aqi_value = None
    if isinstance(us_aqi, (int, float)) and us_aqi >= 101:
        aqi_value = (us_aqi, "US AQI", 101)
    if isinstance(eu_aqi, (int, float)) and eu_aqi >= 60 and (aqi_value is None or eu_aqi >= 80):
        aqi_value = (eu_aqi, "European AQI", 60)
    if aqi_value:
        value, metric, threshold = aqi_value
        if metric == "US AQI":
            level = "Severe" if value >= 201 else "High" if value >= 151 else "Moderate"
        else:
            level = "Severe" if value > 100 else "High" if value >= 80 else "Moderate"
        stamp = air_quality.get("time")
        detail = f"{metric} is {value:g} (elevated threshold {threshold}) at {stamp or 'the latest reading'}."
        risks.append({"type": "Poor Air Quality", "level": level, **_trigger(metric, value, "index", stamp, detail), "precautions": "Consider reducing strenuous outdoor activity, especially for children, older adults, and people with heart or lung conditions. Follow local air-quality guidance."})
    return risks


@app.get("/api/health")
def health():
    with db() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok", "database": "connected"}


@app.get("/api/location/reverse")
async def reverse_location(latitude: float, longitude: float):
    """Resolve browser coordinates to names needed for regional alert routing."""
    global _last_reverse_request
    cache_key = (round(latitude, 3), round(longitude, 3))
    if cache_key in _reverse_cache:
        return _reverse_cache[cache_key]
    try:
        async with _reverse_lock:
            if cache_key in _reverse_cache:
                return _reverse_cache[cache_key]
            delay = 1.05 - (time.monotonic() - _last_reverse_request)
            if delay > 0:
                await asyncio.sleep(delay)
            _last_reverse_request = time.monotonic()
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers={"User-Agent": "ClimateShieldAI/1.0 (local weather awareness application)", "Accept-Language": "en"}) as client:
                response = await client.get("https://nominatim.openstreetmap.org/reverse", params={"lat": latitude, "lon": longitude, "format": "jsonv2", "zoom": 10, "addressdetails": 1})
                response.raise_for_status()
                payload = response.json()
            address = payload.get("address", {})
            name = next((address.get(k) for k in ("city", "town", "village", "municipality", "hamlet", "county") if address.get(k)), "Current location")
            result = {"name": name, "admin1": address.get("state") or address.get("region") or "", "admin2": address.get("state_district") or address.get("county") or "", "country": address.get("country") or "", "country_code": (address.get("country_code") or "").upper(), "latitude": latitude, "longitude": longitude, "attribution": "© OpenStreetMap contributors"}
            _reverse_cache[cache_key] = result
            return result
    except (httpx.HTTPError, ValueError, KeyError):
        raise HTTPException(502, "Location names could not be resolved.")


INDIA_FEEDS = [
    ("NDMA SACHET", "https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml"),
    ("IMD", "https://cap-sources.s3.amazonaws.com/in-imd-en/rss.xml"),
]
GDACS_FEED = "https://www.gdacs.org/xml/rss.xml"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1].lower()


def _child_text(node, name: str, default=""):
    for child in node.iter():
        if _local(child.tag) == name.lower() and child.text:
            return child.text.strip()
    return default


def _parse_date(value: str | None):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (ValueError, TypeError):
        try:
            parsed = parsedate_to_datetime(value.strip())
        except (TypeError, ValueError, OverflowError):
            return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _active(start: str | None, end: str | None, now: datetime, recent: bool = False) -> bool:
    begins, expires = _parse_date(start), _parse_date(end)
    if begins and begins > now:
        return False
    if expires:
        return expires >= now
    return bool(recent and begins and (now - begins).total_seconds() <= 72 * 3600)


def _normal(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").casefold()).strip()


def _point_in_polygon(lat: float, lon: float, polygon: list[tuple[float, float]]) -> bool:
    inside = False
    if len(polygon) < 3:
        return False
    j = len(polygon) - 1
    for i, (yi, xi) in enumerate(polygon):
        yj, xj = polygon[j]
        if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(min(1, a)))


async def _get_xml(client: httpx.AsyncClient, url: str):
    response = await client.get(url)
    response.raise_for_status()
    return ET.fromstring(response.content)


def _cap_nodes(root):
    if _local(root.tag) == "alert":
        return [root]
    found = []
    for node in root.iter():
        if _local(node.tag) == "alert":
            found.append(node)
        elif _local(node.tag) in {"description", "encoded", "content"} and node.text and re.search(r"<(?:[\w.-]+:)?alert(?:\s|>)", node.text, re.IGNORECASE):
            try:
                nested = ET.fromstring(unescape(node.text.strip()))
                if _local(nested.tag) == "alert":
                    found.append(nested)
            except ET.ParseError:
                continue
    return found


def _cap_alert(cap, authority: str, lat: float, lon: float, location_name: str, state: str, district: str, now: datetime):
    status = _child_text(cap, "status", "Actual").casefold()
    msg_type = _child_text(cap, "msgType", "Alert").casefold()
    if status != "actual" or msg_type == "cancel":
        return None
    infos = [n for n in cap.iter() if _local(n.tag) == "info"]
    info = next((n for n in infos if _child_text(n, "language").casefold().startswith("en")), infos[0] if infos else cap)
    headline = _child_text(info, "headline") or _child_text(info, "event") or "Official alert"
    description = _child_text(info, "description")
    severity = _child_text(info, "severity") or "Unknown"
    effective = _child_text(info, "effective") or _child_text(cap, "sent")
    onset = _child_text(info, "onset") or effective
    expires = _child_text(info, "expires")
    if not _active(onset, expires, now):
        return None

    areas = [n for n in info.iter() if _local(n.tag) == "area"]
    area_names, polygons, circles = [], [], []
    for area in areas:
        area_name = _child_text(area, "areaDesc")
        if area_name:
            area_names.append(area_name)
        for item in area.iter():
            kind, value = _local(item.tag), (item.text or "").strip()
            if kind == "polygon":
                points = []
                for pair in value.split():
                    try:
                        plat, plon = map(float, pair.split(",")[:2])
                        points.append((plat, plon))
                    except (ValueError, TypeError):
                        continue
                if len(points) >= 3:
                    polygons.append(points)
            elif kind == "circle":
                try:
                    plat, plon, radius = map(float, value.split()[:3])
                    circles.append((plat, plon, radius))
                except (ValueError, TypeError):
                    continue

    tokens = [_normal(x) for x in (location_name, state, district) if x]
    names = _normal(" ".join(area_names))
    text = _normal(f"{headline} {description}")
    named_match = any(t and (t in names or t in text) for t in tokens)
    geo_match = any(_point_in_polygon(lat, lon, polygon) for polygon in polygons)
    geo_match = geo_match or any(_haversine_km(lat, lon, plat, plon) <= radius for plat, plon, radius in circles)
    if not (named_match or geo_match):
        return None
    issuer = _child_text(cap, "senderName") or authority
    return {"event": _child_text(info, "event") or headline, "headline": headline, "description": description, "sender": issuer, "authority": issuer, "feed_authority": authority, "source": authority, "severity": severity, "starts": onset, "ends": expires, "area": "; ".join(area_names), "url": _child_text(info, "web") or None}


def _rss_items(root):
    return [node for node in root.iter() if _local(node.tag) == "item"]


def _rss_text(item, name: str):
    return _child_text(item, name)


def _india_match(location_name: str, state: str, country: str, country_code: str) -> bool:
    return country_code.upper() == "IN" or _normal(country) in {"india", "republic of india"}


def _us_match(country: str, country_code: str) -> bool:
    return country_code.upper() == "US" or _normal(country) in {"united states", "united states of america", "usa"}


async def _india_alerts(latitude, longitude, location_name, state, district):
    now = datetime.now(timezone.utc)
    succeeded, failed, alerts = [], [], []
    headers = {"User-Agent": "ClimateShieldAI/1.0 (local weather awareness application)", "Accept": "application/rss+xml, application/xml, text/xml"}
    async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers=headers) as client:
        for authority, url in INDIA_FEEDS:
            try:
                root = await _get_xml(client, url)
                succeeded.append(authority)
                cap_nodes = _cap_nodes(root)
                for cap in cap_nodes:
                    alert = _cap_alert(cap, authority, latitude, longitude, location_name, state, district, now)
                    if alert:
                        alerts.append(alert)
                # Some RSS publishers expose CAP fields as regular RSS elements instead of embedding <cap:alert>.
                if not cap_nodes:
                    for item in _rss_items(root):
                        title, description = _rss_text(item, "title"), _rss_text(item, "description")
                        headline = _rss_text(item, "headline") or title
                        area = _rss_text(item, "areaDesc") or _rss_text(item, "category")
                        expires = _rss_text(item, "expires")
                        pub_date = _rss_text(item, "pubDate") or _rss_text(item, "sent")
                        if not _active(_rss_text(item, "onset") or pub_date, expires, now, recent=True):
                            continue
                        combined_area = _normal(f"{area} {headline} {description}")
                        tokens = [_normal(v) for v in (location_name, state, district) if v]
                        if any(t and t in combined_area for t in tokens):
                            alerts.append({"event": title or headline, "headline": headline, "description": description, "sender": authority, "authority": authority, "source": authority, "severity": _rss_text(item, "severity") or "Unknown", "starts": pub_date, "ends": expires or None, "area": area})
            except (httpx.HTTPError, ET.ParseError, ValueError):
                failed.append(authority)
    if not succeeded:
        return {"available": False, "source": " / ".join(a for a, _ in INDIA_FEEDS), "message": "Official India alert feeds could not be reached.", "checked_at": now.isoformat()}
    unique = {}
    for alert in alerts:
        unique[(alert.get("sender"), alert.get("headline"), alert.get("starts"))] = alert
    authorities = succeeded
    return {"available": True, "source": " / ".join(authorities), "checked_authorities": authorities, "failed_authorities": failed, "alerts": list(unique.values()), "message": ("Checked " + " / ".join(authorities) + ("; feed unavailable: " + " / ".join(failed) if failed else "")), "checked_at": now.isoformat()}


async def _us_alerts(latitude, longitude):
    now = datetime.now(timezone.utc)
    headers = {"User-Agent": "ClimateShieldAI/1.0 (local weather awareness application)", "Accept": "application/geo+json"}
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers=headers) as client:
            point_response = await client.get(f"https://api.weather.gov/points/{latitude:.4f},{longitude:.4f}")
            if point_response.status_code == 404:
                return {"available": True, "source": "US National Weather Service", "checked_authorities": ["US National Weather Service"], "alerts": [], "message": "Checked US National Weather Service; this point is outside its alert coverage."}
            point_response.raise_for_status()
            point = point_response.json().get("properties", {})
            if not point.get("forecast"):
                return {"available": True, "source": "US National Weather Service", "checked_authorities": ["US National Weather Service"], "alerts": [], "message": "Checked US National Weather Service; no alert coverage is listed for this point."}
            response = await client.get("https://api.weather.gov/alerts/active", params={"point": f"{latitude:.4f},{longitude:.4f}"})
            response.raise_for_status()
            alerts = []
            for feature in response.json().get("features", []):
                p = feature.get("properties", {})
                alerts.append({"event": p.get("event", "Official weather alert"), "headline": p.get("headline"), "description": p.get("description"), "sender": p.get("senderName") or "US National Weather Service", "authority": "US National Weather Service", "source": "US National Weather Service", "starts": p.get("onset"), "ends": p.get("ends") or p.get("expires"), "severity": p.get("severity"), "url": p.get("web")})
            return {"available": True, "source": "US National Weather Service", "checked_authorities": ["US National Weather Service"], "alerts": alerts, "message": "Checked US National Weather Service", "checked_at": now.isoformat()}
    except (httpx.HTTPError, ValueError, KeyError):
        return {"available": False, "source": "US National Weather Service", "message": "The US National Weather Service could not be reached."}


async def _gdacs_alerts(latitude, longitude):
    now = datetime.now(timezone.utc)
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, headers={"User-Agent": "ClimateShieldAI/1.0 (local weather awareness application)", "Accept": "application/rss+xml, application/xml, text/xml"}) as client:
            root = await _get_xml(client, GDACS_FEED)
        alerts = []
        for item in _rss_items(root):
            title = _rss_text(item, "title") or "GDACS disaster alert"
            description = _rss_text(item, "description")
            start = _rss_text(item, "fromdate") or _rss_text(item, "onset") or _rss_text(item, "pubDate")
            end = _rss_text(item, "todate") or _rss_text(item, "expires")
            if not _active(start, end, now, recent=True):
                continue
            lat = lon = None
            point = next((n.text for n in item.iter() if _local(n.tag) == "point" and n.text), None)
            if point:
                try:
                    lat, lon = map(float, point.split()[:2])
                except (ValueError, TypeError):
                    pass
            if lat is None or lon is None:
                try:
                    lat = float(_rss_text(item, "lat"))
                    lon = float(_rss_text(item, "long"))
                except (ValueError, TypeError):
                    continue
            distance = _haversine_km(latitude, longitude, lat, lon)
            if distance > 150:
                continue
            alertlevel = _rss_text(item, "alertlevel") or "Unknown"
            alerts.append({"event": title, "headline": title, "description": description, "sender": "Global Disaster Alert and Coordination System (GDACS)", "authority": "GDACS", "source": "GDACS", "severity": alertlevel.title(), "starts": start, "ends": end or None, "distance_km": round(distance, 1), "url": _rss_text(item, "link") or None})
        return {"available": True, "source": "GDACS", "checked_authorities": ["GDACS"], "alerts": alerts, "message": "Checked GDACS", "checked_at": now.isoformat()}
    except (httpx.HTTPError, ET.ParseError, ValueError):
        return {"available": False, "source": "GDACS", "message": "The GDACS global disaster alert feed could not be reached."}


@app.get("/api/alerts")
async def official_alerts(
    latitude: float,
    longitude: float,
    location_name: str = Query("", max_length=120),
    state: str = Query("", max_length=120),
    admin1: str = Query("", max_length=120),
    district: str = Query("", max_length=120),
    admin2: str = Query("", max_length=120),
    country: str = Query("", max_length=120),
    country_code: str = Query("", max_length=3),
):
    """Route verified alert lookups to regional official feeds using selected-location metadata."""
    state = state or admin1
    district = district or admin2
    if _india_match(location_name, state, country, country_code):
        return await _india_alerts(latitude, longitude, location_name, state, district)
    if _us_match(country, country_code):
        return await _us_alerts(latitude, longitude)
    return await _gdacs_alerts(latitude, longitude)


async def _weather_bundle(latitude, longitude, location_name="Selected location", state="", admin1="", district="", admin2="", country="", country_code="", allow_missing_weather=False):
    weather_result, air_result, alert_result = await asyncio.gather(
        weather_for(latitude, longitude),
        air_quality_for(latitude, longitude),
        official_alerts(latitude, longitude, location_name=location_name, state=state, admin1=admin1, district=district, admin2=admin2, country=country, country_code=country_code),
        return_exceptions=True,
    )
    if isinstance(weather_result, Exception) and not allow_missing_weather:
        raise weather_result
    weather = weather_result if isinstance(weather_result, dict) else {}
    air_quality = air_result if isinstance(air_result, dict) else {"available": False, "us_aqi": None, "european_aqi": None, "pm2_5": None, "pm10": None, "uv_index": None, "message": "Air quality data could not be retrieved."}
    alerts = alert_result if isinstance(alert_result, dict) else {"available": False, "alerts": [], "message": "Official alerts could not be retrieved."}
    risks = assess(weather, air_quality)
    return {"location": {"name": location_name, "state": state or admin1, "district": district or admin2, "country": country, "country_code": country_code, "latitude": latitude, "longitude": longitude}, "updated_at": datetime.now(timezone.utc).isoformat(), "weather": weather, "weather_available": bool(weather), "air_quality": air_quality, "risks": risks, "official_alerts": alerts}


@app.get("/api/weather")
async def weather_response(
    latitude: float,
    longitude: float,
    location_name: str = Query("Selected location", max_length=120),
    state: str = Query("", max_length=120),
    admin1: str = Query("", max_length=120),
    district: str = Query("", max_length=120),
    admin2: str = Query("", max_length=120),
    country: str = Query("", max_length=120),
    country_code: str = Query("", max_length=3),
):
    try:
        return await _weather_bundle(latitude, longitude, location_name, state, admin1, district, admin2, country, country_code)
    except httpx.HTTPError as exc:
        raise HTTPException(502, "Weather data could not be retrieved.") from exc


class AssistantRequest(BaseModel):
    question: str
    location: str = "your area"
    hazards: list[str] = Field(default_factory=list)
    weather: dict | None = None
    air_quality: dict | None = None
    official_alerts: dict | None = None
    latitude: float | None = None
    longitude: float | None = None
    state: str = ""
    admin1: str = ""
    district: str = ""
    admin2: str = ""
    country: str = ""
    country_code: str = ""


def _assistant_context(req: AssistantRequest, bundle=None):
    weather_data = (bundle or {}).get("weather") or {}
    current = weather_data.get("current") or req.weather or {}
    air = (bundle or {}).get("air_quality") or req.air_quality or {"available": False}
    hazards = bundle.get("risks", req.hazards) if bundle is not None else req.hazards
    official = (bundle or {}).get("official_alerts") or req.official_alerts or {"available": False, "alerts": []}
    hourly = weather_data.get("hourly") or {}
    daily = weather_data.get("daily") or {}
    next24 = min(24, len(hourly.get("time", [])))
    rainfall_24h = sum(v for v in hourly.get("precipitation", [])[:next24] if isinstance(v, (int, float)))
    metrics = {k: current.get(k) for k in ("temperature_2m", "apparent_temperature", "relative_humidity_2m", "precipitation", "weather_code", "pressure_msl", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m") if current.get(k) is not None}
    if next24:
        metrics["forecast_precipitation_24h_mm"] = round(rainfall_24h, 1)
        metrics["forecast_through"] = hourly.get("time", [])[next24 - 1]
    if daily.get("temperature_2m_max"):
        metrics["forecast_highs_c"] = daily.get("temperature_2m_max", [])[:7]
    return {"location": req.location, "weather_metrics": metrics, "air_quality": air, "detected_hazard_triggers": hazards, "official_alert_status": {"available": official.get("available"), "source": official.get("source"), "message": official.get("message")}, "active_official_alerts": official.get("alerts", [])}


def safety_reply(req: AssistantRequest, context=None):
    q = req.question.lower()
    context = context or _assistant_context(req)
    air_context = context.get("air_quality", {})
    if any(k in q for k in ("air quality", "aqi", "pollution", "pm2.5", "pm10", "smoke")):
        if air_context.get("available"):
            us_aqi = air_context.get("us_aqi")
            eu_aqi = air_context.get("european_aqi")
            parts = []
            if us_aqi is not None:
                parts.append(f"US AQI {us_aqi}")
            if eu_aqi is not None:
                parts.append(f"European AQI {eu_aqi}")
            if air_context.get("pm2_5") is not None:
                parts.append(f"PM2.5 {air_context['pm2_5']} µg/m³")
            if air_context.get("pm10") is not None:
                parts.append(f"PM10 {air_context['pm10']} µg/m³")
            answer = f"The latest reading for {req.location} is " + (", ".join(parts) or "partly unavailable") + f" at {air_context.get('time') or 'the latest reported time'}. "
            if (isinstance(us_aqi, (int, float)) and us_aqi >= 151) or (isinstance(eu_aqi, (int, float)) and eu_aqi >= 80):
                answer += "Consider reducing strenuous outdoor activity and keep windows closed if outdoor air appears smoky. People with asthma or heart and lung conditions should follow their clinician’s plan and local air-quality advice."
            else:
                answer += "Sensitive groups should follow local public-health advice and reduce prolonged strenuous outdoor activity if symptoms develop."
        else:
            answer = "Current air-quality readings are unavailable for this location. Check your local environmental authority for the latest AQI and limit exposure if you notice smoke or irritation."
    elif any(k in q for k in ("uv", "sunburn", "sun exposure")):
        uv = air_context.get("uv_index")
        answer = f"The latest UV index for {req.location} is {uv} at {air_context.get('time') or 'the latest reported time'}. " if uv is not None else "The current UV index is unavailable. "
        if isinstance(uv, (int, float)) and uv >= 6:
            answer += "Use shade, protective clothing, sunglasses, and broad-spectrum sunscreen; reduce direct sun exposure around midday."
        else:
            answer += "Use sun protection during prolonged outdoor exposure and follow local health guidance."
    elif any(k in q for k in ("flood", "water rise", "inundat")):
        answer = "If flooding is possible, move to higher ground early and follow evacuation instructions. Never walk, swim, or drive through floodwater; turn around and find another route. Keep a charged phone and essential medicines with you."
    elif any(k in q for k in ("heat", "hot", "heatwave", "sun")):
        answer = "Drink water regularly and avoid waiting until you feel thirsty. Limit strenuous outdoor activity during the hottest part of the day, wear light clothing, and check on older adults, children, and anyone without reliable cooling. Seek medical help for confusion, fainting, or very hot skin."
    elif any(k in q for k in ("thunder", "lightning", "storm")):
        answer = "Go indoors when thunder is heard and stay away from windows, plumbing, and plugged-in equipment. Avoid open areas, water, and isolated trees. Wait at least 30 minutes after the last thunder before going back outside."
    elif any(k in q for k in ("wind", "gust", "hurricane", "cyclone")):
        answer = "Stay indoors and away from windows during strong winds. Bring in or secure loose outdoor items, avoid trees and power lines, and do not go out to inspect damage until authorities say it is safe."
    elif any(k in q for k in ("rain", "precipitation", "wet")):
        answer = "Allow extra travel time, use rain protection, and check local road conditions. Avoid underpasses and fast-moving or standing floodwater. If thunder starts, move indoors and follow official local instructions."
    elif any(k in q for k in ("cold", "snow", "ice", "freeze")):
        answer = "Dress in warm layers, limit time outdoors, and check travel conditions before leaving. Keep devices charged and follow local advice about icy roads, heating safety, and vulnerable neighbors."
    else:
        answer = "Check the latest forecast and any official local alerts before making plans. Keep a charged phone, water, and essential medicines accessible. For immediate danger, contact local emergency services and follow their instructions."
    metrics = context.get("weather_metrics", {})
    current_parts = []
    if metrics.get("temperature_2m") is not None:
        current_parts.append(f"{metrics['temperature_2m']}°C")
    if metrics.get("relative_humidity_2m") is not None:
        current_parts.append(f"{metrics['relative_humidity_2m']}% humidity")
    if metrics.get("wind_speed_10m") is not None:
        gust = metrics.get("wind_gusts_10m")
        current_parts.append(f"wind {metrics['wind_speed_10m']} km/h" + (f" (gusts {gust} km/h)" if gust is not None else ""))
    if metrics.get("forecast_precipitation_24h_mm") is not None:
        current_parts.append(f"{metrics['forecast_precipitation_24h_mm']} mm forecast through {metrics.get('forecast_through', 'the next 24 hours')}")
    if current_parts:
        answer += f" Live conditions for {req.location}: " + "; ".join(current_parts) + "."
    air = context.get("air_quality", {})
    if air.get("available"):
        answer += f" Latest air-quality data: US AQI {air.get('us_aqi', 'unavailable')}, European AQI {air.get('european_aqi', 'unavailable')}, PM2.5 {air.get('pm2_5', 'unavailable')} µg/m³, PM10 {air.get('pm10', 'unavailable')} µg/m³, UV index {air.get('uv_index', 'unavailable')} (at {air.get('time') or 'the latest reading'})."
    else:
        answer += " Current AQI and UV data could not be retrieved."
    hazards = context.get("detected_hazard_triggers", [])
    if hazards:
        entries = []
        for hazard in hazards[:5]:
            if isinstance(hazard, dict):
                entries.append(f"{hazard.get('type', 'Risk')}: {hazard.get('detail', 'trigger details unavailable')}")
            else:
                entries.append(str(hazard))
        answer += " Forecast-based trigger checks: " + "; ".join(entries) + ". These are automated assessments, not official warnings."
    official = context.get("active_official_alerts", [])
    if official:
        entries = [f"{a.get('event') or 'Warning'} by {a.get('authority') or a.get('sender') or 'official authority'} ({a.get('severity') or 'severity not stated'}; {a.get('headline') or a.get('description') or 'follow the source bulletin'}; valid until {a.get('ends') or 'not stated'})" for a in official[:3]]
        answer += " Verified official warnings currently reported: " + "; ".join(entries) + ". Follow the issuing authority's instructions."
    elif context.get("official_alert_status", {}).get("available"):
        answer += f" No active official warnings were returned by {context['official_alert_status'].get('source') or 'the checked authority'} for this area."
    else:
        answer += " Official alert status could not be confirmed; check your local authority directly."
    return answer


@app.post("/api/assistant")
async def assistant(req: AssistantRequest):
    key = os.getenv("LLM_API_KEY")
    endpoint = os.getenv("LLM_API_URL", "https://api.openai.com/v1/chat/completions")
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    bundle = None
    if req.latitude is not None and req.longitude is not None:
        try:
            bundle = await _weather_bundle(req.latitude, req.longitude, req.location, req.state, req.admin1, req.district, req.admin2, req.country, req.country_code, allow_missing_weather=True)
        except httpx.HTTPError:
            bundle = None
    context = _assistant_context(req, bundle)
    if key:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(endpoint, headers={"Authorization": f"Bearer {key}"}, json={"model": model, "temperature": 0.3, "messages": [{"role": "system", "content": "Give concise, practical weather safety advice based on the supplied live conditions, air-quality values, exact forecast hazard triggers, and verified active alerts. Never claim a forecast is guaranteed or an AI assessment is an official alert. Clearly distinguish official alerts from automated risks. If an official warning is present, prioritize its issuing authority and validity period; never fabricate warnings. For immediate danger, tell the user to follow local emergency services."}, {"role": "user", "content": f"Live context (JSON): {json.dumps(context, ensure_ascii=False, default=str)}\nQuestion: {req.question}"}]})
                response.raise_for_status()
                return {"answer": response.json()["choices"][0]["message"]["content"], "powered_by": model}
        except (httpx.HTTPError, KeyError, ValueError):
            pass
    return {"answer": safety_reply(req, context), "powered_by": "built-in safety guidance", "context": context}


class PDFReportRequest(BaseModel):
    latitude: float
    longitude: float
    location: str = "Selected location"
    state: str = ""
    admin1: str = ""
    district: str = ""
    admin2: str = ""
    country: str = ""
    country_code: str = ""


@app.get("/api/report.pdf")
@app.post("/api/report.pdf")
async def report_pdf(
    latitude: float | None = None,
    longitude: float | None = None,
    location: str = Query("Selected location", max_length=120),
    state: str = Query("", max_length=120),
    admin1: str = Query("", max_length=120),
    district: str = Query("", max_length=120),
    admin2: str = Query("", max_length=120),
    country: str = Query("", max_length=120),
    country_code: str = Query("", max_length=3),
    report: PDFReportRequest | None = Body(default=None),
):
    if report:
        latitude, longitude, location = report.latitude, report.longitude, report.location
        state, admin1 = report.state, report.admin1
        district, admin2 = report.district, report.admin2
        country, country_code = report.country, report.country_code
    if latitude is None or longitude is None:
        raise HTTPException(422, "Latitude and longitude are required.")
    try:
        data = await weather_for(latitude, longitude)
    except httpx.HTTPError as exc:
        raise HTTPException(502, "Weather data could not be retrieved; no report was generated.") from exc
    air_quality = await air_quality_for(latitude, longitude)
    current, daily = data.get("current", {}), data.get("daily", {})
    risks = assess(data, air_quality)
    now = datetime.now().astimezone()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=.65*inch, leftMargin=.65*inch, topMargin=.62*inch, bottomMargin=.62*inch)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Brand", parent=styles["Title"], textColor=colors.HexColor("#20674d"), fontName="Helvetica-Bold", fontSize=22, alignment=0, spaceAfter=4))
    styles.add(ParagraphStyle(name="Muted", parent=styles["Normal"], textColor=colors.HexColor("#6c7c72"), fontSize=9, leading=14))
    styles.add(ParagraphStyle(name="HeadGreen", parent=styles["Heading2"], textColor=colors.HexColor("#20674d"), fontSize=13, spaceBefore=16, spaceAfter=8))
    story = [Paragraph("ClimateShield AI", styles["Brand"]), Paragraph("LOCAL WEATHER &amp; RISK REPORT", styles["Muted"]), Spacer(1, 12), Paragraph(f"<b>Location:</b> {escape(location)}", styles["Normal"]), Paragraph(f"<b>Report generated:</b> {now.strftime('%Y-%m-%d %H:%M %Z')}", styles["Normal"]), Paragraph("Weather information from Open-Meteo. Forecasts are estimates, not guarantees.", styles["Muted"]), Paragraph("Current conditions", styles["HeadGreen"])]
    weather_code = current.get("weather_code")
    descriptors = {0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog", 51: "Drizzle", 61: "Rain", 65: "Heavy rain", 71: "Snow", 80: "Rain showers", 95: "Thunderstorm"}
    rows = [["Measure", "Current value"], ["Temperature", f"{current.get('temperature_2m', '—')} °C"], ["Feels like", f"{current.get('apparent_temperature', '—')} °C"], ["Conditions", descriptors.get(weather_code, "Variable")], ["Humidity", f"{current.get('relative_humidity_2m', '—')}%"], ["Precipitation", f"{current.get('precipitation', '—')} mm"], ["Wind", f"{current.get('wind_speed_10m', '—')} km/h at {current.get('wind_direction_10m', '—')}°"], ["Pressure", f"{current.get('pressure_msl', '—')} hPa"]]
    table = Table(rows, colWidths=[2.1*inch, 4.8*inch]); table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edf5ef")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#315b41")), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), .35, colors.HexColor("#e1e8e2")), ("FONTSIZE", (0, 0), (-1, -1), 9), ("BOTTOMPADDING", (0, 0), (-1, -1), 7), ("TOPPADDING", (0, 0), (-1, -1), 7)])); story.append(table)
    story.append(Paragraph("Air quality &amp; UV", styles["HeadGreen"]))
    if air_quality.get("available"):
        aq_rows = [["Measure", "Latest value"], ["US AQI", air_quality.get("us_aqi") if air_quality.get("us_aqi") is not None else "Unavailable"], ["European AQI", air_quality.get("european_aqi") if air_quality.get("european_aqi") is not None else "Unavailable"], ["PM2.5", f"{air_quality['pm2_5']} µg/m³" if air_quality.get("pm2_5") is not None else "Unavailable"], ["PM10", f"{air_quality['pm10']} µg/m³" if air_quality.get("pm10") is not None else "Unavailable"], ["UV index", air_quality.get("uv_index") if air_quality.get("uv_index") is not None else "Unavailable"], ["Reading time", air_quality.get("time") or "Not provided"]]
        aq_table = Table(aq_rows, colWidths=[2.1*inch, 4.8*inch]); aq_table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edf5ef")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#315b41")), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), .35, colors.HexColor("#e1e8e2")), ("FONTSIZE", (0, 0), (-1, -1), 9), ("BOTTOMPADDING", (0, 0), (-1, -1), 6), ("TOPPADDING", (0, 0), (-1, -1), 6)])); story.append(aq_table)
    else:
        story.append(Paragraph(escape(air_quality.get("message") or "Air quality data was unavailable when this report was generated."), styles["Normal"]))
    story.append(Paragraph("7-day outlook", styles["HeadGreen"]))
    forecast_rows = [["Day", "Conditions", "Low", "High", "Rain chance", "Precipitation"]]
    for i, day in enumerate(daily.get("time", [])):
        code = daily.get("weather_code", [None]*7)[i]
        forecast_rows.append([day, descriptors.get(code, weatherTextName(code)), f"{daily.get('temperature_2m_min', ['—']*7)[i]}°", f"{daily.get('temperature_2m_max', ['—']*7)[i]}°", f"{daily.get('precipitation_probability_max', [0]*7)[i]}%", f"{daily.get('precipitation_sum', [0]*7)[i]} mm"])
    ft = Table(forecast_rows, colWidths=[.83*inch, 1.38*inch, .56*inch, .56*inch, .86*inch, .95*inch]); ft.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edf5ef")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#315b41")), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), .35, colors.HexColor("#e1e8e2")), ("FONTSIZE", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 6), ("TOPPADDING", (0, 0), (-1, -1), 6)])); story.append(ft)
    story.append(Paragraph("Automated risk assessment", styles["HeadGreen"]))
    if risks:
        for risk in risks:
            story.append(Paragraph(f"<b>{risk['type']} — {risk['level']} risk</b><br/>{risk['detail']}<br/><b>Precautions:</b> {risk['precautions']}", styles["Normal"]))
            story.append(Spacer(1, 6))
    else:
        story.append(Paragraph("No elevated risks detected by the configured forecast thresholds for the next 24 hours. This does not rule out local hazards.", styles["Normal"]))
    official = await official_alerts(latitude, longitude, location_name=location, state=state, admin1=admin1, district=district, admin2=admin2, country=country, country_code=country_code)
    story.append(Paragraph("Official warnings", styles["HeadGreen"]))
    if official.get("available") and official.get("alerts"):
        for alert in official["alerts"]:
            validity = " · ".join(filter(None, [f"Starts {alert.get('starts')}" if alert.get("starts") else "", f"Until {alert.get('ends')}" if alert.get("ends") else ""]))
            story.append(Paragraph(f"<b>{escape(alert.get('event') or 'Official weather alert')}</b> — {escape(alert.get('sender') or official.get('source') or 'Issuing authority')}", styles["Normal"]))
            story.append(Paragraph(escape(alert.get("headline") or alert.get("description") or "See the issuing authority for details.").replace("\n", "<br/>"), styles["Muted"]))
            if validity:
                story.append(Paragraph(escape(validity), styles["Muted"]))
            story.append(Spacer(1, 6))
    elif official.get("available"):
        story.append(Paragraph(f"No active official alerts were reported by {escape(official.get('source', 'the supported source'))} when checked.", styles["Normal"]))
    else:
        story.append(Paragraph(f"Official alert data is unavailable: {escape(official.get('message', 'the supported source could not be reached'))}", styles["Normal"]))
    story.extend([Spacer(1, 14), Paragraph("Preparedness guidance is informational and not a substitute for instructions from local emergency services or government authorities.", styles["Muted"])])
    doc.build(story); buffer.seek(0)
    with db() as conn:
        conn.execute("INSERT INTO report_history(location,latitude,longitude,generated_at,summary_json) VALUES(?,?,?,?,?)", (location, latitude, longitude, now.isoformat(), json.dumps({"current": current, "air_quality": air_quality, "risks": risks, "official_alerts": official, "location_meta": {"state": state or admin1, "district": district or admin2, "country": country, "country_code": country_code}}, ensure_ascii=False)))
    filename = "climateshield-report.pdf"
    return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": f"attachment; filename={filename}"})


def weatherTextName(code):
    return {0: "Clear", 1: "Mostly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog", 51: "Drizzle", 61: "Rain", 63: "Rain", 65: "Heavy rain", 71: "Snow", 73: "Snow", 75: "Heavy snow", 80: "Showers", 95: "Thunderstorm"}.get(code, "Variable")


@app.get("/api/reports")
def reports():
    with db() as conn:
        records = conn.execute("SELECT id, location, latitude, longitude, generated_at, summary_json FROM report_history ORDER BY id DESC LIMIT 10").fetchall()
    rank = {"Low": 0, "Moderate": 1, "High": 2, "Severe": 3}
    result = []
    for record in records:
        try:
            summary = json.loads(record["summary_json"] or "{}")
        except (TypeError, ValueError):
            summary = {}
        current = summary.get("current") or {}
        hazards = summary.get("risks") or []
        location_meta = summary.get("location_meta") or {}
        risk_level = max((h.get("level", "Low") for h in hazards if isinstance(h, dict)), key=lambda level: rank.get(level, 0), default="Low")
        hazards_summary = [f"{h.get('type', 'Risk')}: {h.get('detail', '')}".rstrip(": ") for h in hazards if isinstance(h, dict)]
        result.append({"id": record["id"], "location_name": record["location"], "latitude": record["latitude"], "longitude": record["longitude"], "state": location_meta.get("state", ""), "district": location_meta.get("district", ""), "country": location_meta.get("country", ""), "country_code": location_meta.get("country_code", ""), "timestamp": record["generated_at"], "risk_level": risk_level, "temperature": current.get("temperature_2m"), "hazards_summary": hazards_summary})
    return {"reports": result}


@app.get("/api/reports/history")
def report_history(limit: int = Query(20, ge=1, le=100)):
    with db() as conn:
        records = conn.execute("SELECT id,location,latitude,longitude,generated_at FROM report_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return {"reports": [dict(row) for row in records]}
