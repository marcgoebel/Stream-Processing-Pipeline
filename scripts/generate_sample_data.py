"""
Generate realistic IoT environmental sensor data for the municipality scenario.

Simulates sensors deployed across a city measuring:
- temperature (°C)
- humidity (%)
- co (carbon monoxide, ppm)
- smoke (ppm)
- lpg (liquid petroleum gas, ppm)
- noise (dB) — future sensor type
- light (boolean)
- motion (boolean)

Devices are placed at different city locations with distinct environmental profiles.
"""

import csv
import random
import time
import os
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

# Sensor locations across the city
DEVICES = {
    "sensor-001": {"location": "city-center",     "lat": 48.1351, "lon": 11.5820},
    "sensor-002": {"location": "industrial-zone",  "lat": 48.1450, "lon": 11.6100},
    "sensor-003": {"location": "residential-north", "lat": 48.1680, "lon": 11.5550},
    "sensor-004": {"location": "park-area",         "lat": 48.1520, "lon": 11.5400},
    "sensor-005": {"location": "highway-bridge",    "lat": 48.1200, "lon": 11.6050},
}

# Environmental profiles per location (base values + noise ranges)
PROFILES = {
    "city-center":      {"temp": (18, 5), "humidity": (50, 15), "co": (0.005, 0.003), "smoke": (0.02, 0.01), "lpg": (0.007, 0.003), "noise": (65, 15)},
    "industrial-zone":  {"temp": (20, 6), "humidity": (45, 12), "co": (0.012, 0.008), "smoke": (0.05, 0.03), "lpg": (0.015, 0.008), "noise": (75, 10)},
    "residential-north":{"temp": (17, 4), "humidity": (55, 10), "co": (0.003, 0.002), "smoke": (0.01, 0.005),"lpg": (0.004, 0.002), "noise": (45, 10)},
    "park-area":        {"temp": (16, 5), "humidity": (60, 12), "co": (0.002, 0.001), "smoke": (0.005,0.003),"lpg": (0.002, 0.001), "noise": (35, 8)},
    "highway-bridge":   {"temp": (19, 5), "humidity": (48, 10), "co": (0.010, 0.006), "smoke": (0.03, 0.02), "lpg": (0.010, 0.005), "noise": (80, 12)},
}


def generate_reading(device_id: str, device_info: dict, timestamp: datetime) -> dict:
    """Generate a single sensor reading with realistic environmental noise."""
    profile = PROFILES[device_info["location"]]

    # Time-of-day effects (temperature higher at noon, noise lower at night)
    hour = timestamp.hour
    time_temp_modifier = 3 * (1 - abs(hour - 14) / 14)  # peaks at 2 PM
    time_noise_modifier = -20 if (hour < 6 or hour > 22) else 0

    def noisy(base, spread):
        return max(0, random.gauss(base, spread))

    return {
        "ts": timestamp.isoformat(),
        "device_id": device_id,
        "location": device_info["location"],
        "lat": device_info["lat"],
        "lon": device_info["lon"],
        "temperature": round(noisy(profile["temp"][0] + time_temp_modifier, profile["temp"][1]), 2),
        "humidity": round(min(100, noisy(profile["humidity"][0], profile["humidity"][1])), 2),
        "co": round(noisy(profile["co"][0], profile["co"][1]), 6),
        "smoke": round(noisy(profile["smoke"][0], profile["smoke"][1]), 6),
        "lpg": round(noisy(profile["lpg"][0], profile["lpg"][1]), 6),
        "noise_db": round(noisy(profile["noise"][0] + time_noise_modifier, profile["noise"][1]), 1),
        "light": hour >= 6 and hour <= 20,
        "motion": random.random() > (0.3 if 6 <= hour <= 22 else 0.85),
    }


def main():
    output_path = os.path.join(os.path.dirname(__file__), "..", "data", "iot_telemetry_data.csv")

    # Generate 7 days of data, one reading per sensor every 30 seconds
    start_time = datetime(2024, 7, 1, 0, 0, 0)
    interval = timedelta(seconds=30)
    duration = timedelta(days=7)

    total_readings = int((duration / interval) * len(DEVICES))
    print(f"Generating {total_readings:,} sensor readings over 7 days...")

    fieldnames = [
        "ts", "device_id", "location", "lat", "lon",
        "temperature", "humidity", "co", "smoke", "lpg",
        "noise_db", "light", "motion"
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        current_time = start_time
        count = 0
        while current_time < start_time + duration:
            for device_id, device_info in DEVICES.items():
                reading = generate_reading(device_id, device_info, current_time)
                writer.writerow(reading)
                count += 1

            current_time += interval

            if count % 50000 == 0:
                print(f"  {count:,} readings generated...")

    print(f"Done! {count:,} readings saved to {output_path}")
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"File size: {file_size_mb:.1f} MB")


if __name__ == "__main__":
    main()
