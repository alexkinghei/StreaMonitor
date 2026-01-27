import os.path
import environ


env = environ.Env()
if os.path.exists('.env'):
    environ.Env.read_env('.env')


DOWNLOADS_DIR = env.str("STRMNTR_DOWNLOAD_DIR", "downloads")
MIN_FREE_DISK_PERCENT = env.float("STRMNTR_MIN_FREE_SPACE", 5.0)  # in %
DEBUG = env.bool("STRMNTR_DEBUG", False)

# The camsoda bot ignores this setting in favor of a chrome useragent generated with the fake-useragent library
HTTP_USER_AGENT = env.str("STRMNTR_USER_AGENT", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0")

# Specify the full path to the ffmpeg binary. By default, ffmpeg found on PATH is used.
FFMPEG_PATH = env.str("STRMNTR_FFMPEG_PATH", 'ffmpeg')

# You can enter a number to select a specific height.
# Use a huge number here and closest match to get the highest resolution variant
# Eg: 240, 360, 480, 720, 1080, 1440, 99999
WANTED_RESOLUTION = env.int("STRMNTR_RESOLUTION", 1080)

# Specify match type when specified height
# Possible values: exact, exact_or_least_higher, exact_or_highest_lower, closest
# Beware of the exact policy. Nothing gets downloaded if the wanted resolution is not available
WANTED_RESOLUTION_PREFERENCE = env.str("STRMNTR_RESOLUTION_PREF", 'closest')

# Specify output container here
# Suggested values are 'mkv' or 'mp4'
CONTAINER = env.str("STRMNTR_CONTAINER", 'mp4')

# Add auto-generated VR format suffix to files
VR_FORMAT_SUFFIX = env.bool("STRMNTR_VR_FORMAT_SUFFIX", True)

# Set ffmpeg readrate to whatever works for you.
# Usually this should be either 0, 1 or 1.3 depending on the network
# Setting it to 0 can result in very fragmented recordings.
# 1 can result in skipped segments
# 1.3 should be the sweet spot but use what works
FFMPEG_READRATE = env.int("STRMNTR_FFMPEG_READRATE", 1.3)

# Specify the segment size in bytes
# If None, the video will be downloaded as a single file
# You can specify size in bytes, or with units (K, M, G)
# Example:
# 800M (800 MB)
# SEGMENT_SIZE = "800M"
# 1G (1 GB)
# SEGMENT_SIZE = "1G"
# 838860800 (800 MB in bytes)
# SEGMENT_SIZE = "838860800"
# Also see the ffmpeg documentation for the segment_size option
# Note: For live streaming, segment_size may not work as expected with some formats.
# You may need to use segment_time instead, or ensure the container format supports size-based segmentation.
SEGMENT_SIZE = env.str("STRMNTR_SEGMENT_SIZE", None)


def parse_segment_size(size_str):
    """
    Parse segment size string to bytes.
    Supports formats: "800M", "1G", "838860800", etc.
    Returns None if size_str is None, or the size in bytes as string.
    """
    if size_str is None:
        return None
    
    size_str = size_str.strip().upper()
    if not size_str:
        return None
    
    # Check if it ends with a unit
    multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
    
    for unit, multiplier in multipliers.items():
        if size_str.endswith(unit):
            try:
                number = float(size_str[:-1])
                return str(int(number * multiplier))
            except ValueError:
                return None
    
    # If no unit, assume bytes
    try:
        return str(int(size_str))
    except ValueError:
        return None

# HTTP Manager configuration

# Bind address for the web server
# 0.0.0.0 for remote access from all host
WEBSERVER_HOST = env.str("STRMNTR_HOST", "127.0.0.1")
WEBSERVER_PORT = env.int("STRMNTR_PORT", 5000)

# Web UI skin
# Available options:
# - kseen715 - 2nd skin, currently broken
# - truck-kun (default) - 3rd skin, row oriented
# - shaftoverflow - 4th skin, card layout, links in menus
WEBSERVER_SKIN = env.str("STRMNTR_SKIN", "truck-kun")

# set frequency in seconds of how often the streamer list will update
WEB_LIST_FREQUENCY = env.int("STRMNTR_LIST_FREQ", 30)

# set frequency in seconds of how often the streamer's status will update on the recording page
WEB_STATUS_FREQUENCY = env.int("STRMNTR_STATUS_FREQ", 5)

# set theater_mode
WEB_THEATER_MODE = env.bool("STRMNTR_THEATER_MODE", False)

# confirm deletes, default to mobile-only.
# set to empty string to disable
# set to "MOBILE" to explicitly confirm deletes only on mobile
# set to any other non-falsy value to always check
WEB_CONFIRM_DELETES = env.str("STRMNTR_CONFIRM_DEL", "MOBILE")

# Password for the web server
# If empty no auth required, else username admin and choosen password
WEBSERVER_PASSWORD = env.str("STRMNTR_PASSWORD", "")
