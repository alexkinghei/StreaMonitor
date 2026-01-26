import os.path
import re
import environ


env = environ.Env()
if os.path.exists('.env'):
    environ.Env.read_env('.env')


def parse_size(size_str):
    """
    Parse size string to bytes.
    Supports formats: "800M", "1G", "838860800", etc.
    Returns None if size_str is None, empty, or "None" (case-insensitive).
    """
    if not size_str:
        return None
    
    size_str = str(size_str).strip()
    
    # Handle explicit "None" string
    if size_str.upper() == 'NONE':
        return None
    
    # Try to parse as integer first (bytes)
    try:
        return int(size_str)
    except ValueError:
        pass
    
    # Parse string format with units (K, M, G, T)
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([KMGTkmgt]?)$', size_str)
    if match:
        value = float(match.group(1))
        unit = match.group(2).upper() if match.group(2) else ''
        
        multipliers = {
            '': 1,
            'K': 1024,
            'M': 1024 * 1024,
            'G': 1024 * 1024 * 1024,
            'T': 1024 * 1024 * 1024 * 1024
        }
        
        return int(value * multipliers.get(unit, 1))
    
    return None


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

# Specify the segment time in seconds
# If None, the video will be downloaded as a single file
# Example:
# 5 minutes
# SEGMENT_TIME = 300
# 1 hour
# SEGMENT_TIME = 3600
# Also see the ffmpeg documentation for the segment_time option
# You can specify time in hh:mm:ss format
# Example:
# 1 hour
# SEGMENT_TIME = '1:00:00'
SEGMENT_TIME = env.str("STRMNTR_SEGMENT_TIME", None)

# Specify the segment size in bytes
# If None, the video will be segmented by time (if SEGMENT_TIME is set) or as a single file
# If both SEGMENT_SIZE and SEGMENT_TIME are set, SEGMENT_SIZE takes priority
# Example:
# 800 MB
# SEGMENT_SIZE = 838860800
# 1 GB
# SEGMENT_SIZE = 1073741824
# You can also use string format with units (M for MB, G for GB)
# Example:
# 800 MB
# SEGMENT_SIZE = '800M'
# 1 GB
# SEGMENT_SIZE = '1G'
SEGMENT_SIZE_STR = env.str("STRMNTR_SEGMENT_SIZE", "800M")
SEGMENT_SIZE = parse_size(SEGMENT_SIZE_STR) if SEGMENT_SIZE_STR else None

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
WEBSERVER_SKIN = env.str("STRMNTR_SKIN", "shaftoverflow")

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
