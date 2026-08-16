# ableton_mcp_server.py
from mcp.server.fastmcp import FastMCP, Context
import socket
import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Any, List, Union

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AbletonMCPServer")

@dataclass
class AbletonConnection:
    host: str
    port: int
    sock: socket.socket = None
    
    def connect(self) -> bool:
        """Connect to the Ableton Remote Script socket server"""
        if self.sock:
            return True
            
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            logger.info(f"Connected to Ableton at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Ableton: {str(e)}")
            self.sock = None
            return False
    
    def disconnect(self):
        """Disconnect from the Ableton Remote Script"""
        if self.sock:
            try:
                self.sock.close()
            except Exception as e:
                logger.error(f"Error disconnecting from Ableton: {str(e)}")
            finally:
                self.sock = None

    def receive_full_response(self, sock, buffer_size=8192):
        """Receive the complete response, potentially in multiple chunks"""
        chunks = []
        sock.settimeout(15.0)  # Increased timeout for operations that might take longer
        
        try:
            while True:
                try:
                    chunk = sock.recv(buffer_size)
                    if not chunk:
                        if not chunks:
                            raise Exception("Connection closed before receiving any data")
                        break
                    
                    chunks.append(chunk)
                    
                    # Check if we've received a complete JSON object
                    try:
                        data = b''.join(chunks)
                        json.loads(data.decode('utf-8'))
                        logger.info(f"Received complete response ({len(data)} bytes)")
                        return data
                    except json.JSONDecodeError:
                        # Incomplete JSON, continue receiving
                        continue
                except socket.timeout:
                    logger.warning("Socket timeout during chunked receive")
                    break
                except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
                    logger.error(f"Socket connection error during receive: {str(e)}")
                    raise
        except Exception as e:
            logger.error(f"Error during receive: {str(e)}")
            raise
            
        # If we get here, we either timed out or broke out of the loop
        if chunks:
            data = b''.join(chunks)
            logger.info(f"Returning data after receive completion ({len(data)} bytes)")
            try:
                json.loads(data.decode('utf-8'))
                return data
            except json.JSONDecodeError:
                raise Exception("Incomplete JSON response received")
        else:
            raise Exception("No data received")

    def send_command(self, command_type: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Send a command to Ableton and return the response"""
        if not self.sock and not self.connect():
            raise ConnectionError("Not connected to Ableton")
        
        command = {
            "type": command_type,
            "params": params or {}
        }
        
        # Check if this is a state-modifying command
        is_modifying_command = command_type in [
            "create_midi_track", "create_audio_track", "set_track_name",
            "create_clip", "add_notes_to_clip", "set_clip_name",
            "set_tempo", "fire_clip", "stop_clip", "set_device_parameter",
            "start_playback", "stop_playback", "load_instrument_or_effect",
            "set_song_time", "set_arrangement_loop", "jump_to_cue",
            "create_cue_point", "delete_cue_point",
            "create_arrangement_clip", "create_arrangement_audio_clip",
            "duplicate_to_arrangement", "delete_arrangement_clip",
            "set_arrangement_clip_property",
            "set_view", "control_arrangement_view",
            "manage_clip_automation",
            "add_notes_to_arrangement_clip",
            "set_device_parameter", "set_device_enabled",
            "delete_device", "navigate_preset",
            "set_track_volume", "set_track_panning",
        ]
        
        try:
            logger.info(f"Sending command: {command_type} with params: {params}")
            
            # Send the command
            self.sock.sendall(json.dumps(command).encode('utf-8'))
            logger.info(f"Command sent, waiting for response...")
            
            # For state-modifying commands, add a small delay to give Ableton time to process
            if is_modifying_command:
                import time
                time.sleep(0.1)  # 100ms delay
            
            # Set timeout based on command type
            timeout = 15.0 if is_modifying_command else 10.0
            self.sock.settimeout(timeout)
            
            # Receive the response
            response_data = self.receive_full_response(self.sock)
            logger.info(f"Received {len(response_data)} bytes of data")
            
            # Parse the response
            response = json.loads(response_data.decode('utf-8'))
            logger.info(f"Response parsed, status: {response.get('status', 'unknown')}")
            
            if response.get("status") == "error":
                logger.error(f"Ableton error: {response.get('message')}")
                raise Exception(response.get("message", "Unknown error from Ableton"))
            
            # For state-modifying commands, add another small delay after receiving response
            if is_modifying_command:
                import time
                time.sleep(0.1)  # 100ms delay
            
            return response.get("result", {})
        except socket.timeout:
            logger.error("Socket timeout while waiting for response from Ableton")
            self.sock = None
            raise Exception("Timeout waiting for Ableton response")
        except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
            logger.error(f"Socket connection error: {str(e)}")
            self.sock = None
            raise Exception(f"Connection to Ableton lost: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Ableton: {str(e)}")
            if 'response_data' in locals() and response_data:
                logger.error(f"Raw response (first 200 bytes): {response_data[:200]}")
            self.sock = None
            raise Exception(f"Invalid response from Ableton: {str(e)}")
        except Exception as e:
            logger.error(f"Error communicating with Ableton: {str(e)}")
            self.sock = None
            raise Exception(f"Communication error with Ableton: {str(e)}")

@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[Dict[str, Any]]:
    """Manage server startup and shutdown lifecycle"""
    try:
        logger.info("AbletonMCP server starting up")
        
        try:
            ableton = get_ableton_connection()
            logger.info("Successfully connected to Ableton on startup")
        except Exception as e:
            logger.warning(f"Could not connect to Ableton on startup: {str(e)}")
            logger.warning("Make sure the Ableton Remote Script is running")
        
        yield {}
    finally:
        global _ableton_connection
        if _ableton_connection:
            logger.info("Disconnecting from Ableton on shutdown")
            _ableton_connection.disconnect()
            _ableton_connection = None
        _invalidate_external_plugin_cache()
        logger.info("AbletonMCP server shut down")

# Create the MCP server with lifespan support
mcp = FastMCP(
    "AbletonMCP",
    instructions="Ableton Live integration through the Model Context Protocol",
    lifespan=server_lifespan
)

# ── Index conversion helpers ─────────────────────────────────────
#
# Convention: every MCP tool exposes **1-based** indices to callers.
# The Remote Script expects **0-based** indices.  These helpers
# enforce the rule in one place so individual tools stay simple.


def _to_zero_based(index: int, field_name: str = "index") -> int:
    """Convert a required 1-based MCP index to 0-based for the Remote Script.

    Raises ValueError when the caller passes 0 or a negative value.
    """
    if index < 1:
        raise ValueError(
            f"{field_name} must be >= 1 (1-based), got {index}"
        )
    return index - 1


def _optional_to_zero_based(index: int, field_name: str = "index") -> int | None:
    """Convert an optional 1-based index to 0-based.

    Returns None when *index* is 0 (meaning "not specified").
    Raises ValueError for negative values.
    """
    if index < 0:
        raise ValueError(
            f"{field_name} must be >= 0 (0 = unset, 1+ = 1-based), got {index}"
        )
    if index == 0:
        return None
    return index - 1


# Parameter normalization utilities


def normalize_param(value: float, min_val: float, max_val: float) -> float:
    """Normalize a raw parameter value to 0.0-1.0 range.

    Parameters:
    - value: raw parameter value
    - min_val: parameter minimum
    - max_val: parameter maximum

    Returns normalized value clamped to 0.0-1.0.
    """
    if max_val == min_val:
        return 0.0
    normalized = (value - min_val) / (max_val - min_val)
    return max(0.0, min(1.0, normalized))


def denormalize_param(normalized: float, min_val: float, max_val: float) -> float:
    """Convert a normalized 0.0-1.0 value to raw parameter range.

    Parameters:
    - normalized: value in 0.0-1.0 range
    - min_val: parameter minimum
    - max_val: parameter maximum

    Returns raw value. Input is clamped to 0.0-1.0 before conversion.
    """
    clamped = max(0.0, min(1.0, normalized))
    return min_val + clamped * (max_val - min_val)


# Bar/beat conversion utilities

def bar_to_beat(bar: int, numerator: int = 4, denominator: int = 4) -> float:
    """Convert a 1-based bar number to a beat position.

    Parameters:
    - bar: 1-based bar number (bar 1 = beat 0)
    - numerator: time signature numerator (e.g., 4 in 4/4)
    - denominator: time signature denominator (e.g., 4 in 4/4)
    """
    return (bar - 1) * numerator * (4 / denominator)


def beat_to_bar(beat: float, numerator: int = 4, denominator: int = 4) -> int:
    """Convert a beat position to a 1-based bar number.

    Parameters:
    - beat: beat position (0-based)
    - numerator: time signature numerator
    - denominator: time signature denominator
    """
    beats_per_bar = numerator * (4 / denominator)
    return int(beat / beats_per_bar) + 1


# Global connection for resources
_ableton_connection = None
_EXTERNAL_PLUGIN_CACHE_TTL_SECONDS = 120.0
_external_plugin_cache_lock = threading.Lock()
_external_plugin_cache: Dict[str, Any] = {
    "plugins": None,
    "built_at": 0.0,
}


def _invalidate_external_plugin_cache() -> None:
    """Invalidate cached external plugin discovery results."""
    with _external_plugin_cache_lock:
        _external_plugin_cache["plugins"] = None
        _external_plugin_cache["built_at"] = 0.0

def get_ableton_connection():
    """Get or create a persistent Ableton connection"""
    global _ableton_connection
    
    if _ableton_connection is not None:
        try:
            # Test the connection with a simple ping
            # We'll try to send an empty message, which should fail if the connection is dead
            # but won't affect Ableton if it's alive
            _ableton_connection.sock.settimeout(1.0)
            _ableton_connection.sock.sendall(b'')
            return _ableton_connection
        except Exception as e:
            logger.warning(f"Existing connection is no longer valid: {str(e)}")
            try:
                _ableton_connection.disconnect()
            except:
                pass
            _ableton_connection = None
            _invalidate_external_plugin_cache()
    
    # Connection doesn't exist or is invalid, create a new one
    if _ableton_connection is None:
        # Try to connect up to 3 times with a short delay between attempts
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Connecting to Ableton (attempt {attempt}/{max_attempts})...")
                _ableton_connection = AbletonConnection(host="localhost", port=9877)
                if _ableton_connection.connect():
                    logger.info("Created new persistent connection to Ableton")
                    
                    # Validate connection with a simple command
                    try:
                        # Get session info as a test
                        _ableton_connection.send_command("get_session_info")
                        logger.info("Connection validated successfully")
                        return _ableton_connection
                    except Exception as e:
                        logger.error(f"Connection validation failed: {str(e)}")
                        _ableton_connection.disconnect()
                        _ableton_connection = None
                        _invalidate_external_plugin_cache()
                        # Continue to next attempt
                else:
                    _ableton_connection = None
            except Exception as e:
                logger.error(f"Connection attempt {attempt} failed: {str(e)}")
                if _ableton_connection:
                    _ableton_connection.disconnect()
                    _ableton_connection = None
                    _invalidate_external_plugin_cache()
            
            # Wait before trying again, but only if we have more attempts left
            if attempt < max_attempts:
                import time
                time.sleep(1.0)
        
        # If we get here, all connection attempts failed
        if _ableton_connection is None:
            logger.error("Failed to connect to Ableton after multiple attempts")
            raise Exception("Could not connect to Ableton. Make sure the Remote Script is running.")
    
    return _ableton_connection


# Core Tool endpoints

@mcp.tool()
def get_session_info(ctx: Context) -> str:
    """Get detailed information about the current Ableton session"""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_session_info")
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting session info from Ableton: {str(e)}")
        return f"Error getting session info: {str(e)}"

@mcp.tool()
def get_track_info(ctx: Context, track_index: int) -> str:
    """
    Get detailed information about a specific track in Ableton.

    Parameters:
    - track_index: Track number (1-based).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("get_track_info", {"track_index": ti})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting track info from Ableton: {str(e)}")
        return f"Error getting track info: {str(e)}"

@mcp.tool()
def create_midi_track(ctx: Context, index: int = -1) -> str:
    """
    Create a new MIDI track in the Ableton session.
    
    Parameters:
    - index: The index to insert the track at (-1 = end of list)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_midi_track", {"index": index})
        return f"Created new MIDI track: {result.get('name', 'unknown')}"
    except Exception as e:
        logger.error(f"Error creating MIDI track: {str(e)}")
        return f"Error creating MIDI track: {str(e)}"


@mcp.tool()
def set_track_name(ctx: Context, track_index: int, name: str) -> str:
    """
    Set the name of a track.

    Parameters:
    - track_index: Track number (1-based).
    - name: The new name for the track.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("set_track_name", {"track_index": ti, "name": name})
        return f"Renamed track to: {result.get('name', name)}"
    except Exception as e:
        logger.error(f"Error setting track name: {str(e)}")
        return f"Error setting track name: {str(e)}"


@mcp.tool()
def get_track_volume(ctx: Context, track_index: int) -> str:
    """Get the current fader volume and panning for a track.

    Returns the raw normalized value, its min/max range, and the panning.
    Volume 0.85 = 0 dB unity gain. Use this before set_track_volume to
    understand the current state.

    Parameters:
    - track_index: Track number (1-based). Return tracks come after session tracks.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("get_track_volume", {"track_index": ti})
        vol = result.get("volume", 0)
        pan = result.get("panning", 0)
        name = result.get("track_name", "?")
        vol_min = result.get("volume_min", 0)
        vol_max = result.get("volume_max", 1)
        # Approximate dB: unity is at 0.85 normalized
        unity = 0.85
        if vol > 0:
            import math
            db_approx = 20 * math.log10(vol / unity) if vol > 0 else -float('inf')
            db_str = f"{db_approx:+.1f} dB" if vol > 0 else "-inf dB"
        else:
            db_str = "-inf dB"
        pan_str = "center" if abs(pan) < 0.01 else (f"{abs(pan):.2f} {'L' if pan < 0 else 'R'}")
        return (
            f"Track '{name}':\n"
            f"  Volume: {vol:.4f} (range {vol_min:.2f}–{vol_max:.2f}) ≈ {db_str}\n"
            f"  Panning: {pan:.4f} ({pan_str})\n"
            f"  Unity gain (0 dB) = 0.85"
        )
    except Exception as e:
        logger.error(f"Error getting track volume: {str(e)}")
        return f"Error getting track volume: {str(e)}"


@mcp.tool()
def set_track_volume(ctx: Context, track_index: int, volume: float) -> str:
    """Set the mixer fader volume for a track directly.

    This controls the actual track fader, not any device parameter.

    Volume scale (normalized):
      0.0   = silence
      0.85  = 0 dB (unity gain, Ableton's default fader position)
      1.0   = maximum (~+6 dB)

    Parameters:
    - track_index: Track number (1-based). Return tracks come after session tracks.
    - volume: Normalized volume 0.0–1.0. Use 0.85 for unity (0 dB).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("set_track_volume", {
            "track_index": ti,
            "volume": volume,
        })
        name = result.get("track_name", "?")
        vol = result.get("volume", volume)
        import math
        unity = 0.85
        db_str = f"{20 * math.log10(vol / unity):+.1f} dB" if vol > 0 else "-inf dB"
        return f"Set '{name}' fader to {vol:.4f} (≈ {db_str})"
    except Exception as e:
        logger.error(f"Error setting track volume: {str(e)}")
        return f"Error setting track volume: {str(e)}"


@mcp.tool()
def set_track_panning(ctx: Context, track_index: int, panning: float) -> str:
    """Set the mixer panning for a track.

    Parameters:
    - track_index: Track number (1-based).
    - panning: -1.0 = full left, 0.0 = center, +1.0 = full right.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("set_track_panning", {
            "track_index": ti,
            "panning": panning,
        })
        name = result.get("track_name", "?")
        pan = result.get("panning", panning)
        pan_str = "center" if abs(pan) < 0.01 else (f"{abs(pan):.2f} {'L' if pan < 0 else 'R'}")
        return f"Set '{name}' panning to {pan:.4f} ({pan_str})"
    except Exception as e:
        logger.error(f"Error setting track panning: {str(e)}")
        return f"Error setting track panning: {str(e)}"


@mcp.tool()
def create_clip(ctx: Context, track_index: int, clip_index: int, length: float = 4.0) -> str:
    """
    Create a new MIDI clip in the specified track and clip slot.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip slot number (1-based).
    - length: The length of the clip in beats (default: 4.0).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index")
        result = ableton.send_command("create_clip", {
            "track_index": ti,
            "clip_index": ci,
            "length": length
        })
        return f"Created new clip at track {track_index}, slot {clip_index} with length {length} beats"
    except Exception as e:
        logger.error(f"Error creating clip: {str(e)}")
        return f"Error creating clip: {str(e)}"

@mcp.tool()
def add_notes_to_clip(
    ctx: Context,
    track_index: int,
    clip_index: int,
    notes: List[Dict[str, Union[int, float, bool]]]
) -> str:
    """
    Add MIDI notes to a clip.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip slot number (1-based).
    - notes: List of note dictionaries, each with pitch, start_time, duration, velocity, and mute.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index")
        result = ableton.send_command("add_notes_to_clip", {
            "track_index": ti,
            "clip_index": ci,
            "notes": notes
        })
        return f"Added {len(notes)} notes to clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error adding notes to clip: {str(e)}")
        return f"Error adding notes to clip: {str(e)}"

@mcp.tool()
def set_clip_name(ctx: Context, track_index: int, clip_index: int, name: str) -> str:
    """
    Set the name of a clip.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip slot number (1-based).
    - name: The new name for the clip.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index")
        result = ableton.send_command("set_clip_name", {
            "track_index": ti,
            "clip_index": ci,
            "name": name
        })
        return f"Renamed clip at track {track_index}, slot {clip_index} to '{name}'"
    except Exception as e:
        logger.error(f"Error setting clip name: {str(e)}")
        return f"Error setting clip name: {str(e)}"

@mcp.tool()
def set_tempo(ctx: Context, tempo: float) -> str:
    """
    Set the tempo of the Ableton session.
    
    Parameters:
    - tempo: The new tempo in BPM
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_tempo", {"tempo": tempo})
        return f"Set tempo to {tempo} BPM"
    except Exception as e:
        logger.error(f"Error setting tempo: {str(e)}")
        return f"Error setting tempo: {str(e)}"


@mcp.tool()
def load_instrument_or_effect(ctx: Context, track_index: int, uri: str) -> str:
    """
    Load an instrument or effect onto a track using its URI.

    Parameters:
    - track_index: Track number (1-based).
    - uri: The URI of the instrument or effect to load (e.g., 'query:Synths#Instrument%20Rack:Bass:FileId_5116').
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("load_browser_item", {
            "track_index": ti,
            "item_uri": uri
        })
        
        # Check if the instrument was loaded successfully
        if result.get("loaded", False):
            head = (f"Loaded '{result.get('item_name', uri)}' on track "
                    f"{track_index} ('{result.get('track_name', '?')}')")
            new_devices = result.get("new_devices", [])
            if new_devices:
                return f"{head}. New devices: {', '.join(new_devices)}"
            devices = result.get("devices_after", [])
            tail = ", ".join(devices) if devices else "(none reported yet)"
            note = result.get("note", "")
            return f"{head}. Devices on track: {tail}" + (f"\n  {note}" if note else "")
        else:
            return f"Failed to load instrument with URI '{uri}'"
    except Exception as e:
        logger.error(f"Error loading instrument by URI: {str(e)}")
        return f"Error loading instrument by URI: {str(e)}"

@mcp.tool()
def fire_clip(ctx: Context, track_index: int, clip_index: int) -> str:
    """
    Start playing a clip.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip slot number (1-based).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index")
        result = ableton.send_command("fire_clip", {
            "track_index": ti,
            "clip_index": ci
        })
        return f"Started playing clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error firing clip: {str(e)}")
        return f"Error firing clip: {str(e)}"

@mcp.tool()
def stop_clip(ctx: Context, track_index: int, clip_index: int) -> str:
    """
    Stop playing a clip.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip slot number (1-based).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index")
        result = ableton.send_command("stop_clip", {
            "track_index": ti,
            "clip_index": ci
        })
        return f"Stopped clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error stopping clip: {str(e)}")
        return f"Error stopping clip: {str(e)}"

@mcp.tool()
def start_playback(ctx: Context) -> str:
    """Start playing the Ableton session."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("start_playback")
        return "Started playback"
    except Exception as e:
        logger.error(f"Error starting playback: {str(e)}")
        return f"Error starting playback: {str(e)}"

@mcp.tool()
def stop_playback(ctx: Context) -> str:
    """Stop playing the Ableton session."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("stop_playback")
        return "Stopped playback"
    except Exception as e:
        logger.error(f"Error stopping playback: {str(e)}")
        return f"Error stopping playback: {str(e)}"

@mcp.tool()
def get_browser_tree(ctx: Context, category_type: str = "all") -> str:
    """
    Get a hierarchical tree of browser categories from Ableton.
    
    Parameters:
    - category_type: Type of categories to get ('all', 'instruments', 'sounds', 'drums', 'audio_effects', 'midi_effects')
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_browser_tree", {
            "category_type": category_type
        })
        
        # Check if we got any categories
        if "available_categories" in result and len(result.get("categories", [])) == 0:
            available_cats = result.get("available_categories", [])
            return (f"No categories found for '{category_type}'. "
                   f"Available browser categories: {', '.join(available_cats)}")
        
        # Format the tree in a more readable way
        total_folders = result.get("total_folders", 0)
        formatted_output = f"Browser tree for '{category_type}' (showing {total_folders} folders):\n\n"
        
        def format_tree(item, indent=0):
            output = ""
            if item:
                prefix = "  " * indent
                name = item.get("name", "Unknown")
                path = item.get("path", "")
                has_more = item.get("has_more", False)
                
                # Add this item
                output += f"{prefix}• {name}"
                if path:
                    output += f" (path: {path})"
                if has_more:
                    output += " [...]"
                output += "\n"
                
                # Add children
                for child in item.get("children", []):
                    output += format_tree(child, indent + 1)
            return output
        
        # Format each category
        for category in result.get("categories", []):
            formatted_output += format_tree(category)
            formatted_output += "\n"
        
        return formatted_output
    except Exception as e:
        error_msg = str(e)
        if "Browser is not available" in error_msg:
            logger.error(f"Browser is not available in Ableton: {error_msg}")
            return f"Error: The Ableton browser is not available. Make sure Ableton Live is fully loaded and try again."
        elif "Could not access Live application" in error_msg:
            logger.error(f"Could not access Live application: {error_msg}")
            return f"Error: Could not access the Ableton Live application. Make sure Ableton Live is running and the Remote Script is loaded."
        else:
            logger.error(f"Error getting browser tree: {error_msg}")
            return f"Error getting browser tree: {error_msg}"

@mcp.tool()
def get_browser_items_at_path(ctx: Context, path: str) -> str:
    """
    Get browser items at a specific path in Ableton's browser.
    
    Parameters:
    - path: Path in the format "category/folder/subfolder"
            where category is one of the available browser categories in Ableton
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_browser_items_at_path", {
            "path": path
        })
        
        # Check if there was an error with available categories
        if "error" in result and "available_categories" in result:
            error = result.get("error", "")
            available_cats = result.get("available_categories", [])
            return (f"Error: {error}\n"
                   f"Available browser categories: {', '.join(available_cats)}")
        
        return json.dumps(result, indent=2)
    except Exception as e:
        error_msg = str(e)
        if "Browser is not available" in error_msg:
            logger.error(f"Browser is not available in Ableton: {error_msg}")
            return f"Error: The Ableton browser is not available. Make sure Ableton Live is fully loaded and try again."
        elif "Could not access Live application" in error_msg:
            logger.error(f"Could not access Live application: {error_msg}")
            return f"Error: Could not access the Ableton Live application. Make sure Ableton Live is running and the Remote Script is loaded."
        elif "Unknown or unavailable category" in error_msg:
            logger.error(f"Invalid browser category: {error_msg}")
            return f"Error: {error_msg}. Please check the available categories using get_browser_tree."
        elif "Path part" in error_msg and "not found" in error_msg:
            logger.error(f"Path not found: {error_msg}")
            return f"Error: {error_msg}. Please check the path and try again."
        else:
            logger.error(f"Error getting browser items at path: {error_msg}")
            return f"Error getting browser items at path: {error_msg}"


def _normalize_plugin_search_text(value: str) -> str:
    """Normalize plugin names/queries for tolerant matching."""
    if not value:
        return ""
    cleaned = re.sub(r"[\s\-_]+", " ", value.strip().lower())
    return re.sub(r"\s+", " ", cleaned)


def _plugin_match_score(plugin_name: str, query: str) -> int:
    """Compute a rough match score for plugin name search."""
    normalized_name = _normalize_plugin_search_text(plugin_name)
    normalized_query = _normalize_plugin_search_text(query)

    if not normalized_query:
        return 1
    if normalized_name == normalized_query:
        return 1000  # exact match
    if normalized_name.startswith(normalized_query):
        return 900  # strong prefix match

    query_tokens = [t for t in normalized_query.split(" ") if t]
    if query_tokens and all(token in normalized_name for token in query_tokens):
        # token coverage, weighted by total query token length
        return 700 + sum(len(t) for t in query_tokens)

    if normalized_query in normalized_name:
        return 600 + len(normalized_query)  # simple substring match

    return 0


def _collect_external_plugins_from_root(
    ableton: AbletonConnection,
    root_path: str,
    max_depth: int = 8,
    max_visited_paths: int = 2000,
) -> List[Dict[str, Any]]:
    """Recursively walk a browser root path and collect loadable plugin items."""
    stack: List[tuple[str, int]] = [(root_path, 0)]
    visited: set[str] = set()
    plugins: List[Dict[str, Any]] = []

    while stack:
        current_path, depth = stack.pop()
        if current_path in visited:
            continue
        visited.add(current_path)

        if len(visited) > max_visited_paths:
            raise RuntimeError(
                "Plugin traversal exceeded safety limit ({0} paths).".format(max_visited_paths)
            )

        result = ableton.send_command("get_browser_items_at_path", {"path": current_path})
        if "error" in result:
            # Root errors matter; deeper path misses are expected from stale paths.
            if depth == 0:
                raise ValueError(result.get("error", "Unknown browser root error"))
            continue

        items = result.get("items", [])
        for item in items:
            name = (item.get("name") or "").strip()
            if not name:
                continue

            child_path = "{0}/{1}".format(current_path, name)
            is_folder = bool(item.get("is_folder", False))
            is_loadable = bool(item.get("is_loadable", False))
            uri = item.get("uri")

            if is_loadable and uri:
                plugins.append({
                    "name": name,
                    "uri": uri,
                    "path": child_path,
                    "is_device": bool(item.get("is_device", False)),
                    "root": root_path,
                })

            if is_folder and depth < max_depth:
                stack.append((child_path, depth + 1))

    return plugins


def _discover_external_plugins(ableton: AbletonConnection) -> List[Dict[str, Any]]:
    """Discover loadable external plugins from common browser roots."""
    # Include aliases to survive differences in browser category naming.
    candidate_roots = ["plugins", "vst3", "vst2", "au", "plug-ins"]
    discovered_any_root = False
    errors: List[str] = []

    for root in candidate_roots:
        try:
            found = _collect_external_plugins_from_root(ableton, root_path=root)
            discovered_any_root = True
            if not found:
                continue

            # First successful non-empty root is enough; aliases can point to the same tree
            # and rescanning them is expensive.
            found.sort(key=lambda p: _normalize_plugin_search_text(p.get("name", "")))
            return found
        except Exception as e:
            errors.append("{0}: {1}".format(root, str(e)))

    if discovered_any_root:
        return []

    raise ValueError(
        "Could not discover external plugins. Tried roots: {0}. Last errors: {1}".format(
            ", ".join(candidate_roots),
            " | ".join(errors) if errors else "none",
        )
    )


def _get_cached_external_plugins(
    ableton: AbletonConnection,
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:
    """Get external plugins using a short-lived cache to avoid repeated deep scans."""
    now = time.monotonic()
    with _external_plugin_cache_lock:
        cached_plugins = _external_plugin_cache.get("plugins")
        built_at = float(_external_plugin_cache.get("built_at", 0.0) or 0.0)
        if (
            not force_refresh
            and cached_plugins is not None
            and (now - built_at) <= _EXTERNAL_PLUGIN_CACHE_TTL_SECONDS
        ):
            return list(cached_plugins)

    discovered = _discover_external_plugins(ableton)
    with _external_plugin_cache_lock:
        _external_plugin_cache["plugins"] = list(discovered)
        _external_plugin_cache["built_at"] = time.monotonic()
    return discovered


@mcp.tool()
def list_external_plugins(
    ctx: Context,
    query: str = "",
    max_results: int = 50,
    refresh_cache: bool = False,
) -> str:
    """List discovered external plugins (VST/AU), optionally filtered by name query.

    Parameters:
    - query: Optional case-insensitive search string.
    - max_results: Maximum number of plugins to display.
    - refresh_cache: If True, force a rescan instead of using cached results.
    """
    try:
        ableton = get_ableton_connection()
        plugins = _get_cached_external_plugins(ableton, force_refresh=refresh_cache)

        if query:
            scored = []
            for plugin in plugins:
                score = _plugin_match_score(plugin.get("name", ""), query)
                if score > 0:
                    scored.append((score, plugin))
            scored.sort(key=lambda x: (-x[0], _normalize_plugin_search_text(x[1].get("name", ""))))
            filtered = [item for _, item in scored]
        else:
            filtered = plugins

        if not filtered:
            if query:
                return "No external plugins matched query '{0}'.".format(query)
            return "No external plugins were discovered."

        max_results = max(1, int(max_results))
        shown = filtered[:max_results]
        lines = [
            "External plugins discovered: {0} total, showing {1}".format(len(filtered), len(shown)),
            "",
        ]
        for idx, plugin in enumerate(shown, start=1):
            lines.append(
                "  {0}. {1} (path: {2})".format(
                    idx,
                    plugin.get("name", "Unknown"),
                    plugin.get("path", "?"),
                )
            )

        if len(filtered) > len(shown):
            lines.append("")
            lines.append(
                "Use max_results={0} (or a tighter query) to see more.".format(len(filtered))
            )
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error listing external plugins: {str(e)}")
        return f"Error listing external plugins: {str(e)}"


@mcp.tool()
def load_external_plugin(
    ctx: Context,
    track_index: int,
    plugin_name: str,
    exact_match: bool = False,
    refresh_cache: bool = False,
) -> str:
    """Load an external plugin onto a track by plugin name (no URI required).

    Parameters:
    - track_index: Track number (1-based).
    - plugin_name: Plugin name to match (e.g., "FabFilter Pro-Q 3").
    - exact_match: If True, require exact normalized name match.
    - refresh_cache: If True, force a rescan before matching.
    """
    try:
        if not plugin_name or not plugin_name.strip():
            return "Error: plugin_name is required."

        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        plugins = _get_cached_external_plugins(ableton, force_refresh=refresh_cache)

        scored = []
        for plugin in plugins:
            score = _plugin_match_score(plugin.get("name", ""), plugin_name)
            if exact_match and score < 1000:
                continue
            if score > 0:
                scored.append((score, plugin))

        scored.sort(key=lambda x: (-x[0], _normalize_plugin_search_text(x[1].get("name", ""))))
        if not scored:
            return (
                "No external plugin matched '{0}'. Try list_external_plugins(query='{0}') "
                "to inspect candidates."
            ).format(plugin_name)

        top_score = scored[0][0]
        top_plugins = [plugin for score, plugin in scored if score == top_score]

        # For non-exact lookup, avoid guessing when multiple strongest candidates exist.
        if len(top_plugins) > 1 and top_score < 1000:
            options = ", ".join(p.get("name", "?") for p in top_plugins[:5])
            return (
                "Multiple plugins match '{0}': {1}. "
                "Please be more specific or set exact_match=True."
            ).format(plugin_name, options)

        chosen = top_plugins[0]
        result = ableton.send_command("load_browser_item", {
            "track_index": ti,
            "item_uri": chosen.get("uri"),
        })

        if result.get("loaded", False):
            return (
                "Loaded external plugin '{0}' on track {1} (matched '{2}')."
            ).format(chosen.get("name", "?"), track_index, plugin_name)
        return "Failed to load external plugin '{0}'.".format(chosen.get("name", "?"))
    except Exception as e:
        logger.error(f"Error loading external plugin: {str(e)}")
        return f"Error loading external plugin: {str(e)}"


@mcp.tool()
def load_drum_kit(ctx: Context, track_index: int, rack_uri: str, kit_path: str) -> str:
    """
    Load a drum rack and then load a specific drum kit into it.

    Parameters:
    - track_index: Track number (1-based).
    - rack_uri: The URI of the drum rack to load (e.g., 'Drums/Drum Rack').
    - kit_path: Path to the drum kit inside the browser (e.g., 'drums/acoustic/kit1').
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")

        # Step 1: Load the drum rack
        result = ableton.send_command("load_browser_item", {
            "track_index": ti,
            "item_uri": rack_uri
        })
        
        if not result.get("loaded", False):
            return f"Failed to load drum rack with URI '{rack_uri}'"
        
        # Step 2: Get the drum kit items at the specified path
        kit_result = ableton.send_command("get_browser_items_at_path", {
            "path": kit_path
        })
        
        if "error" in kit_result:
            return f"Loaded drum rack but failed to find drum kit: {kit_result.get('error')}"
        
        # Step 3: Find a loadable drum kit
        kit_items = kit_result.get("items", [])
        loadable_kits = [item for item in kit_items if item.get("is_loadable", False)]
        
        if not loadable_kits:
            return f"Loaded drum rack but no loadable drum kits found at '{kit_path}'"
        
        # Step 4: Load the first loadable kit
        kit_uri = loadable_kits[0].get("uri")
        load_result = ableton.send_command("load_browser_item", {
            "track_index": ti,
            "item_uri": kit_uri
        })
        
        return f"Loaded drum rack and kit '{loadable_kits[0].get('name')}' on track {track_index}"
    except Exception as e:
        logger.error(f"Error loading drum kit: {str(e)}")
        return f"Error loading drum kit: {str(e)}"

# --- Arrangement View Tools ---

_ARRANGEMENT_TIP = "\nTip: use set_ableton_view(view='Arranger') to see changes in arrangement view."


def _get_time_signature():
    """Get current time signature from Ableton."""
    ableton = get_ableton_connection()
    info = ableton.send_command("get_session_info")
    return info.get("signature_numerator", 4), info.get("signature_denominator", 4)


def _convert_bar_to_beat(bar: int, beat: float = 0.0) -> float:
    """Convert bar (1-based) to beat, fetching time signature from Ableton."""
    if bar > 0:
        num, denom = _get_time_signature()
        return bar_to_beat(bar, num, denom)
    return beat


@mcp.tool()
def get_arrangement_info(ctx: Context, track_index: int = 0) -> str:
    """Get arrangement clips and transport state.

    Parameters:
    - track_index: Track number (1-based). 0 = all tracks.
    """
    try:
        ableton = get_ableton_connection()
        idx = _optional_to_zero_based(track_index, "track_index")
        result = ableton.send_command("get_arrangement_info", {"track_index": idx if idx is not None else -1})

        num = result.get("transport", {}).get("signature_numerator", 4)
        denom = result.get("transport", {}).get("signature_denominator", 4)
        transport = result.get("transport", {})

        lines = ["=== Arrangement Info ==="]
        lines.append(f"Tempo: {transport.get('tempo')} BPM | "
                     f"Time Sig: {num}/{denom} | "
                     f"Playing: {transport.get('is_playing')} | "
                     f"Position: bar {beat_to_bar(transport.get('current_time', 0), num, denom)}")
        if transport.get("loop_enabled"):
            ls = transport.get("loop_start", 0)
            ll = transport.get("loop_length", 0)
            lines.append(f"Loop: bars {beat_to_bar(ls, num, denom)}-"
                         f"{beat_to_bar(ls + ll, num, denom)}")

        for t in result.get("tracks", []):
            clips = t.get("arrangement_clips", [])
            lines.append(f"\nTrack {t['index'] + 1}: {t['name']} "
                         f"({'MIDI' if t.get('is_midi') else 'Audio'}) — "
                         f"{len(clips)} clip(s)")
            for c in clips:
                st = c.get("start_time", 0)
                et = c.get("end_time", 0)
                muted = " [MUTED]" if c.get("muted") else ""
                lines.append(f"  {c.get('index', 0) + 1}. \"{c.get('name', '')}\" "
                             f"bars {beat_to_bar(st, num, denom)}-"
                             f"{beat_to_bar(et, num, denom)}{muted}")

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting arrangement info: {str(e)}")
        return f"Error getting arrangement info: {str(e)}"


@mcp.tool()
def get_cue_points(ctx: Context) -> str:
    """List all cue points (locators) with bar positions."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_cue_points")
        num, denom = _get_time_signature()

        cues = result.get("cue_points", [])
        if not cues:
            return "No cue points in this project."

        lines = ["=== Cue Points ==="]
        for cp in cues:
            bar = beat_to_bar(cp.get("time", 0), num, denom)
            lines.append(f"  \"{cp.get('name', '')}\" — bar {bar}")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting cue points: {str(e)}")
        return f"Error getting cue points: {str(e)}"


@mcp.tool()
def set_song_time(ctx: Context, bar: int = 0, beat: float = 0.0) -> str:
    """Jump playback to a position.

    Parameters:
    - bar: Bar number (1-based). Takes precedence over beat.
    - beat: Beat position (0-based).
    """
    try:
        ableton = get_ableton_connection()
        time_val = _convert_bar_to_beat(bar, beat)
        result = ableton.send_command("set_song_time", {"time": time_val})
        num, denom = _get_time_signature()
        return f"Jumped to bar {beat_to_bar(time_val, num, denom)} (beat {time_val})"
    except Exception as e:
        logger.error(f"Error setting song time: {str(e)}")
        return f"Error setting song time: {str(e)}"


@mcp.tool()
def set_arrangement_loop(
    ctx: Context,
    enabled: bool = True,
    start_bar: int = 0,
    end_bar: int = 0,
    start_beat: float = 0.0,
    length_beats: float = 0.0,
) -> str:
    """Enable/disable arrangement loop and set region.

    Parameters:
    - enabled: Whether loop is on.
    - start_bar: Loop start (1-based). Takes precedence over start_beat.
    - end_bar: Loop end (1-based). Used with start_bar to compute length.
    - start_beat: Loop start in beats.
    - length_beats: Loop length in beats.
    """
    try:
        ableton = get_ableton_connection()
        num, denom = _get_time_signature()

        start = None
        length = None
        if start_bar > 0:
            start = bar_to_beat(start_bar, num, denom)
            if end_bar > start_bar:
                length = bar_to_beat(end_bar, num, denom) - start
        else:
            if start_beat > 0:
                start = start_beat
            if length_beats > 0:
                length = length_beats

        params = {"enabled": enabled}
        if start is not None:
            params["start"] = start
        if length is not None:
            params["length"] = length

        result = ableton.send_command("set_arrangement_loop", params)
        state = "enabled" if result.get("enabled") else "disabled"
        ls = result.get("start", 0)
        ll = result.get("length", 0)
        return (f"Loop {state}: bars {beat_to_bar(ls, num, denom)}-"
                f"{beat_to_bar(ls + ll, num, denom)}")
    except Exception as e:
        logger.error(f"Error setting arrangement loop: {str(e)}")
        return f"Error setting arrangement loop: {str(e)}"


@mcp.tool()
def jump_to_cue_point(ctx: Context, direction: str = "", name: str = "") -> str:
    """Jump to a cue point.

    Parameters:
    - direction: "next" or "prev"
    - name: Cue point name to jump to.
    """
    try:
        ableton = get_ableton_connection()
        params = {}
        if direction:
            params["direction"] = direction
        if name:
            params["name"] = name
        result = ableton.send_command("jump_to_cue", params)
        return f"Jumped to cue point: {result}"
    except Exception as e:
        logger.error(f"Error jumping to cue point: {str(e)}")
        return f"Error jumping to cue point: {str(e)}"


@mcp.tool()
def create_cue_point(ctx: Context, bar: int = 0, beat: float = 0.0, name: str = "") -> str:
    """Create a cue point at a position.

    Parameters:
    - bar: Bar number (1-based).
    - beat: Beat position (0-based).
    - name: Name for the cue point.
    """
    try:
        ableton = get_ableton_connection()
        time_val = _convert_bar_to_beat(bar, beat)
        result = ableton.send_command("create_cue_point", {"time": time_val, "name": name})
        return f"Created cue point '{name}' at bar {bar if bar > 0 else '?'}"
    except Exception as e:
        logger.error(f"Error creating cue point: {str(e)}")
        return f"Error creating cue point: {str(e)}"


@mcp.tool()
def delete_cue_point(ctx: Context, bar: int = 0, beat: float = 0.0) -> str:
    """Delete a cue point at a position.

    Parameters:
    - bar: Bar number (1-based).
    - beat: Beat position (0-based).
    """
    try:
        ableton = get_ableton_connection()
        time_val = _convert_bar_to_beat(bar, beat)
        ableton.send_command("delete_cue_point", {"time": time_val})
        return f"Deleted cue point at bar {bar if bar > 0 else '?'}"
    except Exception as e:
        logger.error(f"Error deleting cue point: {str(e)}")
        return f"Error deleting cue point: {str(e)}"


@mcp.tool()
def create_arrangement_midi_clip(
    ctx: Context,
    track_index: int,
    start_bar: int = 0,
    end_bar: int = 0,
    start_beat: float = 0.0,
    length_beats: float = 4.0,
    name: str = "",
) -> str:
    """Create an empty MIDI clip in the arrangement.

    Parameters:
    - track_index: Track number (1-based).
    - start_bar: Start bar (1-based). Takes precedence over start_beat.
    - end_bar: End bar (1-based). Used with start_bar to compute length.
    - start_beat: Start position in beats.
    - length_beats: Clip length in beats.
    - name: Optional clip name.
    """
    try:
        ableton = get_ableton_connection()
        num, denom = _get_time_signature()

        if start_bar > 0:
            position = bar_to_beat(start_bar, num, denom)
            if end_bar > start_bar:
                length = bar_to_beat(end_bar, num, denom) - position
            else:
                length = length_beats
        else:
            position = start_beat
            length = length_beats

        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("create_arrangement_clip", {
            "track_index": ti,
            "position": position,
            "length": length,
        })

        msg = (f"Created MIDI clip on track {track_index} at "
               f"bar {beat_to_bar(position, num, denom)}, "
               f"length {length} beats")

        overlapped = result.get("overlapped_clips", [])
        if overlapped:
            msg += f"\nWarning: overlapped existing clips: {', '.join(overlapped)}"

        return msg + _ARRANGEMENT_TIP
    except Exception as e:
        logger.error(f"Error creating arrangement MIDI clip: {str(e)}")
        return f"Error creating arrangement MIDI clip: {str(e)}"


@mcp.tool()
def create_arrangement_audio_clip(
    ctx: Context,
    track_index: int,
    file_path: str,
    start_bar: int = 0,
    start_beat: float = 0.0,
    warp: bool = None,
) -> str:
    """Place an audio file as a clip in the arrangement.

    Live WARPS imported audio by default, which time-stretches it to the grid.
    Measured on 12.4.3: a 15.085 s file became 16.000 s at 120 BPM, +6.1 %,
    with nothing in the API saying so. Pass warp=False whenever the audio has
    to stay at its true speed — anything cut against picture, and any extract
    whose lip-sync matters. The reply reports the stretch either way.

    A clip cannot be moved afterwards (start_time is read-only), so place it at
    its final position now.

    Parameters:
    - track_index: Track number (1-based).
    - file_path: Path to the audio file. A Windows path (E:\\...), never a
      /home/... or \\\\wsl.localhost\\... path — Live cannot read those.
    - start_bar: Start bar (1-based).
    - start_beat: Start position in beats.
    - warp: False to play the file at its true speed, True to warp it to the
      grid, None to leave Live's own default (which warps).
    """
    try:
        ableton = get_ableton_connection()
        position = _convert_bar_to_beat(start_bar, start_beat)

        ti = _to_zero_based(track_index, "track_index")
        result = ableton.send_command("create_arrangement_audio_clip", {
            "track_index": ti,
            "position": position,
            "file_path": file_path,
            "warp": warp,
        })
        lines = [f"Created audio clip from '{file_path}' on track {track_index}"]

        # Measure in a SECOND round trip, never in the reply above. Writing
        # `clip.warping` does not make Live recompute the clip's extent in the
        # same tick, so anything measured there still describes the warped
        # import: a 6.000 s file posted with warp=False reported 8.000 s and a
        # +33.33 % stretch that did not exist. One extra round trip lands on a
        # later tick and reads the settled extent.
        measured = None
        if isinstance(result, dict) and result.get("clip_index") is not None:
            try:
                measured = ableton.send_command("measure_arrangement_clip", {
                    "track_index": ti,
                    "clip_index": result["clip_index"],
                })
            except Exception as e:
                lines.append(f"  (stretch not measured: {e})")

        if isinstance(measured, dict):
            if measured.get("file_seconds") is not None:
                lines.append(
                    "  file {0:.3f}s -> clip {1:.3f}s ({2:+.2f}%), warping={3}".format(
                        measured.get("file_seconds", 0.0),
                        measured.get("clip_seconds", 0.0),
                        measured.get("stretch_percent", 0.0),
                        measured.get("warping")))
            if measured.get("WARNING"):
                lines.append("  WARNING: " + measured["WARNING"])
        return "\n".join(lines) + _ARRANGEMENT_TIP
    except Exception as e:
        logger.error(f"Error creating arrangement audio clip: {str(e)}")
        return f"Error creating arrangement audio clip: {str(e)}"


@mcp.tool()
def duplicate_clip_to_arrangement(
    ctx: Context,
    track_index: int,
    clip_index: int,
    destination_bar: int = 0,
    destination_beat: float = 0.0,
) -> str:
    """Copy a session clip to the arrangement.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Session clip slot (1-based).
    - destination_bar: Destination bar (1-based).
    - destination_beat: Destination beat.
    """
    try:
        ableton = get_ableton_connection()
        dest = _convert_bar_to_beat(destination_bar, destination_beat)

        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index")
        result = ableton.send_command("duplicate_to_arrangement", {
            "track_index": ti,
            "clip_index": ci,
            "destination_time": dest,
        })
        return f"Duplicated session clip to arrangement on track {track_index}" + _ARRANGEMENT_TIP
    except Exception as e:
        logger.error(f"Error duplicating clip to arrangement: {str(e)}")
        return f"Error duplicating clip to arrangement: {str(e)}"


@mcp.tool()
def delete_arrangement_clip(
    ctx: Context,
    track_index: int,
    clip_index: int = 0,
    clip_name: str = "",
) -> str:
    """Delete an arrangement clip.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip position in arrangement (1-based).
    - clip_name: Clip name (alternative to clip_index).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        params = {"track_index": ti}
        if clip_name:
            params["clip_name"] = clip_name
        elif clip_index > 0:
            params["clip_index"] = _to_zero_based(clip_index, "clip_index")
        else:
            return "Error: provide clip_index or clip_name"

        ableton.send_command("delete_arrangement_clip", params)
        ref = f"'{clip_name}'" if clip_name else f"#{clip_index}"
        return f"Deleted arrangement clip {ref} on track {track_index}" + _ARRANGEMENT_TIP
    except Exception as e:
        logger.error(f"Error deleting arrangement clip: {str(e)}")
        return f"Error deleting arrangement clip: {str(e)}"


@mcp.tool()
def set_arrangement_clip_property(
    ctx: Context,
    track_index: int,
    clip_index: int = 1,
    clip_name: str = "",
    name: str = "",
    muted: bool = None,
    color: int = None,
    looping: bool = None,
    loop_start: float = None,
    loop_end: float = None,
    gain: float = None,
    pitch_coarse: int = None,
    pitch_fine: float = None,
    warping: bool = None,
    warp_mode: int = None,
) -> str:
    """Set properties on an arrangement clip.

    An arrangement clip CANNOT BE TRIMMED OR MOVED through this API, measured
    on Live 12.4.3: start_time and end_time have no setter, start_marker is
    silently reset to 0, end_marker is accepted and read back while changing
    nothing, and position moves the loop brace inside the sample rather than
    the clip on the timeline. Cut the extract upstream and import it already
    the right length, at its final position. To relocate an existing clip,
    duplicate_clip_to_arrangement to the new spot then delete the original.

    `looping` + `loop_start`/`loop_end` do work, but they LOOP the clip inside
    its unchanged extent; they do not shorten it.

    Beware two unit traps: clip `gain` is 0.4 at 0.00 dB (not 1.0), and
    `warping=True` time-stretches the audio to the grid.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip position (1-based).
    - clip_name: Clip name (alternative to clip_index).
    - name: New clip name.
    - muted: Mute state.
    - color: Color (0x00RRGGBB).
    - looping: Loop on/off.
    - loop_start: Loop start in beats.
    - loop_end: Loop end in beats.
    - gain: Audio gain (0.0-1.0).
    - pitch_coarse: Semitone pitch shift (-48 to 48).
    - pitch_fine: Fine pitch shift (-50 to 49 cents).
    - warping: Warp on/off.
    - warp_mode: Warp mode (0-6).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index") if clip_index > 0 else 0

        props = {
            "name": name, "muted": muted, "color": color, "looping": looping,
            "loop_start": loop_start, "loop_end": loop_end, "gain": gain,
            "pitch_coarse": pitch_coarse, "pitch_fine": pitch_fine,
            "warping": warping, "warp_mode": warp_mode,
        }

        changes = []
        warnings = []
        last = None
        for prop_name, value in props.items():
            if value is not None and value != "":
                last = ableton.send_command("set_arrangement_clip_property", {
                    "track_index": ti,
                    "clip_index": ci,
                    "property": prop_name,
                    "value": value,
                })
                changes.append(f"{prop_name}={value}")
                if isinstance(last, dict) and last.get("WARNING"):
                    warnings.append(f"  WARNING ({prop_name}): {last['WARNING']}")

        if not changes:
            return "No properties specified to change."

        ref = f"'{clip_name}'" if clip_name else f"clip {clip_index}"
        lines = [f"Updated {ref} on track {track_index}: {', '.join(changes)}"]
        if isinstance(last, dict) and last.get("extent_beats") is not None:
            lines.append(
                "  clip now spans {0:.3f}-{1:.3f} ({2:.3f} beats), "
                "loop={3:.3f} looping={4} warping={5}".format(
                    last.get("start_time", 0.0), last.get("end_time", 0.0),
                    last.get("extent_beats", 0.0), last.get("loop_length_beats", 0.0),
                    last.get("looping"), last.get("warping")))
        lines.extend(warnings)
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error setting arrangement clip property: {str(e)}")
        return f"Error setting arrangement clip property: {str(e)}"


@mcp.tool()
def set_ableton_view(ctx: Context, view: str = "Arranger") -> str:
    """Switch Ableton's main view.

    Parameters:
    - view: View name. Options: Arranger, Session, Detail, Detail/Clip,
            Detail/DeviceChain, Browser.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_view", {"view_name": view})
        return f"Switched to {view} view"
    except Exception as e:
        logger.error(f"Error setting view: {str(e)}")
        return f"Error setting view: {str(e)}"


@mcp.tool()
def control_arrangement_view(ctx: Context, action: str, track_index: int = 0) -> str:
    """Control the arrangement view.

    Parameters:
    - action: One of: zoom_in, zoom_out, scroll_left, scroll_right,
              follow_on, follow_off, collapse_track, expand_track.
    - track_index: Track number (1-based, for collapse/expand).
    """
    try:
        ableton = get_ableton_connection()
        ti = _optional_to_zero_based(track_index, "track_index")
        result = ableton.send_command("control_arrangement_view", {
            "action": action,
            "track_index": ti if ti is not None else 0,
        })
        return f"Arrangement view: {action} done"
    except Exception as e:
        logger.error(f"Error controlling arrangement view: {str(e)}")
        return f"Error controlling arrangement view: {str(e)}"


@mcp.tool()
def manage_clip_automation(
    ctx: Context,
    track_index: int,
    clip_index: int = 1,
    clip_name: str = "",
    action: str = "create",
    parameter_name: str = "volume",
) -> str:
    """Create or clear automation envelopes on SESSION clips.

    This targeted arrangement clips, which made it unusable: they reject every
    envelope call in Live 12.4.3 with "Not a session clip", so it could never
    succeed. It now resolves a session clip slot.

    Creating an envelope only makes an EMPTY one — it draws no fade. Use
    set_clip_fade to actually write a ramp.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: SESSION clip slot (1-based).
    - clip_name: ignored — session slots are addressed by index.
    - action: "create", "clear", or "clear_all".
    - parameter_name: Parameter to automate. Live's mixer parameters are named
      "Track Volume" and "Track Panning"; "volume" and "pan" are accepted.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        ci = _to_zero_based(clip_index, "clip_index") if clip_index > 0 else 0
        result = ableton.send_command("manage_clip_automation", {
            "track_index": ti,
            "clip_index": ci,
            "action": action,
            "parameter_name": parameter_name,
        })

        if action == "clear_all":
            return f"Cleared all automation on clip {clip_index}, track {track_index}"
        return f"Automation {action}: {parameter_name} on clip {clip_index}, track {track_index}"
    except Exception as e:
        logger.error(f"Error managing clip automation: {str(e)}")
        return f"Error managing clip automation: {str(e)}"


# ── Device / Parameter Tools ──────────────────────────────────────

@mcp.tool()
def get_device_parameters(
    ctx: Context,
    track_index: int,
    device_index: int = 1,
    chain_index: int = 0,
    category: str = "",
    show_all: bool = False,
) -> str:
    """List parameters for a device on a track.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number on the track (1-based, default 1).
    - chain_index: Chain number inside a rack (1-based, 0 = no chain).
    - category: Filter by category name (returns detail for that category).
    - show_all: If True, return all parameters in detail mode.

    Default mode returns a summary grouped by category with counts.
    Specify category or show_all=True for full parameter details.
    """
    try:
        from MCP_Server.plugin_aliases import get_categories, get_alias_for_param

        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _to_zero_based(device_index, "device_index")
        ci = _optional_to_zero_based(chain_index, "chain_index")
        result = ableton.send_command("get_device_parameters", {
            "track_index": ti,
            "device_index": di,
            "chain_index": ci,
            "show_all": True,  # Always get full list from RS, group MCP-side
        })

        device_name = result.get("device_name", "Unknown")
        params = result.get("parameters", [])
        param_count = result.get("parameter_count", len(params))

        # Attach aliases
        for p in params:
            alias = get_alias_for_param(device_name, p["name"])
            if alias:
                p["alias"] = alias

        # Category grouping
        categories = get_categories(device_name)

        def categorize(p_name):
            if categories:
                for cat_name, prefixes in categories.items():
                    for prefix in prefixes:
                        if p_name.startswith(prefix):
                            return cat_name
            return "Other"

        # Detail mode
        if show_all or category:
            filtered = params
            if category:
                filtered = [p for p in params if categorize(p["name"]).lower() == category.lower()]
                if not filtered:
                    return "No parameters found in category '{0}'. Available categories: {1}".format(
                        category, ", ".join(sorted(set(categorize(p["name"]) for p in params))))

            lines = ["{0} — {1} parameters".format(device_name, len(filtered)), ""]
            for p in filtered:
                alias_str = " ({0})".format(p["alias"]) if p.get("alias") else ""
                enabled_str = "" if p["is_enabled"] else " [disabled]"
                lines.append("  {0}. {1}{2}: {3} (normalized {4}){5}".format(
                    p["index"] + 1, p["name"], alias_str,
                    p["display_value"], round(p["value"], 2), enabled_str))
            return "\n".join(lines)

        # Summary mode
        groups = {}
        for p in params:
            cat = categorize(p["name"])
            groups.setdefault(cat, []).append(p)

        lines = ["{0} — {1} parameters total".format(device_name, param_count), ""]
        for cat_name, cat_params in groups.items():
            lines.append("  {0}: {1} parameters".format(cat_name, len(cat_params)))
        lines.append("")
        lines.append("Use category='<name>' or show_all=True for full details.")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting device parameters: {str(e)}")
        return f"Error getting device parameters: {str(e)}"


@mcp.tool()
def set_device_parameter(
    ctx: Context,
    track_index: int,
    device_index: int = 1,
    chain_index: int = 0,
    parameter_name: str = "",
    parameter_index: int = 0,
    value: float = 0.0,
) -> str:
    """Set a device parameter value.

    parameter_index=1 is "Device On" on every Live device — parameter 0 of the
    chain. Setting it turns the device off or on, which is rarely what was
    meant; address parameters by name and keep get_device_parameters at hand.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based, default 1).
    - chain_index: Chain number inside a rack (1-based, 0 = no chain).
    - parameter_name: Parameter name, friendly alias, or partial match.
    - parameter_index: Parameter number (1-based, alternative to name).
      1 = "Device On".
    - value: Normalized value 0.0-1.0.
    """
    try:
        from MCP_Server.plugin_aliases import resolve_alias

        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _to_zero_based(device_index, "device_index")
        ci = _optional_to_zero_based(chain_index, "chain_index")
        pi = _optional_to_zero_based(parameter_index, "parameter_index")

        # Resolve alias if parameter_name is provided
        resolved_name = parameter_name
        alias_used = None
        if parameter_name:
            # First get device name for alias resolution
            info = ableton.send_command("get_device_parameters", {
                "track_index": ti,
                "device_index": di,
                "chain_index": ci,
                "show_all": False,
            })
            device_name = info.get("device_name", "")
            real_name = resolve_alias(device_name, parameter_name)
            if real_name:
                alias_used = parameter_name
                resolved_name = real_name

        result = ableton.send_command("set_device_parameter", {
            "track_index": ti,
            "device_index": di,
            "chain_index": ci,
            "parameter_name": resolved_name if resolved_name else None,
            "parameter_index": pi,
            "value": value,
        })

        param_name = result.get("parameter_name", "?")
        display = result.get("display_value", "?")
        new_val = result.get("new_value", value)
        clamped = result.get("clamped", False)

        msg = "Set {0} to {1} (normalized {2})".format(param_name, display, round(new_val, 2))
        if alias_used:
            msg += " [alias: {0}]".format(alias_used)
        if clamped:
            msg += " (value was clamped to 0.0-1.0 range)"
        return msg
    except Exception as e:
        logger.error(f"Error setting device parameter: {str(e)}")
        return f"Error setting device parameter: {str(e)}"


@mcp.tool()
def enable_device(
    ctx: Context,
    track_index: int,
    device_index: int = 0,
    device_name: str = "",
    chain_index: int = 0,
) -> str:
    """Enable (activate) a device on a track.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based). Use 0 if using device_name.
    - device_name: Device name (alternative to device_index).
    - chain_index: Chain number inside a rack (1-based, 0 = no chain).
    """
    return _toggle_device(track_index, device_index, device_name, chain_index, True)


@mcp.tool()
def disable_device(
    ctx: Context,
    track_index: int,
    device_index: int = 0,
    device_name: str = "",
    chain_index: int = 0,
) -> str:
    """Disable (bypass) a device on a track.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based). Use 0 if using device_name.
    - device_name: Device name (alternative to device_index).
    - chain_index: Chain number inside a rack (1-based, 0 = no chain).
    """
    return _toggle_device(track_index, device_index, device_name, chain_index, False)


def _toggle_device(track_index, device_index, device_name, chain_index, enabled):
    """Shared logic for enable/disable device."""
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _optional_to_zero_based(device_index, "device_index")
        ci = _optional_to_zero_based(chain_index, "chain_index")

        # If device_name provided, resolve to index
        if device_name and di is None:
            info = ableton.send_command("get_track_info", {"track_index": ti})
            devices = info.get("devices", [])
            matches = [d for d in devices if d["name"].lower() == device_name.lower()]
            if len(matches) == 0:
                return "Error: Device '{0}' not found on track {1}".format(device_name, track_index)
            if len(matches) > 1:
                match_list = ", ".join("{0} (index {1})".format(d["name"], d["index"] + 1) for d in matches)
                return "Error: Multiple devices named '{0}' on track {1}: {2}".format(
                    device_name, track_index, match_list)
            di = matches[0]["index"]
        elif di is None:
            di = 0

        result = ableton.send_command("set_device_enabled", {
            "track_index": ti,
            "device_index": di,
            "chain_index": ci,
            "enabled": enabled,
        })

        name = result.get("device_name", "?")
        state = "enabled" if result.get("is_active", enabled) else "disabled"
        return "{0} {1}".format(name, state)
    except Exception as e:
        logger.error(f"Error toggling device: {str(e)}")
        return f"Error toggling device: {str(e)}"


@mcp.tool()
def get_chain_info(
    ctx: Context,
    track_index: int,
    device_index: int = 1,
    chain_index: int = 0,
) -> str:
    """List chains in a rack device, or devices within a specific chain.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based, default 1).
    - chain_index: Chain number (1-based) to drill into. 0 = list all chains.
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _to_zero_based(device_index, "device_index")
        ci = _optional_to_zero_based(chain_index, "chain_index")
        result = ableton.send_command("get_chain_info", {
            "track_index": ti,
            "device_index": di,
            "chain_index": ci,
        })

        if chain_index > 0:
            # Detail for specific chain
            chain_name = result.get("chain_name", "?")
            devices = result.get("devices", [])
            lines = ["Chain '{0}' — {1} devices".format(chain_name, len(devices)), ""]
            for d in devices:
                active = "" if d.get("is_active", True) else " [disabled]"
                lines.append("  {0}. {1} ({2}, {3} params){4}".format(
                    d["index"] + 1, d["name"], d["type"],
                    d.get("parameter_count", "?"), active))
            return "\n".join(lines)
        else:
            # List all chains
            device_name = result.get("device_name", "?")
            chains = result.get("chains", [])
            lines = ["{0} — {1} chains".format(device_name, len(chains)), ""]
            for c in chains:
                mute_str = " [muted]" if c.get("mute") else ""
                solo_str = " [solo]" if c.get("solo") else ""
                dev_names = ", ".join(d["name"] for d in c.get("devices", []))
                lines.append("  {0}. {1}{2}{3}: {4} devices ({5})".format(
                    c["index"] + 1, c["name"], mute_str, solo_str,
                    c["device_count"], dev_names or "empty"))
            return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting chain info: {str(e)}")
        return f"Error getting chain info: {str(e)}"


@mcp.tool()
def get_drum_pad_info(ctx: Context, track_index: int, device_index: int = 1) -> str:
    """List filled drum pads in a Drum Rack.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based, default 1).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _to_zero_based(device_index, "device_index")
        result = ableton.send_command("get_drum_pad_info", {
            "track_index": ti,
            "device_index": di,
        })

        device_name = result.get("device_name", "?")
        pads = result.get("filled_pads", [])
        lines = ["{0} — {1} filled pads".format(device_name, len(pads)), ""]
        for pad in pads:
            mute_str = " [muted]" if pad.get("mute") else ""
            solo_str = " [solo]" if pad.get("solo") else ""
            dev_names = []
            for chain in pad.get("chains", []):
                for d in chain.get("devices", []):
                    dev_names.append(d["name"])
            lines.append("  Note {0}: {1}{2}{3} → {4}".format(
                pad["note"], pad["name"], mute_str, solo_str,
                ", ".join(dev_names) or "empty"))
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting drum pad info: {str(e)}")
        return f"Error getting drum pad info: {str(e)}"


@mcp.tool()
def delete_device(
    ctx: Context,
    track_index: int,
    device_index: int = 0,
    device_name: str = "",
) -> str:
    """Delete a device from a track.

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based).
    - device_name: Device name (alternative to device_index).
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _optional_to_zero_based(device_index, "device_index")

        # Resolve by name if needed
        if device_name and di is None:
            info = ableton.send_command("get_track_info", {"track_index": ti})
            devices = info.get("devices", [])
            matches = [d for d in devices if d["name"].lower() == device_name.lower()]
            if len(matches) == 0:
                return "Error: Device '{0}' not found on track {1}".format(device_name, track_index)
            if len(matches) > 1:
                match_list = ", ".join("{0} (index {1})".format(d["name"], d["index"] + 1) for d in matches)
                return "Error: Multiple devices named '{0}': {1}".format(device_name, match_list)
            di = matches[0]["index"]
        elif di is None:
            di = 0

        result = ableton.send_command("delete_device", {
            "track_index": ti,
            "device_index": di,
        })

        return "Deleted {0}. {1} devices remaining.".format(
            result.get("deleted_device", "device"),
            result.get("remaining_devices", "?"))
    except Exception as e:
        logger.error(f"Error deleting device: {str(e)}")
        return f"Error deleting device: {str(e)}"


@mcp.tool()
def get_track_deletion_status(ctx: Context) -> str:
    """Check whether session tracks can be deleted right now.

    Returns a quick safety summary so agents can avoid attempting deletes
    when Ableton's minimum-track constraint would block them.
    """
    try:
        ableton = get_ableton_connection()
        info = ableton.send_command("get_session_info")
        track_count = info.get("track_count", 0)
        max_deletions_now = max(0, track_count - 1)

        if track_count <= 1:
            return (
                "Track deletion blocked: 1 session track remaining. "
                "Ableton requires at least one session track. "
                "Create a new track before deleting."
            )

        return (
            "Track deletion available: {0} session tracks currently exist. "
            "You can delete up to {1} more track(s) before hitting Ableton's "
            "minimum-track limit."
        ).format(track_count, max_deletions_now)
    except Exception as e:
        logger.error(f"Error checking track deletion status: {str(e)}")
        return f"Error checking track deletion status: {str(e)}"


@mcp.tool()
def delete_track(
    ctx: Context,
    track_index: int = 0,
    track_name: str = "",
) -> str:
    """Delete a track from the Ableton session.

    Parameters:
    - track_index: Track number (1-based). Use 0 to resolve by name instead.
    - track_name: Track name (alternative to track_index). If both are given, track_index takes priority.
    """
    try:
        ableton = get_ableton_connection()
        info = ableton.send_command("get_session_info")
        track_count = info.get("track_count", 0)

        # Safety guard: Ableton requires at least one session track.
        if track_count <= 1:
            return (
                "Error: Cannot delete the last remaining session track. "
                "Ableton must always have at least one track. "
                "Create a new track before deleting."
            )

        # Resolve by name if no index given
        if track_index <= 0:
            if not track_name:
                return "Error: provide either track_index (1-based) or track_name."
            matched_index = None
            for i in range(track_count):
                t = ableton.send_command("get_track_info", {"track_index": i})
                if t.get("name", "").lower() == track_name.lower():
                    matched_index = i
                    break
            if matched_index is None:
                return f"Error: No track named '{track_name}' found."
            ti = matched_index
        else:
            ti = _to_zero_based(track_index, "track_index")

        result = ableton.send_command("delete_track", {"track_index": ti})
        return "Deleted track '{0}'. {1} tracks remaining.".format(
            result.get("deleted_track", "unknown"),
            result.get("remaining_tracks", "?"),
        )
    except Exception as e:
        logger.error(f"Error deleting track: {str(e)}")
        return f"Error deleting track: {str(e)}"


@mcp.tool()
def navigate_device_preset(
    ctx: Context,
    track_index: int,
    device_index: int = 1,
    chain_index: int = 0,
    direction: str = "next",
) -> str:
    """Navigate device presets (next/previous/current).

    Parameters:
    - track_index: Track number (1-based).
    - device_index: Device number (1-based, default 1).
    - chain_index: Chain number inside a rack (1-based, 0 = no chain).
    - direction: "next", "previous", or "current".
    """
    try:
        ableton = get_ableton_connection()
        ti = _to_zero_based(track_index, "track_index")
        di = _to_zero_based(device_index, "device_index")
        ci = _optional_to_zero_based(chain_index, "chain_index")
        result = ableton.send_command("navigate_preset", {
            "track_index": ti,
            "device_index": di,
            "chain_index": ci,
            "direction": direction,
        })

        preset_name = result.get("preset_name", "?")
        preset_idx = result.get("preset_index", 0)
        preset_count = result.get("preset_count", 0)
        device_n = result.get("device_name", "?")

        if direction == "current":
            return "{0}: current preset is '{1}' ({2}/{3})".format(
                device_n, preset_name, preset_idx + 1, preset_count)
        return "{0}: loaded preset '{1}' ({2}/{3})".format(
            device_n, preset_name, preset_idx + 1, preset_count)
    except Exception as e:
        logger.error(f"Error navigating preset: {str(e)}")
        return f"Error navigating preset: {str(e)}"


# ---------------------------------------------------------------------------
# Extensions maison : pistes audio, mute/solo/arm, sends, master, introspection.
#
# Convention d'index de piste pour ces outils : 1..n = pistes normales,
# n+1..n+r = pistes de retour, 0 = piste master.
# ---------------------------------------------------------------------------


def _to_remote_track_index(track_index: int, field_name: str = "track_index") -> int:
    """Convert a 1-based MCP track index to the Remote Script's convention.

    0 means the master track, which the Remote Script addresses as -1.
    """
    if track_index < 0:
        raise ValueError(
            f"{field_name} must be >= 0 (0 = master, 1+ = 1-based), got {track_index}"
        )
    return -1 if track_index == 0 else track_index - 1


@mcp.tool()
def create_audio_track(ctx: Context, index: int = 0) -> str:
    """Create a new audio track — needed to host samples, loops or recordings.

    Parameters:
    - index: 1-based position for the new track. 0 (default) appends it at the end.
    """
    try:
        ableton = get_ableton_connection()
        remote_index = -1 if index == 0 else _to_zero_based(index, "index")
        result = ableton.send_command("create_audio_track", {"index": remote_index})
        return (
            f"Created audio track '{result.get('name', '?')}' "
            f"at position {result.get('index', 0) + 1}"
        )
    except Exception as e:
        logger.error(f"Error creating audio track: {str(e)}")
        return f"Error creating audio track: {str(e)}"


@mcp.tool()
def create_return_track(ctx: Context) -> str:
    """Create a new return track, to host a shared effect (reverb, delay) fed by sends."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_return_track", {})
        return (
            f"Created return track '{result.get('name', '?')}' — "
            f"address it as track_index {result.get('track_index', 0) + 1}"
        )
    except Exception as e:
        logger.error(f"Error creating return track: {str(e)}")
        return f"Error creating return track: {str(e)}"


@mcp.tool()
def set_track_state(
    ctx: Context,
    track_index: int,
    mute: bool | None = None,
    solo: bool | None = None,
    arm: bool | None = None,
) -> str:
    """Mute, solo or arm a track. Only the flags you pass are changed.

    Parameters:
    - track_index: 1-based track number (0 = master, which cannot be muted or soloed).
    - mute: True to mute, False to unmute.
    - solo: True to solo, False to unsolo.
    - arm: True to record-arm, False to disarm.
    """
    try:
        ableton = get_ableton_connection()
        params: Dict[str, Any] = {
            "track_index": _to_remote_track_index(track_index)
        }
        if mute is not None:
            params["mute"] = mute
        if solo is not None:
            params["solo"] = solo
        if arm is not None:
            params["arm"] = arm
        if len(params) == 1:
            return "Nothing to do: pass at least one of mute, solo or arm."
        result = ableton.send_command("set_track_state", params)
        return (
            f"'{result.get('track_name', '?')}': mute={result.get('mute')}, "
            f"solo={result.get('solo')}, arm={result.get('arm')}"
        )
    except Exception as e:
        logger.error(f"Error setting track state: {str(e)}")
        return f"Error setting track state: {str(e)}"


@mcp.tool()
def get_sends(ctx: Context, track_index: int) -> str:
    """List a track's send levels, each named after the return track it feeds.

    Parameters:
    - track_index: 1-based track number.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_sends", {
            "track_index": _to_remote_track_index(track_index)
        })
        sends = result.get("sends", [])
        if not sends:
            return (
                f"'{result.get('track_name', '?')}' has no sends "
                "(the set has no return track yet — use create_return_track)."
            )
        lines = [f"Sends of '{result.get('track_name', '?')}':"]
        for send in sends:
            lines.append(
                f"  send_index {send['index'] + 1} -> '{send['name']}': {send['value']:.4f}"
            )
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting sends: {str(e)}")
        return f"Error getting sends: {str(e)}"


@mcp.tool()
def set_send(ctx: Context, track_index: int, send_index: int, value: float) -> str:
    """Set how much of a track is sent to a return track (reverb, delay, ...).

    Parameters:
    - track_index: 1-based track number.
    - send_index: 1-based send number, matching the return track order.
    - value: 0.0 = off, 1.0 = unity. Use get_sends to see the current levels.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_send", {
            "track_index": _to_remote_track_index(track_index),
            "send_index": _to_zero_based(send_index, "send_index"),
            "value": value,
        })
        return (
            f"Set send {send_index} of '{result.get('track_name', '?')}' "
            f"to {result.get('value', value):.4f}"
        )
    except Exception as e:
        logger.error(f"Error setting send: {str(e)}")
        return f"Error setting send: {str(e)}"


@mcp.tool()
def get_mixer_info(ctx: Context) -> str:
    """Full mixer snapshot in one call: every track, return and master.

    Reports volume, panning, mute, solo and send levels. Cheaper and more
    complete than calling get_track_info track by track.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_mixer_info", {})

        def describe(entry: Dict[str, Any], label: str) -> str:
            flags = []
            if entry.get("mute"):
                flags.append("MUTED")
            if entry.get("solo"):
                flags.append("SOLO")
            sends = entry.get("sends") or []
            active = [
                f"{s['name']}={s['value']:.2f}" for s in sends if s["value"] > 0.001
            ]
            if active:
                flags.append("sends: " + ", ".join(active))
            suffix = f" [{'; '.join(flags)}]" if flags else ""
            return (
                f"  {label} '{entry.get('name', '?')}': "
                f"vol={entry.get('volume', 0):.3f} pan={entry.get('panning', 0):+.2f}"
                f"{suffix}"
            )

        lines = [f"Mixer @ {result.get('tempo', 0):.2f} BPM"]
        tracks = result.get("tracks", [])
        lines.append(f"Tracks ({len(tracks)}):")
        for entry in tracks:
            kind = "audio" if entry.get("is_audio_track") else "midi"
            if entry.get("is_group_track"):
                kind = "group"
            lines.append(describe(entry, f"{entry['index'] + 1}. [{kind}]"))
        returns = result.get("returns", [])
        if returns:
            lines.append(f"Returns ({len(returns)}):")
            for entry in returns:
                lines.append(describe(entry, f"{entry['index'] + 1}. [return]"))
        master = result.get("master")
        if master:
            lines.append("Master (track_index 0):")
            lines.append(describe(master, "0. [master]"))
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting mixer info: {str(e)}")
        return f"Error getting mixer info: {str(e)}"


@mcp.tool()
def set_clip_fade(
    ctx: Context,
    track_index: int,
    clip_index: int,
    fade_in: float = 0.0,
    fade_out: float = 0.0,
    parameter_name: str = "Track Volume",
) -> str:
    """Ramp a parameter in and/or out across a SESSION clip.

    ONLY SESSION CLIPS. Arrangement clips reject every envelope call in Live
    12.4.3 ("Not a session clip"), and arrangement automation is not writable
    through the LOM at all. To fade an extract in the arrangement, bake the
    fade into the audio file before importing it (ffmpeg afade) or draw it by
    hand in Live — there is no third way.

    The ramp ends at the parameter's current value, not its maximum: Track
    Volume runs 0.0-1.0 with unity at 0.85, so ramping to max would land a
    fade-in 6 dB above the level the track was set to.

    Parameters:
    - track_index: 1-based track number.
    - clip_index: 1-based SESSION clip slot on that track.
    - fade_in: fade-in duration in beats (0 = none).
    - fade_out: fade-out duration in beats (0 = none).
    - parameter_name: parameter to automate. Live calls the mixer parameters
      "Track Volume" and "Track Panning"; "volume" and "pan" are accepted.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_clip_fade", {
            "track_index": _to_zero_based(track_index, "track_index"),
            "clip_index": _to_zero_based(clip_index, "clip_index"),
            "fade_in": fade_in,
            "fade_out": fade_out,
            "parameter_name": parameter_name,
            "session": True,
        })
        return (
            f"'{result.get('clip_name', '?')}' on '{result.get('track_name', '?')}': "
            f"{result.get('parameter', '?')} fades in over {result.get('fade_in', 0):.2f} "
            f"beats and out over {result.get('fade_out', 0):.2f} beats "
            f"(clip is {result.get('clip_length', 0):.2f} beats long, "
            f"ramping to {result.get('ramps_to', 0):.3f})"
        )
    except Exception as e:
        logger.error(f"Error setting clip fade: {str(e)}")
        return f"Error setting clip fade: {str(e)}"


@mcp.tool()
def get_clip_notes(
    ctx: Context,
    track_index: int,
    clip_index: int = 1,
    arrangement: bool = False,
) -> str:
    """Read the MIDI notes of a clip — the other half of add_notes_to_clip.

    Writing notes without being able to read them back is half a loop: use this
    to check what actually landed, before transposing, fixing or replacing it.

    Times are in beats from the clip start.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip number (1-based). A session slot unless arrangement=True.
    - arrangement: True to read an arrangement clip instead of a session slot.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_clip_notes", {
            "track_index": _to_zero_based(track_index, "track_index"),
            "clip_index": _to_zero_based(clip_index, "clip_index"),
            "arrangement": arrangement,
        })
        notes = result.get("notes", [])
        lines = [
            f"'{result.get('clip_name', '?')}' on '{result.get('track_name', '?')}' — "
            f"{result.get('note_count', 0)} note(s), "
            f"clip is {result.get('clip_length', 0):.3f} beats"
        ]
        for note in notes:
            extra = ""
            if note.get("mute"):
                extra += " MUTED"
            if note.get("probability") not in (None, 1.0):
                extra += f" p={note['probability']:.2f}"
            lines.append(
                "  beat {0:8.3f}  pitch {1:3d}  dur {2:6.3f}  vel {3:3.0f}{4}".format(
                    note.get("start_time", 0.0), int(note.get("pitch", 0)),
                    note.get("duration", 0.0), note.get("velocity", 0.0), extra))
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error getting clip notes: {str(e)}")
        return f"Error getting clip notes: {str(e)}"


@mcp.tool()
def remove_clip_notes(
    ctx: Context,
    track_index: int,
    clip_index: int = 1,
    from_time: float = 0.0,
    time_span: float = None,
    from_pitch: int = 0,
    pitch_span: int = 128,
    arrangement: bool = False,
) -> str:
    """Delete MIDI notes from a clip, inside a time and pitch window.

    add_notes_to_clip only ever adds, so this is how a part gets corrected:
    clear, then add again. The defaults clear the whole clip.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Clip number (1-based). A session slot unless arrangement=True.
    - from_time: window start, in beats from the clip start.
    - time_span: window length in beats. None = to the end of the clip.
    - from_pitch: lowest MIDI note to clear (0-127).
    - pitch_span: how many semitones up from from_pitch. 128 = all.
    - arrangement: True to edit an arrangement clip instead of a session slot.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("remove_clip_notes", {
            "track_index": _to_zero_based(track_index, "track_index"),
            "clip_index": _to_zero_based(clip_index, "clip_index"),
            "from_time": from_time,
            "time_span": time_span,
            "from_pitch": from_pitch,
            "pitch_span": pitch_span,
            "arrangement": arrangement,
        })
        line = (
            f"'{result.get('clip_name', '?')}' on '{result.get('track_name', '?')}': "
            f"removed {result.get('removed', 0)} note(s), "
            f"{result.get('notes_before', 0)} -> {result.get('notes_after', 0)}"
        )
        if result.get("WARNING"):
            line += "\n  WARNING: " + result["WARNING"]
        return line
    except Exception as e:
        logger.error(f"Error removing clip notes: {str(e)}")
        return f"Error removing clip notes: {str(e)}"


@mcp.tool()
def delete_session_clip(ctx: Context, track_index: int, clip_index: int = 1) -> str:
    """Empty a session clip slot.

    Parameters:
    - track_index: Track number (1-based).
    - clip_index: Session clip slot (1-based).
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("delete_session_clip", {
            "track_index": _to_zero_based(track_index, "track_index"),
            "clip_index": _to_zero_based(clip_index, "clip_index"),
        })
        if not result.get("deleted"):
            return (f"Slot {clip_index} of '{result.get('track_name', '?')}' "
                    f"was already empty")
        return (f"Deleted '{result.get('clip_name', '')}' from slot {clip_index} "
                f"of '{result.get('track_name', '?')}'")
    except Exception as e:
        logger.error(f"Error deleting session clip: {str(e)}")
        return f"Error deleting session clip: {str(e)}"


@mcp.tool()
def inspect_lom(ctx: Context, path: str = "") -> str:
    """Introspect Ableton's Live Object Model at runtime to discover its real API.

    Live's Python API changes between versions and the documentation lags behind,
    so use this instead of guessing whether a property exists or is writable.

    Parameters:
    - path: dotted path from the Song object, with integer indexing. Empty = the
      Song itself. Examples: "tracks[0]", "tracks[0].arrangement_clips[0]",
      "master_track.mixer_device.volume", "view".
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("inspect_lom", {"path": path})
        lines = [
            f"{result.get('type', '?')} at Song.{result.get('path') or '<root>'}",
            "Properties:",
        ]
        for name, value in (result.get("attributes") or {}).items():
            lines.append(f"  {name} = {value!r}")
        methods = result.get("methods") or []
        if methods:
            lines.append("Methods: " + ", ".join(methods))
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error inspecting LOM: {str(e)}")
        return f"Error inspecting LOM: {str(e)}"


# Main execution
def main():
    """Run the MCP server"""
    mcp.run()

if __name__ == "__main__":
    main()
