"""
Log parser for drain test jobs.

Parses structured output from WindowedDrainTestJob to extract
window emissions and event processing information.
"""
import re
import logging
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
from enum import Enum

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Window trigger type."""
    NORMAL = "NORMAL"
    DRAIN = "DRAIN"


@dataclass
class WindowEmission:
    """Represents a window emission event."""
    key: str
    window_start: int
    window_end: int
    count: int
    sum_value: int
    min_event_time: int
    max_event_time: int
    watermark: int
    trigger: TriggerType
    wall_time: int

    @property
    def window_id(self) -> str:
        """Unique identifier for this window."""
        return f"{self.key}:[{self.window_start}-{self.window_end}]"

    @property
    def is_drain_triggered(self) -> bool:
        """Check if this emission was triggered by drain."""
        return self.trigger == TriggerType.DRAIN


@dataclass
class EventProcess:
    """Represents an event processing log entry."""
    key: str
    window_start: int
    window_end: int
    event_time: int
    value: int
    wall_time: int

    @property
    def window_id(self) -> str:
        """Window this event belongs to."""
        return f"{self.key}:[{self.window_start}-{self.window_end}]"


class DrainLogParser:
    """
    Parse structured logs from WindowedDrainTestJob.

    Expected log formats:
    - WINDOW_EMIT|key=X|window=[start-end]|count=N|sum=S|min_event_time=T1|max_event_time=T2|watermark=W|trigger=NORMAL|DRAIN|wall_time=T
    - EVENT_PROCESS|key=X|window=[start-end]|event_time=T|value=V|wall_time=T
    """

    # Regex patterns for parsing
    WINDOW_PATTERN = re.compile(
        r'WINDOW_EMIT\|'
        r'key=([^\|]+)\|'
        r'window=\[(\d+)-(\d+)\]\|'
        r'count=(\d+)\|'
        r'sum=(\d+)\|'
        r'min_event_time=(\d+)\|'
        r'max_event_time=(\d+)\|'
        r'watermark=(-?\d+)\|'
        r'trigger=(\w+)\|'
        r'wall_time=(\d+)'
    )

    EVENT_PATTERN = re.compile(
        r'EVENT_PROCESS\|'
        r'key=([^\|]+)\|'
        r'window=\[(\d+)-(\d+)\]\|'
        r'event_time=(\d+)\|'
        r'value=(\d+)\|'
        r'wall_time=(\d+)'
    )

    # MAX_WATERMARK as defined in Flink
    MAX_WATERMARK = 9223372036854775806  # Long.MAX_VALUE - 1

    def parse_logs(self, logs: str) -> Tuple[List[WindowEmission], List[EventProcess]]:
        """
        Parse logs and extract window emissions and event processing entries.

        Args:
            logs: Raw log string (can be multi-line)

        Returns:
            Tuple of (window_emissions, event_processes)
        """
        windows = []
        events = []

        for line in logs.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Try to match window emission
            window_match = self.WINDOW_PATTERN.search(line)
            if window_match:
                try:
                    trigger_str = window_match.group(9)
                    trigger = TriggerType(trigger_str)
                except ValueError:
                    logger.warning(f"Unknown trigger type: {trigger_str}")
                    trigger = TriggerType.NORMAL

                windows.append(WindowEmission(
                    key=window_match.group(1),
                    window_start=int(window_match.group(2)),
                    window_end=int(window_match.group(3)),
                    count=int(window_match.group(4)),
                    sum_value=int(window_match.group(5)),
                    min_event_time=int(window_match.group(6)),
                    max_event_time=int(window_match.group(7)),
                    watermark=int(window_match.group(8)),
                    trigger=trigger,
                    wall_time=int(window_match.group(10))
                ))
                continue

            # Try to match event process
            event_match = self.EVENT_PATTERN.search(line)
            if event_match:
                events.append(EventProcess(
                    key=event_match.group(1),
                    window_start=int(event_match.group(2)),
                    window_end=int(event_match.group(3)),
                    event_time=int(event_match.group(4)),
                    value=int(event_match.group(5)),
                    wall_time=int(event_match.group(6))
                ))

        logger.info(f"Parsed {len(windows)} window emissions, {len(events)} event processes")
        return windows, events

    def get_drain_triggered_windows(self, windows: List[WindowEmission]) -> List[WindowEmission]:
        """Filter to only drain-triggered emissions."""
        return [w for w in windows if w.is_drain_triggered]

    def get_normal_windows(self, windows: List[WindowEmission]) -> List[WindowEmission]:
        """Filter to only normal (non-drain) emissions."""
        return [w for w in windows if not w.is_drain_triggered]

    def get_windows_by_key(self, windows: List[WindowEmission], key: str) -> List[WindowEmission]:
        """Get all windows for a specific key."""
        return [w for w in windows if w.key == key]

    def get_windows_after_time(self, windows: List[WindowEmission], wall_time: int) -> List[WindowEmission]:
        """Get windows emitted after a specific wall time."""
        return [w for w in windows if w.wall_time > wall_time]

    def get_unique_keys(self, windows: List[WindowEmission]) -> set:
        """Get set of unique keys from window emissions."""
        return set(w.key for w in windows)

    def get_windows_by_window_id(self, windows: List[WindowEmission]) -> Dict[str, List[WindowEmission]]:
        """Group windows by their window ID (key + time range)."""
        result = {}
        for w in windows:
            if w.window_id not in result:
                result[w.window_id] = []
            result[w.window_id].append(w)
        return result

    def find_duplicate_emissions(self, windows: List[WindowEmission]) -> Dict[str, List[WindowEmission]]:
        """
        Find windows that were emitted multiple times.

        This can happen if a window is emitted normally and then
        again during drain.
        """
        by_id = self.get_windows_by_window_id(windows)
        return {wid: wlist for wid, wlist in by_id.items() if len(wlist) > 1}

    def summarize(self, windows: List[WindowEmission], events: List[EventProcess]) -> dict:
        """
        Generate a summary of the parsed data.

        Returns dict with:
        - total_windows: Total window emissions
        - drain_windows: Number of drain-triggered emissions
        - normal_windows: Number of normal emissions
        - unique_keys: Set of unique keys
        - duplicate_windows: Windows emitted more than once
        - total_events: Total events processed
        """
        drain_windows = self.get_drain_triggered_windows(windows)
        normal_windows = self.get_normal_windows(windows)
        duplicates = self.find_duplicate_emissions(windows)

        return {
            'total_windows': len(windows),
            'drain_windows': len(drain_windows),
            'normal_windows': len(normal_windows),
            'unique_keys': self.get_unique_keys(windows),
            'duplicate_windows': len(duplicates),
            'duplicate_details': duplicates,
            'total_events': len(events),
            'keys_with_drain': set(w.key for w in drain_windows),
        }

    def verify_drain_behavior(self, windows: List[WindowEmission], expected_keys: set) -> Tuple[bool, str]:
        """
        Verify that drain behavior occurred as expected.

        Args:
            windows: List of window emissions
            expected_keys: Set of keys that should have drain emissions

        Returns:
            Tuple of (success, message)
        """
        drain_windows = self.get_drain_triggered_windows(windows)

        if not drain_windows:
            return False, "No drain-triggered windows found"

        drain_keys = set(w.key for w in drain_windows)
        missing_keys = expected_keys - drain_keys

        if missing_keys:
            return False, f"Missing drain emissions for keys: {missing_keys}"

        # Check that drain watermark is MAX_WATERMARK
        for w in drain_windows:
            if w.watermark < self.MAX_WATERMARK:
                return False, f"Drain window has unexpected watermark: {w.watermark} (expected >= {self.MAX_WATERMARK})"

        return True, f"Drain behavior verified: {len(drain_windows)} windows from {len(drain_keys)} keys"

    def verify_no_drain(self, windows: List[WindowEmission]) -> Tuple[bool, str]:
        """
        Verify that no drain behavior occurred.

        Args:
            windows: List of window emissions

        Returns:
            Tuple of (success, message)
        """
        drain_windows = self.get_drain_triggered_windows(windows)

        if drain_windows:
            return False, f"Unexpected drain-triggered windows: {len(drain_windows)}"

        return True, "No drain behavior detected (as expected)"
