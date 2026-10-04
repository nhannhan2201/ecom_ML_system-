"""
================================================================================
MODULE: LATE EVENT BUFFER (STREAMING EVENT-TIME DELAY QUEUE)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Purpose:
  Provides an isolated, testable queue for buffering delayed streaming events
  based on event-time progression (Simulating Late Arrival / Out-of-Order).
================================================================================
"""

from datetime import datetime
from typing import Dict, List, Tuple, Any
import numpy as np


class LateEventBuffer:
    """
    Buffer managing delayed streaming events based on Event Time.
    Events are released when the stream's current event time passes release_dt.
    """

    def __init__(self):
        # Buffer stores tuples of (release_dt, event_payload)
        self._buffer: List[Tuple[datetime, Dict[str, Any]]] = []
        self._delays_minutes: List[float] = []

    def add(self, release_dt: datetime, event: Dict[str, Any], delay_minutes: float = 0.0) -> None:
        """
        Add an event to the late buffer scheduled to be released at release_dt.
        """
        self._buffer.append((release_dt, event))
        if delay_minutes > 0.0:
            self._delays_minutes.append(float(delay_minutes))

    def release_ready(self, curr_dt: datetime) -> List[Dict[str, Any]]:
        """
        Release and return all events whose release_dt <= curr_dt.
        Maintains remaining unready events in buffer.
        """
        ready: List[Dict[str, Any]] = []
        remaining: List[Tuple[datetime, Dict[str, Any]]] = []

        for target_dt, ev in self._buffer:
            if curr_dt >= target_dt:
                ready.append(ev)
            else:
                remaining.append((target_dt, ev))

        self._buffer = remaining
        return ready

    def release_all(self) -> List[Dict[str, Any]]:
        """
        Flush and return all buffered events regardless of release_dt.
        """
        flushed = [ev for _, ev in self._buffer]
        self._buffer.clear()
        return flushed

    def count(self) -> int:
        """Return the current number of buffered events."""
        return len(self._buffer)

    def __len__(self) -> int:
        return self.count()

    def get_delay_distribution(self) -> Dict[str, float]:
        """
        Calculate and return min, median, max, and mean delay in minutes.
        """
        if not self._delays_minutes:
            return {"min": 0.0, "median": 0.0, "max": 0.0, "mean": 0.0, "count": 0}

        arr = np.array(self._delays_minutes, dtype=np.float64)
        return {
            "min": round(float(np.min(arr)), 2),
            "median": round(float(np.median(arr)), 2),
            "max": round(float(np.max(arr)), 2),
            "mean": round(float(np.mean(arr)), 2),
            "count": len(self._delays_minutes),
        }
