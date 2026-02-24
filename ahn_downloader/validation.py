"""Shared validation utilities for LAZ/LAS files."""

from pathlib import Path

import laspy


def is_valid_laz_file(path: Path) -> tuple[bool, str | None]:
    """Check if a LAZ/LAS file has valid data.
    
    Args:
        path: Path to the LAZ/LAS file
        
    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if file is valid
        - error_message: None if valid, otherwise description of the problem
    """
    try:
        with laspy.open(str(path)) as reader:
            hdr = reader.header
            
            # Check for empty file
            if hdr.point_count == 0:
                return False, "Empty file: 0 points"
            
            # Check for zero bounding box
            x_range = hdr.x_max - hdr.x_min
            y_range = hdr.y_max - hdr.y_min
            z_range = hdr.z_max - hdr.z_min
            
            if x_range == 0 or y_range == 0 or z_range == 0:
                return False, f"Zero bounding box: X={x_range:.3f}, Y={y_range:.3f}, Z={z_range:.3f}"
            
            return True, None
            
    except Exception as exc:
        return False, f"Failed to read: {exc}"


def get_bbox_info(path: Path) -> tuple[float, float, float] | None:
    """Get bounding box ranges (X, Y, Z) for a LAZ/LAS file.
    
    Returns:
        Tuple of (x_range, y_range, z_range) or None on error
    """
    try:
        with laspy.open(str(path)) as reader:
            hdr = reader.header
            return (
                hdr.x_max - hdr.x_min,
                hdr.y_max - hdr.y_min,
                hdr.z_max - hdr.z_min
            )
    except Exception:
        return None
