"""
Services module - Business logic and data processing
"""

from .contribution_processor import ContributionProcessor
from .preset_manager import PresetManager

__all__ = [
    'ContributionProcessor',
    'PresetManager',
]
