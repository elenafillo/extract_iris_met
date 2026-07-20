"""
Entry point for running the extract_um_met package as a module.

Usage:
  python -m extract_um_met run --domain SA --date 2016
  python -m extract_um_met make-native-grid --mk 10
"""

from .cli import main

if __name__ == '__main__':
    main()
