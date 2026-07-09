"""
Entry point for running the met_extract package as a module.

Usage:
  python -m met_extract run --domain SA --date 2016
  python -m met_extract make-native-grid --mk 10
"""

from .cli import main

if __name__ == '__main__':
    main()
