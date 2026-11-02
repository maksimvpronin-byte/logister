"""Compatibility entry point for the isolated acceptance suite.

Run `python3 deploy/tests/acceptance.py --help` for the current options.
Build the Docker images with `docker compose build` first.
"""
from acceptance import main


if __name__ == '__main__':
    main()
