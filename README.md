# nyamatrix

A re-calculator for bancho.py with elegant performance.

## Introduction

nyamatrix is a high-performance tool designed for recalculating osu! performance points (pp) in bancho.py environments. It features optimized processing capabilities to handle large datasets efficiently.

## Project Structure

```
nyamatrix/
├── nyamatrix/         # Main package
│   ├── __init__.py
│   └── main.py        # Entry point
|   └── processor.py   # Core calculation logic
│   └── statements.py  # Utility functions for SQLAlchemy
├── README.md          # This file
├── pyproject.toml     # Project metadata and dependencies
├── poetry.toml        # Poetry configuration
└── LICENSE            # License
```

## Requirements

- Python 3.12 or higher
- Poetry (dependency management)
- MySQL (bancho.py repository)
- Redis (bancho.py leaderboard)

## Usage

```bash
# Activate the poetry environment (if using poetry)
poetry shell

# Run the calculator with help option
poetry run nyacalc --help

# Or directly with the installed command
nyacalc --help
```

## Configuration

Configuration options can be provided via command line arguments or environment variables. For detailed information, run:

```bash
poetry run nyacalc --help
```

## Dependencies

- typer: Command-line interface
- rosu-pp-py: osu! pp calculation library
- sqlalchemy: Database ORM
- mysqlclient: MySQL client library
- redis: Redis client for caching
- tqdm: Progress bar for long-running operations
- coloredlogs: Enhanced logging output

## License

This project is licensed under the MIT License.

## Acknowledgements

- [bancho.py](https://github.com/osuAkatsuki/bancho.py) - The osu! server implementation
- [rosu-pp-py](https://github.com/ppy-sb/rosu-pp-py) - The performance calculation library
