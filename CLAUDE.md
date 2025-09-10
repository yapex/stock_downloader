# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **quantitative stock data downloader** for Chinese A-share markets using Tushare Pro API. The project follows a **data lake architecture** with producer-consumer pattern and dependency injection.

## Development Environment

**Package Management**: Uses `uv` instead of pip. All Python commands should be run with `uv run`.

**Python Version**: Requires Python 3.13+

**Key Dependencies**:
- **DuckDB**: Embedded OLAP database for metadata management
- **Tushare Pro**: Chinese stock market data API
- **Parquet**: Columnar storage format for data lake
- **Huey**: Task queue system with 3 isolated queues
- **Dependency Injector**: IoC container for service management

## Architecture Overview

### Core Architectural Patterns

1. **Dependency Injection**: All services managed through `AppContainer` in `src/neo/containers.py`
2. **Producer-Consumer**: Isolated task queues separate download (fast) from processing (slow)
3. **Data Lake**: Parquet files with DuckDB metadata, partitioned by year

### Three-Queue System

- **`huey_fast`**: High-concurrency API downloads (8 workers)
- **`huey_slow`**: Single-threaded data processing/writing (1 worker) 
- **`huey_maint`**: Maintenance tasks like metadata sync (1 worker)

### Key Components

**Producer (`dl` command)**:
- `AppService`: Entry point, coordinates download flow
- `DownloaderService`: Submits tasks to fast queue
- `SimpleDownloader`: Core API data fetcher
- `FetcherBuilder`: Creates task-specific data fetchers
- `RateLimitManager`: Global API rate limiting

**Consumer (`dp` command)**:
- `ConsumerRunner`: Manages queue consumers
- `SimpleDataProcessor`: Unified data processor with strategy selection
- `ParquetWriter`: Handles both incremental and full-replace writes
- `ParquetDBQueryer`: Queries Parquet metadata via DuckDB

**Data Storage**:
- **Parquet files**: Partitioned by year in `data/parquet/`
- **Metadata DB**: `data/metadata.db` (DuckDB, schema/file manifest only)
- **Legacy DB**: `data/stock.db` (old physical data storage)

## Common Commands

### Development Commands
```bash
# Run tests
uv run poe test
uv run poe test-cov
uv run poe test-fast

# Code quality
uv run poe lint          # Ruff linting
uv run poe lint-fix      # Auto-fix linting issues
uv run poe format        # Format code
uv run poe format-check  # Check formatting
uv run poe type-check    # MyPy type checking

# Combined tasks
uv run poe check         # lint + format-check + type-check
uv run poe fix           # lint-fix + format
uv run poe ci            # check + test-cov
```

### Application Commands
```bash
# Download data (producer)
uv run neo dl --group <group_name>

# Start consumers (separate terminals)
uv run neo dp fast       # Fast queue consumer
uv run neo dp slow       # Slow queue consumer
uv run neo dp maint      # Maintenance queue consumer

# Helper scripts
uv run poe monitor       # Huey task monitor
uv run poe qdata         # Query Parquet data
uv run poe msync         # Manual metadata sync
```

## Configuration

### Main Config: `config.toml`
- **Task definitions**: Rate limits, update strategies, symbol grouping
- **Queue settings**: Worker counts, SQLite paths
- **Storage paths**: Parquet base directory, database locations
- **Schema config**: Path to `stock_schema.toml`

### Update Strategies
- **`incremental`**: Appends data with timestamped files (non-idempotent)
- **`full_replace`**: Atomic full replacement (idempotent, safe for repeats)

### Task Groups
Predefined groups in config: `sys`, `daily`, `hfq`, `financial`, `all`, `test`

## Development Guidelines

### Code Style
- **Ruff**: Configured for Python 3.13, ignores E501 (line length)
- **MyPy**: Strict type checking enabled
- **Testing**: pytest with coverage, use `unittest.mock` for external dependencies

### Adding New Data Types (Metadata-Driven Approach)

This project uses a **metadata-driven architecture** - adding new data types requires only configuration changes, no new fetcher classes!

1. **Add Schema Configuration**: In `scripts/create_schema.py`, add table configuration to `TABLE_CONFIGS`:
   ```python
   "dividend": {
       "table_name": "dividend",
       "primary_key": ["ts_code", "end_date"],
       "date_col": "end_date", 
       "description": "分红送股数据",
       "api_method": "dividend",
       "base_object": "pro",
       "default_params": {"ts_code": "600519.SH"},
       "fields": [],
       "output_file": "stock_schema.toml",
   }
   ```

2. **Generate Schema**: Run the schema generator:
   ```bash
   uv run python scripts/create_schema.py --tables dividend
   ```

3. **Add Task Configuration**: In `config.toml`, add task settings:
   ```toml
   [download_tasks.dividend]
   rate_limit_per_minute = 195
   update_strategy = "incremental"
   update_by_symbol = true
   ```

4. **Update Task Groups**: Add to existing or create new task groups:
   ```toml
   [task_groups]
   dividend = ["dividend"]
   all = ["stock_daily", "daily_basic", "dividend", ...]
   ```

**How it works**:
- `FetcherBuilder` dynamically creates API fetchers from `stock_schema.toml`
- `SimpleDataProcessor` automatically selects strategy based on `update_strategy` config
- `RateLimitManager` applies task-specific rate limiting
- Zero code changes needed for new data types!

### Database Schema
- Schema defined in `stock_schema.toml`
- Loaded via `SchemaLoader` singleton
- Parquet partitioning: `{table_name}/year={YYYY}/data.parquet`

## Testing

### Test Structure
- **Unit tests**: Focus on individual components with mocked dependencies
- **Integration tests**: Test component interactions
- **Fixtures**: Use `tests/fixtures/` for test data
- **Mocking**: Prefer `unittest.mock` over patch decorators

### Running Tests
```bash
# All tests with coverage
uv run poe test-cov

# Fast test run (stop on first failure)
uv run poe test-fast

# Verbose output
uv run poe test-verbose
```

## Data Processing Patterns

### Producer Flow
1. `dl` command → `build_and_enqueue_downloads_task` (slow queue)
2. Queries existing data via `ParquetDBQueryer`
3. Calculates incremental ranges
4. Enqueues `download_task` to fast queue

### Consumer Flow
1. `download_task` → API fetch → `process_data_task` (slow queue)
2. `process_data_task` → `SimpleDataProcessor`
3. Processor selects strategy based on config
4. Writes via `ParquetWriter` (incremental or full-replace)

## File Structure Conventions

```
src/neo/
├── containers.py          # DI container definition
├── main.py                # CLI entry point
├── app.py                 # Container instance
├── database/              # Database operations
├── downloader/            # Data fetching logic
├── tasks/                 # Huey task definitions
├── writers/               # Data writing (Parquet)
├── data_processor/        # Data processing
├── helpers/               # Utility services
├── services/              # Business services
└── configs/               # Configuration management
```

## Important Notes

- **Always use `uv run`** for Python commands to maintain environment consistency
- **Task isolation**: Fast queue for I/O, slow queue for disk operations to prevent conflicts
- **Data integrity**: Use appropriate update strategies to avoid data duplication
- **Testing**: Mock all external dependencies (APIs, filesystem, databases)
- **Type safety**: MyPy strict mode enabled, ensure all code passes type checking
- 启动huey服务: poe huey-all