# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **File Path Structure**: Removed "entity=" prefix from file paths for stock data
  - **Old format**: `data_type/entity=STOCK_CODE/data.parquet`
  - **New format**: `data_type/STOCK_CODE/data.parquet`
- **Automatic Stock Code Normalization**: All stock codes are now automatically normalized using `normalize_stock_code()` function
  - Supports various input formats: `600519`, `SH600519`, `600519SH`, `sh600519`
  - All are normalized to standard format: `600519.SH`

### Added
- **Legacy Path Support**: System automatically checks legacy "entity=" paths for backward compatibility
- **Enhanced Migration Support**: `get_latest_date()` method now checks both new and legacy path formats

### Migration Instructions

#### For Existing Data Directories

If you have existing data stored in the old format, you have several options:

##### Option 1: Automatic Migration (Recommended)
The system will automatically read from legacy paths when new paths don't exist. New data will be saved in the new format, allowing for gradual migration.

##### Option 2: Manual Migration Script
Create a migration script to rename your existing directories:

```python
#!/usr/bin/env python3
"""
Migration script to convert old file paths to new format.
Run this from your data directory root.
"""

import os
import shutil
from pathlib import Path

def migrate_data_directory(base_path: str):
    """Migrate data from old 'entity=' format to new format."""
    base_path = Path(base_path)
    
    if not base_path.exists():
        print(f"Base path {base_path} does not exist")
        return
    
    # Find all directories with 'entity=' prefix
    for data_type_dir in base_path.iterdir():
        if not data_type_dir.is_dir():
            continue
            
        for entity_dir in data_type_dir.iterdir():
            if entity_dir.is_dir() and entity_dir.name.startswith('entity='):
                # Extract stock code from entity=STOCK_CODE
                stock_code = entity_dir.name[7:]  # Remove 'entity=' prefix
                new_path = data_type_dir / stock_code
                
                print(f"Migrating: {entity_dir} -> {new_path}")
                
                # Move the directory
                if not new_path.exists():
                    shutil.move(str(entity_dir), str(new_path))
                    print(f"✅ Migrated {entity_dir.name} to {stock_code}")
                else:
                    print(f"⚠️  Target {new_path} already exists, skipping {entity_dir}")

if __name__ == "__main__":
    # Update this path to your data directory
    data_directory = "./data"
    migrate_data_directory(data_directory)
    print("Migration completed!")
```

##### Option 3: Fresh Start
If you prefer to start fresh:
1. Backup your existing data directory
2. Delete the old data directory
3. Run your download tasks again - they will create the new format

#### File Path Examples

**Before (Legacy Format):**
```
data/
├── daily_qfq/
│   ├── entity=600519.SH/
│   │   └── data.parquet
│   └── entity=000001.SZ/
│       └── data.parquet
└── financials/
    ├── entity=600519.SH/
    │   └── data.parquet
    └── entity=000001.SZ/
        └── data.parquet
```

**After (New Format):**
```
data/
├── daily_qfq/
│   ├── 600519.SH/
│   │   └── data.parquet
│   └── 000001.SZ/
│       └── data.parquet
└── financials/
    ├── 600519.SH/
    │   └── data.parquet
    └── 000001.SZ/
        └── data.parquet
```

#### Stock Code Normalization Examples

The system now automatically normalizes various stock code formats:

| Input Format | Normalized Output | Exchange |
|-------------|------------------|----------|
| `600519` | `600519.SH` | Shanghai |
| `SH600519` | `600519.SH` | Shanghai |
| `600519SH` | `600519.SH` | Shanghai |
| `sh600519` | `600519.SH` | Shanghai |
| `000001` | `000001.SZ` | Shenzhen |
| `SZ000001` | `000001.SZ` | Shenzhen |
| `000001SZ` | `000001.SZ` | Shenzhen |
| `sz000001` | `000001.SZ` | Shenzhen |
| `300001` | `300001.SZ` | Shenzhen (ChiNext) |

#### Breaking Changes
- Configuration files referencing specific file paths may need updating
- Custom scripts that directly access the file system may need path updates
- The `entity=` prefix is no longer used in new file paths

#### Compatibility Notes
- The system maintains backward compatibility by checking legacy paths
- Existing data remains accessible during the transition period
- New data is always saved using the new path format
