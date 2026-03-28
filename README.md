# ML/MLOps Assessment Task 0

## Overview
A batch processing job that computes rolling mean signals on OHLCV data.

## Local Setup

### Prerequisites
- Python 3.x+ (preferably 3.9+)
- pip

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the script:
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Docker Setup

### Build
```bash
docker build -t mlops-task .
```

### Run
```bash
docker run --rm mlops-task
```

## Output Files

- `metrics.json` - metrics in JSON format
- `run.log` - detailed execution logs with timestamps (no overwriting)

## Example Output

### metrics.json (Success)
```json
{
    "version": "v1",
    "rows_processed": 10000,
    "metric": "signal_rate",
    "value": 0.4990,
    "latency_ms": 491,
    "seed": 42,
    "status": "success"
}
```

### metrics.json (Error)
- (Missing column error)
```json
{
    "version": "v1",
    "status": "error",
    "error_message": "Missing column in data.csv: close"
}
```

## Error Handling:
- Rolling Mean Handling : The first (window-1) rows have NaN rolling mean values since there aren't enough previous values to calculate the average. These rows are treated as signal=0 (no signal).
- No Columns Errors handled
- No Config details handled
- No file detected



## Files required for a succeful run:
- `run.py` - Main script
- `config.yaml` - Configuration (seed, window, version)
- `data.csv` - OHLCV data
- `requirements.txt` - Python dependencies
- `Dockerfile` - Docker configuration
