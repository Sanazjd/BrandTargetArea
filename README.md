# Brand Target Area Map

An interactive R Shiny application for visualizing brand target areas and P4 breeding locations across the United States.

## Features

- **Interactive Map**: Choropleth visualization of acreage by county
- **FieldView Data**: Filter by brand name and crop year
- **P4 Breeding Locations**: Visualize breeding sites with market and RM filters
- **Dynamic Filtering**: Real-time updates with selection counts

## Project Structure

```
BrandTargetArea/
├── appbrandtarget.R       # Main Shiny application
├── data/                  # Data files (parquet format)
│   ├── FV_brand_acreage.parquet
│   └── NA_market_P4_breeding_locationsII.parquet
├── scripts/               # Data preprocessing scripts
│   ├── convert_csv_to_parquet.py
│   └── GettingFVfromBigQueryMD.R
├── DESCRIPTION            # R package dependencies
├── .gitignore
└── README.md
```

## Requirements

- R >= 4.0
- Required R packages:
  - shiny
  - leaflet
  - sf
  - dplyr
  - tigris
  - DT
  - arrow

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/BrandTargetArea.git
   cd BrandTargetArea
   ```

2. Install R dependencies:
   ```r
   install.packages(c("shiny", "leaflet", "sf", "dplyr", "tigris", "DT", "arrow"))
   ```

3. Ensure data files are in the `data/` folder

## Usage

Run the Shiny app:

```r
shiny::runApp("appbrandtarget.R")
```

Or from the command line:

```bash
Rscript -e "shiny::runApp('appbrandtarget.R', port = 3838)"
```

The app will be available at `http://localhost:3838`

## Data Sources

- **FieldView Data**: Brand acreage data by county (FIPS codes)
- **P4 Breeding Locations**: Coordinates and metadata for breeding sites

## License

Internal use only - Bayer Crop Science

## Contributing

1. Create a feature branch
2. Make your changes
3. Submit a pull request
