# Ques3

This Scala application is built using Apache Spark and processes export data by aggregating, categorizing, and writing data to a file system. The application accepts a year and a commodity as input, then categorizes countries based on export volumes into "big", "medium", and "small" exporters. The output is written to directories partitioned by category for efficient data organization.

## Requirements

- **Scala**: Ensure Scala is installed.
- **Apache Spark**: This application is built on Apache Spark, so you need to have Spark installed.
- **Data File**: The application processes a CSV file with export data. Ensure your CSV has the following columns:
  - `year`
  - `commodity`
  - `country`
  - `value`
  - `HSCode`

## Setup

1. Clone the repository.
2. Open the project in your IDE (e.g., IntelliJ).
3. Adjust file paths in the code as needed:
   - `filePath`: Path to the input data file.
   - `outputPath`: Path for storing output data.
   
## Application Components

### 1. `ExportDataJob`

The `ExportDataJob` object contains methods for loading, processing, and writing export data. Below is a breakdown of each component:

- **`loadData`**: Reads the CSV file, filters the data by year and commodity, and checks if required columns are present.
  
- **`aggregateExportsByCountry`**: Aggregates export data by country.

- **`categorizeCountries`**: Categorizes countries based on export value thresholds:
  - `big`: `total_export > 10`
  - `medium`: `total_export > 1`
  - `small`: All other values.

- **`savePartitionedData`**: Writes the categorized data to an output path, partitioned by category (`big`, `medium`, `small`).

- **`runJob`**: Orchestrates the entire process from loading to writing data.

### 2. `Main Method`

The `main` method accepts command-line arguments for `year` and `commodity`, loads data, aggregates exports, categorizes countries, and writes the output data.


## Output Structure

<img width="265" alt="image" src="https://github.com/user-attachments/assets/76283de8-cf4e-438d-8fc6-155c9bea03f4">

## Sample Input
1. Command Line arguments
    - **`<Year,Commodity>`**
3. [2018-2010_export.csv](https://drive.google.com/file/d/1ev-30a1L0okJSGhlfqndSNr_QDqjS4-p/view?usp=sharing)

## Sample Output

<img width="265" alt="image" src="https://github.com/user-attachments/assets/72cc2179-f155-4b68-b223-0e5485e2bedd">

## Testing

This `ExportDataJobTest` suite verifies the correctness of key functions in the `ExportDataJob` application using `ScalaTest`. The tests cover:

1. **`loadData`**: Ensures correct filtering by year and commodity.
2. **`aggregateExportsByCountry`**: Validates aggregation of export values by country.
3. **`categorizeCountries`**: Confirms countries are categorized as "big," "medium," or "small" based on total exports.

Each test uses sample data to validate functionality, ensuring accurate and reliable data processing in the export job. You can run the tests using the **Run** button in your IDE for verification.
