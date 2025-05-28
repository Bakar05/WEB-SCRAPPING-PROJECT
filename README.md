#  ETL Pipeline: Largest Banks by Market Capitalization

This project is an end-to-end ETL (Extract, Transform, Load) pipeline implemented in Python. It scrapes the top 10 banks by market capitalization from a Wikipedia page, converts their market cap from USD to other currencies, and stores the processed data in both CSV and SQLite formats.

---

##  Features

- **Extract** data from a historical snapshot of Wikipedia using `requests` and `BeautifulSoup`
- **Transform** USD values to **GBP**, **EUR**, and **INR** using an exchange rate CSV
- **Load** final results to:
  - CSV file (`Largest_banks_data.csv`)
  - SQLite database (`Banks.db`)
- Logs every step of the process to a file (`code_log.txt`)
- Executes sample queries to validate database insertion

---

##  Project Structure

```

├── exchange\_rate.csv              # Contains currency conversion rates
├── code\_log.txt                   # Log file for process tracking
├── Largest\_banks\_data.csv        # Output CSV with enriched data
├── Banks.db                        # Output SQLite database
├── etl\_banks.py                   # Main ETL script
└── README.md                       # This file

````

---

##  Prerequisites

Make sure you have the following Python libraries installed:

```bash
pip install requests beautifulsoup4 pandas numpy
````

---

##  Input

* **URL**: Archived Wikipedia page of [List of Largest Banks](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
* **Exchange Rate File**: `exchange_rate.csv`

### Sample `exchange_rate.csv` Format:

```csv
Currency,Rate
GBP,0.76
EUR,0.85
INR,83.2
```

---

##  How to Run

```bash
python etl_banks.py
```

---

##  Example Queries

After running, the following sample queries are automatically executed on the database:

### 1. Show top 5 rows:

```sql
SELECT * FROM Largest_banks LIMIT 5;
```

### 2. Average market cap in USD:

```sql
SELECT AVG(MC_USD_Billion) as Avg_MC_USD FROM Largest_banks;
```

### 3. Top 3 banks by market cap:

```sql
SELECT Name FROM Largest_banks ORDER BY MC_USD_Billion DESC LIMIT 3;
```

---

##  Log File

All progress and error messages are written to `code_log.txt` with timestamps. Useful for debugging and auditing the ETL process.

---

##  Notes

* This project fetches only the **top 10 banks** to keep execution fast and focused.
* The script automatically handles errors and logs them for traceability.

---

##  License

This project is provided for educational and demonstration purposes.

