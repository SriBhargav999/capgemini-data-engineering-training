**Objective:**
  The goal of this phase is to build a complete end-to-end data pipeline using PySpark. Instead of solving individual problems, this phase focuses on creating a structured workflow that processes data and generates meaningful business insights.
______________________________________________________________________________________________________________________________________________________
**Problem Statement (Summary):**
1. Cleaning the data by removing null values, duplicates, and invalid records
2. Calculating daily sales
3. Computing city-wise revenue
4. Identifying top 5 customers based on total spend
5. Finding repeat customers with more than one order
6. Creating customer segmentation (Gold, Silver, Bronze) based on spending
7. Building a final reporting dataset combining all insights
8. Saving the final output to a file
_______________________________________________________________________________________________________________________________________________________
**Dataset Used**
Datasets: customers,sales
source: spark playground
tables: customers,sales
____________________________________________________________________________________________________________________________________________________
**Approach:**
  - Loaded datasets into PySpark DataFrames and inspected using show() and printSchema()
  - Performed data cleaning by removing null keys, handling missing values, removing duplicates, and filtering invalid records oined datasets to create a unified view
  - Applied aggregations to calculate total sales, revenue, and customer spend
  - Implemented business logic for Gold, Silver, and Bronze customer segmentation  '
  - Combined all resuls into a final reporting dataset and saved the output
__________________________________________________________________________________________________________________________________________________
**Key Transformations:**
  - dropna() / fillna() – handling missing data
  - dropDuplicates() – removing duplicate records
  - filter() – removing invalid values
  - join() – combining datasets
  - groupBy() and agg() – performing aggregations
  - when() / otherwise() – implementing business logic
  - orderBy() – sorting results
  - write() – saving final output
_________________________________________________________________________________________________________________________________________________
**Output:**
  1. Calculate daily sales trends
  2. Analyze revenue across different cities
  3. Identify top-performing customers
  4. Detect repeat customers
  5. Segment customers based on spending behavior
  6. Create a final reporting dataset for business insights
__________________________________________________________________________________________________________________________________________________
**Challenges Faced**
  1. Deciding the correct order of steps (cleaning → joining → aggregation → output)
  2. Handling data cleaning properly before joins to avoid incorrect results
  3. Implementing correct business logic for customer segmentation
  4. Combining multiple outputs into a single final reporting table
________________________________________________________________________________________________________________________________________________
**Learnings**
  - How to design and implement an end-to-end ETL pipeline
  - Importance of cleaning data before performing any transformations
  - How to combine multiple datasets using joins
  - Applying aggregations to generate business insights
  - Implementing business logic using conditional transformations
_________________________________________________________________________________________________________________________________________________
**Files in this Folder**
 1. phase4_problem_statement.pdf → Problem description
 2. pyspark_code.py → Implementation
 3. outputs/ → Final results
