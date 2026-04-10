**Objectives**
  The goal of this phase is to strengthen PySpark skills by practicing advanced concepts like joins, window functions, and date operations. This phase acts as a final preparation step before moving to real-world environments like Databricks.
_________________________________________________________________________________________________________________________________________________
**Problem Statement (Summary)**
  1. Practice different types of joins (inner, left, left_anti)
  2. Work with window functions for ranking and running totals
  3. Perform date-based analysis and aggregations
  4. Validate data and check for inconsistencies
  5. Build a complete pipeline within a time limit
_________________________________________________________________________________________________________________________________________________
**Dataset Used**
  Datasets: customers_data, orders_data
  Sources: kaggle
  Tables: customer_data, orders_data
_________________________________________________________________________________________________________________________________________________
**Approach**
  1. Loaded datasets into PySpark DataFrames and explored structure
  2. Practiced different types of joins and validated outputs
  3. Applied window functions for ranking, running totals, and comparisons
  4. Performed date transformations and monthly analysis
  5. Cleaned and validated data before processing
  6. Built a complete pipeline within a fixed time limit
_________________________________________________________________________________________________________________________________________________
**Key Transformations**
  - join() – inner, left, left_anti joins
  - groupBy() and agg() – aggregations
  - window functions – ROW_NUMBER, RANK, LAG
  - date functions – extracting month, date calculations
  - filter() – data cleaning
  - orderBy() – sorting
  - withColumn() – creating new column
_________________________________________________________________________________________________________________________________________________
**Outputs**
  - Correct join outputs with validation
  - Ranked customers based on spending
  - Running total of sales
  - Monthly sales analysis
________________________________________________________________________________________________________________________________________________
**Challenges Faced**
  1. Managing time while solving multiple tasks in one session
  2. Writing correct join conditions and validating outputs
  3. Understanding and applying window functions properly
  4. Handling date transformations and calculations
________________________________________________________________________________________________________________________________________________
**Learnings**
  - Improved fluency in PySpark transformations
  - Strong understanding of joins and when to use each type
  - Practical use of window functions for analytics
  - Handling date-based analysis in PySpark
  - Importance of validation at every step
________________________________________________________________________________________________________________________________________________
**Files in the folder**
  - phase6_problem_statement.pdf -> Problem description
  - pyspark_code.py -> Implementation
  - outputs -> Results
