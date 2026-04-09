**Objective**
  The goal of this phase is to work with a real-world dataset and build a complete data engineering pipeline using Databricks and PySpark. This phase focuses on data ingestion, validation, transformation, and advanced analytics using window functions.
_________________________________________________________________________________________________________________________________________________
**Problem Statement (Summary)**
   - Set up Databricks environment and upload dataset
   - Work with multiple tables from a real-world e-commerce dataset
   - Perform data validation and ensure referential integrity
   - Calculate top customers per city using window functions
   - Compute running total of daily sales
   - Identify top products per category using ranking
   - Calculate customer lifetime value (CLV)
   - Perform customer segmentation (Gold, Silver, Bronze)
   - Build a final reporting dataset combining all insights
_________________________________________________________________________________________________________________________________________________
**Dataset Used**
  Datasets: Olist Brazilian E-commerce Dataset
  Source: kaggle
  Tables: orders, customers, order_items, products
__________________________________________________________________________________________________________________________________________________
**Approach**
   1. Set up Databricks environment and created a notebook.
   2. Uploaded all dataset files and verified data availability
   3. Loaded multiple CSV files into PySpark DataFrames
   4. Validated data and checked referential integrity between tables
   5. Joined fact and dimension tables to create a unified dataset
   6. Applied aggregations to calculate metrics like total spend and sales
   7. window functions for ranking and running totals
   8. Implemented business logic for customer segmentation
   9. Built a final reporting dataset combining all required insights
____________________________________________________________________________________________________________________________________________________
**Key Transformations**

  - read() – loading multiple datasets
  - join() – combining fact and dimension tables
  - groupBy() and agg() – aggregations
  - window functions – ROW_NUMBER, DENSE_RANK, running totals
  - when() / otherwise() – segmentation logic
  - orderBy() – sorting
  - count() – aggregations for reporting
____________________________________________________________________________________________________________________________________________________
**Output**

  - Top 3 customers in each city
  - Running total of sales over time
  - Top products in each category
  - Customer lifetime value (CLV)
  - Customer segmentation (Gold, Silver, Bronze)
  - Final reporting dataset with combined insights
____________________________________________________________________________________________________________________________________________________
**Challenges Faced**

  1. Setting up Databricks environment and uploading large datasets
  2. Understanding relationships between multiple tables
  3. Validating joins to avoid incorrect or duplicate data
  4. Handling missing or inconsistent data across tables
  5. Writing complex window functions correctly
_____________________________________________________________________________________________________________________________________________________
**Learnings**

  - How real-world data engineering pipelines are built
  - Working with large, multi-table datasets
  - Importance of data validation before processing
  - Practical use of window functions for analytics
  - Understanding fact and dimension table concepts
  - Building scalable and structured workflows businesis N,, requirements into data transformation
  - Importance of organizing code for readability and reuse
  - Gaining confidence in handling real-world datasets
  - End-to-end pipeline development using Databricks and PySpark
____________________________________________________________________________________________________________________________________________________
**outputs**
  - phase5_problem_statement.pdf -> Problem description
  - outputs.py -> ImplementationE
  - outputs/ -> Final results
