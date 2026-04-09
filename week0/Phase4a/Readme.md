**Objective**
  The goal of this phase is to understand how continuous numerical data can be converted into meaningful categories (like Gold, Silver, Bronze). This helps in simplifying analysis and making better business decisions.
________________________________________________________________________________________________________________________________________________________
**Problem Statement (Summary)**
  - Perform customer segmentation using total spend
  - Implement Gold, Silver, Bronze categories using conditional logic
  - Group customers based on segments and count them
  - Apply alternative methods like quantile-based segmentation
____________________________________________________________________________________________________________________________________________________
**Dataset Used**
  - Datasets: customers, sales
  - Source: pyspark playground
  - Tables: customers, sales
____________________________________________________________________________________________________________________________________________________
**Approach**
  1. Loaded dataset into PySpark DataFrame and inspected structure  
  2. Applied conditional logic (when/otherwise) to create Gold, Silver, Bronze segments
  3. Grouped data by segment to count number of customers in each category
  4. Compared results from different segmentation methods
____________________________________________________________________________________________________________________________________________________
**Key Transformations**
  1. withColumn() – to create new segment column
  2. when() / otherwise() – for conditional bucketing
  3. groupBy() and count() – for aggregation
  4. approxQuantile() – for quantile-based segmentation
____________________________________________________________________________________________________________________________________________________
**Output**
  - Customers categorized into Gold, Silver, Bronze segments
  - Count of customers in each segment
  - Comparison of segmentation results using different methods
  - Better understanding of customer distribution
____________________________________________________________________________________________________________________________________________________
**Challenges Faced**
   - Understanding differences between fixed rules and dynamic (quantile-based) segmentation
   - Implementing multiple methods and comparing their outputs
_____________________________________________________________________________________________________________________________________________________
**Learnings**
  1. How continuous data can be converted into meaningful categories
  2. When to use fixed thresholds vs quantile-based methods
  3. How different segmentation methods can produce different results
  4. Practical understanding of PySpark functions for segmentation
_____________________________________________________________________________________________________________________________________________________
**Files in this Folder**

   - phase4a_problem_statement.pdf -> Problem description
   - solution.py -> Implementation
   - outputs → Results
