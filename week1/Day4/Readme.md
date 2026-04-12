**Objective**
  The objective of this assignment is to analyze student submission data using advanced SQL concepts. 
  It focuses on handling real-world data issues such as inconsistencies, duplicates, and invalid records using joins and window functions.
______________________________________________________________________________________________________________________________________________
**Problem Statement (Summary)**
  1. Normalize email data (lowercase, remove spaces)
  2. Create a unified mapping between student IDs and multiple emails
  3. Identify students who have not submitted tasks
  4. Find valid and invalid submissions
  5. Detect duplicate submissions using window functions
  6. Compare GROUP BY vs window function approaches
  7. Generate insights like submission counts per student
  8. Classify students into categories (Submitted, Not Submitted, Duplicate, Invalid)
_____________________________________________________________________________________________________________________________________________
**Dataset used**
  - Datasets: Master Data, Task1, Task1_file2
  - Tables: Master Data, Task1, Task1_file2
_____________________________________________________________________________________________________________________________________________
**Approach**
  1. Cleaned and normalized email data using lower and trim functions
  2. Created a unified email mapping table linking both emails to student_id
  3. Used LEFT JOIN to identify students who did not submit
  4. Used INNER JOIN to find valid submissions
  5. Used LEFT ANTI JOIN (or equivalent) to detect invalid submissions
  6. Applied window functions (ROW_NUMBER) to identify duplicate submissions
_____________________________________________________________________________________________________________________________________________
**Key Concepts Covered**
  - Data cleaning (LOWER, TRIM)
  - Joins – INNER, LEFT, LEFT ANTI
  - Window functions – ROW_NUMBER()
  - GROUP BY vs Window functions
  - COALESCE for handling multiple email columns
  - Data validation and integrity checks
_____________________________________________________________________________________________________________________________________________
**Output**
  - List of students who submitted and did not submit
  - Valid and invalid submission records
  - Identification of duplicate submissions
  - Submission count per student
  - Final classified dataset with categories:
      - Submitted
      - Not Submitted
      - Duplicate
      - Invalid
_____________________________________________________________________________________________________________________________________________
**Challenges Faced**
  1. Handling multiple email IDs for a single student
  2. Ensuring proper normalization to avoid mismatches
  3. Identifying duplicates correctly using window functions
  4. Understanding difference between GROUP BY and window functions
  5. Managing joins across multiple datasets
  6. Avoiding data loss while cleaning and filtering
  7. Debugging mismatches in email mapping
_____________________________________________________________________________________________________________________________________________
**Learnings**
  1. Importance of data cleaning before analysis
  2. Handling real-world messy datasets
  3. Practical use of window functions for duplicate detection
  4. Difference between aggregation and window-based analysis
  5. How to validate and classify data effectively
  6. Improved understanding of joins and data relationships
__________________________________________________________________________________________________________________________________________
**Files in this Folder**

  - Advanced_SQL_Assignment_Capgemini.pdf -> Problem description
  - Day4_assignment.ipynb -> SQL queries
  - outputs/ -> Results
