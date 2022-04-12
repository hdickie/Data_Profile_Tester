## Background

This project is a python module that can be used to define tests for data sets.  

In my roles as Data Analyst and System Analyst, I have had to analyze many datasets and I found that it is possible to break up the questions we ask about datasets into a handful of cases.

Quickly reviewing some terms from set theory:
Cardinality: The count of unique items in a set
Cross Product: Every unique combination of elements from two or more columns.
Dimension: One value used to categorize obersvations.
Measure:  One value that is an element of an observation.

For example:
Date		Gender	Count
2000-01-01	M		1
2000-01-01	F		2
2000-01-02	M		3
2000-01-02	F		4

Date and Gender are dimensions, and Count is a measure.

### Dataset Metadata
<ol>
<li>1. Column Names</li>
<li>2. Data Type</li>
<ol>

### Data Profile
<ol>
<li>3. Absolute File Row Count</li>
<li>4. Absolute Column Cardinality</li>
<li>5. Absolute Column Null Counts</li>
<li>6. Absolute Dimension Cross Product Cardinality</li>
<li>7. Absolute Dimension Cross Product Element Row Count</li>
<li>8. Absolute Dimension Cross Product Element Measure Cardinality</li>
<li>9. Absolute Dimension Cross Product Element Measure Null Count</li>
<li>10. Relative File Row Count</li>
<li>11. Relative Column Cardinality</li>
<li>12. Relative Column Null Counts</li>
<li>13. Relative Dimension Cross Product Cardinality</li>
<li>14. Relative Dimension Cross Product Element Count</li>
<li>15. Relative Dimension Cross Product Element Row Count</li>
<li>16. Relative Dimension Cross Product Element Measure Cardinality</li>
<li>17. Relative Dimension Cross Product Element Measure Null Count</li>
</ol>

### Cross Product Element Statistics
<ol>
<li>18. Absolute Dimension Cross Product Element Min</li>
<li>19. Absolute Dimension Cross Product Element Max</li>
<li>20. Absolute Dimension Cross Product Element Sum</li>
<li>21. Absolute Dimension Cross Product Element Mean</li>
<li>22. Absolute Dimension Cross Product Element Median</li>
<li>23. Absolute Dimension Cross Product Element Mode</li>
<li>24. Relative Dimension Cross Product Element Min</li>
<li>25. Relative Dimension Cross Product Element Max</li>
<li>26. Relative Dimension Cross Product Element Sum</li>
<li>27. Relative Dimension Cross Product Element Mean</li>
<li>28. Relative Dimension Cross Product Element Median</li>
</ol>

### Data Intersection
<ol>
<li>29. Mutual Exclusivity<li>
<li>30. Complete Overlap<li>
<li>31. Bounded Overlap<li>
</ol>


<a href="https://hdickie.github.io/Data_Profile_Tester/htmlcov/index.html">Test Coverage Report</a>

