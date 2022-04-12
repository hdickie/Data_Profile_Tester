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
<li>Column Names</li>
<li>Data Type</li>
</ol>

### Data Profile
<ol>
<li>Absolute File Row Count</li>
<li>Absolute Column Cardinality</li>
<li>Absolute Column Null Counts</li>
<li>Absolute Dimension Cross Product Cardinality</li>
<li>Absolute Dimension Cross Product Element Row Count</li>
<li>Absolute Dimension Cross Product Element Measure Cardinality</li>
<li>Absolute Dimension Cross Product Element Measure Null Count</li>
<li>Relative File Row Count</li>
<li>Relative Column Cardinality</li>
<li>Relative Column Null Counts</li>
<li>Relative Dimension Cross Product Cardinality</li>
<li>Relative Dimension Cross Product Element Count</li>
<li>Relative Dimension Cross Product Element Row Count</li>
<li>Relative Dimension Cross Product Element Measure Cardinality</li>
<li>Relative Dimension Cross Product Element Measure Null Count</li>
</ol>

### Cross Product Element Statistics
<ol>
<li>Absolute Dimension Cross Product Element Min</li>
<li>Absolute Dimension Cross Product Element Max</li>
<li>Absolute Dimension Cross Product Element Sum</li>
<li>Absolute Dimension Cross Product Element Mean</li>
<li>Absolute Dimension Cross Product Element Median</li>
<li>Absolute Dimension Cross Product Element Mode</li>
<li>Relative Dimension Cross Product Element Min</li>
<li>Relative Dimension Cross Product Element Max</li>
<li>Relative Dimension Cross Product Element Sum</li>
<li>Relative Dimension Cross Product Element Mean</li>
<li>Relative Dimension Cross Product Element Median</li>
</ol>

### Data Intersection
<ol>
<li>Mutual Exclusivity</li>
<li>Complete Overlap</li>
<li>Bounded Overlap</li>
</ol>


##Project Status

### Test Results


### Test Coverage
<a href="https://hdickie.github.io/Data_Profile_Tester/htmlcov/index.html">Test Coverage Report</a>

