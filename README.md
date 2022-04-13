## Motivation
This project is a python module that can be used to define tests for data sets. This is part of a larger project to bring all my personal data into governed data pipelines. Automating these sort of tests has a huge potential to save time and accelerate development.

## Background
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

#### Dataset Metadata
<ol>
<li>Column Names</li>
<li>Data Type</li>
</ol>
  
#### Data Profile
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
  
#### Cross Product Element Statistics
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
  
#### Data Intersection
<ol>
<li>Mutual Exclusivity</li>
<li>Complete Overlap</li>
<li>Bounded Overlap</li>
</ol>
  
### Example Use Case: Monitoring Disk Usage
WizTree is a free program that will analyze disk usage and allow the computed summary statistics to be visualized in a TreeMap, as well as output in a tabular form.
  
  
### About the Data
  
WizTree Disk Usage report data has these headers:  
File Name, Size, Allocated, Modified, Attributes,Files, Folders
  
I've used Windows Task Scheduler to automate the recording of this data every day.
  
#### Business Objectives for Automatic Analysis of WizTree data
<ol>
<li>Business Users will be able to provide a list of directory to exclude from disk usage analysis.</li>
<li>Business Users will be able to provide a list of directories to consider for backup.</li>
<li>Business Users will be able to provide a directory as the current backup to help identify backup suggestions.</li>
<li>Business Users will be able to compare two disk usage reports and identify: files added, files removed, memory deltas. </li>
<li>Business Users will be able to analyze a folder a receive suggestions for file to delete. </li>
<li>Business Users will be able to analyze a folder a receive suggestions for file to backup. </li>
<li>Business Users will be able to access new reports from an output directory. No email notifications will be sent.</li>
<li>Business Users will be able action on delete and backup recommendations with one script each.</li>
</ol>
  
#### Business Requirements for Automatic Analysis of WizTree data
I am the sole stakeholder here, so I define the business requirements.
<ol>
<li>Backup: Directory Paths can be added to ignore list</li>
<li>Backup: If a directory is on the ignore list, do not include it in any backup recommendations.</li>
<li>Backup: If a file is on the backup list and has a different size that the provided backup location, suggest the file for backup.</li>
<li>Wasted Space: If a directory is on the ignore list, do not include it in any delete recommendations.</li>
<li>Wasted Space: If a directory has more than 0.25 GB of memory that is not on the ignore list, and is not on the backup list, suggest qualifying files in that folder for deletion.</li>
</ol>
  
#### Technical Objectives for Automatic Analysis of WizTree data
<ol>
<li>WizTree data collection is automated.</li>
<li>WizTree data is aggregated for analysis.</li>
<li>Business User backup and deletion suppression lists are saved to file.</li>
<li>Suppressions to backup and deletion recommendations can be applied.</li>
<li>A delta between two disk usage repors can be computed.</li>
<li>Notifications are written to file.</li>
<li>A script executing deletion recommendations is output to file.</li>
<li>A script executing backup recommendations is output to file.</li>
</ol>
  
#### Technical Requirements for Automatic Analysis of WizTree data  
<ol>
<li>Wiztree disk usage report is output to C:/sandbox/data/raw/wiztree/wiztree__YYYYMMDD_HHMM.dat every day at 7pm.</li>
<li>Business User backup suppressions are stored at this location: C:/sandbox/data/input/wiztree/suppressed_backup_paths.dat</li>
<li>Business User delete suppressions are stored at this location: C:/sandbox/data/input/wiztree/suppressed_delete_paths.dat</li>
<li>Disk usage delta reports are stored at this location: C:/sandbox/data/output/wiztree/disk_usage_delta_report_YYYYMMDD__HHMM__YYYYMMDD__HHMM.dat</li>
<li>Delete recommendation reports are stored at this location: C:/sandbox/data/output/wiztree/delete_recommendation_report_YYYYMMDD__HHMM.dat</li>
<li>Backup recommendation reports are stored at this location: C:/sandbox/data/output/wiztree/backup_recommendation_report_YYYYMMDD__HHMM.dat</li>
<li>Delete execution scripts are stored at this location: C:/sandbox/data/output/wiztree/execute_delete_recommendations_YYYYMMDD__HHMM.cmd</li>
<li>Backup execution scripts are stored at this location: C:/sandbox/data/output/wiztree/execute_backup_recommendations_YYYYMMDD__HHMM.cmd</li>
</ol>
  
#### Example Business Cases for WizTree data

Compute Disk Usage Delta
<ol>
<li>Disk Usage Report 1: C:/sandbox/data/raw/wiztree/wiztree__20220328_0801.dat</li>
<li>Disk Usage Report 2: C:/sandbox/data/raw/wiztree/wiztree__20220401_0524.dat</li>
<li>Expected Output: C:/sandbox/data/output/wiztree/disk_usage_delta_report_20220328_0801__20220401_0524.dat</li>
</ol>
  
Identify Deletion Prospects
<ol>
<li>Target Directory: C:/</li>
<li>Supression list: C:/sandbox/data/input/wiztree/suppressed_delete_paths.dat</li>
<li>Expected Output: C:/sandbox/data/output/wiztree/delete_recommendation_report_YYYYMMDD__HHMM.dat</li>
<li>Deletion Script: C:/sandbox/data/output/wiztree/execute_delete_recommendations_YYYYMMDD__HHMM.cmd</li>
</ol>
  
Identify Backup Prospects
<ol>
<li>Target Directory: C:/</li>
<li>Backup directory: C:/backup</li>
<li>Supression list: C:/sandbox/data/input/wiztree/suppressed_backup_paths.dat</li>
<li>Expected Output: C:/sandbox/data/output/wiztree/backup_recommendation_report_YYYYMMDD__HHMM.dat</li>
<li>Backup Script: C:/sandbox/data/output/wiztree/execute_backup_recommendations_YYYYMMDD__HHMM.cmd</li>
</ol>
  
  
  
#### Data Model
  
The first step in implementing this automatic analysis is to define the data model we want to use, and the necessary transformations from the raw data.  
  
<table>
<thead>
	<tr>
		<th colspan="2">Raw WizTree data</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td>Column Name</td>
		<td>Data Type</td>
	</tr>
	<tr>
		<td>File Name</td>
		<td>String</td>
	</tr>
	<tr>
		<td>Size</td>
		<td>Integer</td>
	</tr>
	<tr>
		<td>Allocated</td>
		<td>Integer</td>
	</tr>
	<tr>
		<td>Modified</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Attributes</td>
		<td>Integer</td>
	</tr>
	<tr>
		<td>Files</td>
		<td>Integer</td>
	</tr>
	<tr>
		<td>Folders</td>
		<td>Integer</td>
	</tr>
</tbody>
</table>
  
<table>
<thead>
	<tr>
		<th colspan="2">Disk Usage Delta</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td>Column Name</td>
		<td>Data Type</td>
	</tr>
	<tr>
		<td>File Path</td>
		<td>String</td>
	</tr>
	<tr>
		<td>Detected Change</td>
		<td>String</td>
	</tr>
	<tr>
		<td>Modified Timestamp Delta</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Size Delta</td>
		<td>Integer</td>
	</tr>
</tbody>
</table>
  
<table>
<thead>
	<tr>
		<th colspan="2">Backup Path Suppressions</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td>Column Name</td>
		<td>Data Type</td>
	</tr>
	<tr>
		<td>File Path</td>
		<td>String</td>
	</tr>
</tbody>
</table>
  
<table>
<thead>
	<tr>
		<th colspan="2">Delete Path Suppressions</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td>Column Name</td>
		<td>Data Type</td>
	</tr>
	<tr>
		<td>File Path</td>
		<td>String</td>
	</tr>
</tbody>
</table>
  
<table>
<thead>
	<tr>
		<th colspan="2">Delete Path Suggestions</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td>Column Name</td>
		<td>Data Type</td>
	</tr>
	<tr>
		<td>File Path</td>
		<td>String</td>
	</tr>
	<tr>
		<td>Created Timestamp</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Modified Timestamp</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Size</td>
		<td>Integer</td>
	</tr>
</tbody>
</table>
  
  
<table>
<thead>
	<tr>
		<th colspan="2">Backup Path Suggestions</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td>Column Name</td>
		<td>Data Type</td>
	</tr>
	<tr>
		<td>File Path</td>
		<td>String</td>
	</tr>
	<tr>
		<td>Created Timestamp</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Modified Timestamp</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Size</td>
		<td>Integer</td>
	</tr>
	<tr>
		<td>Backup File Path</td>
		<td>String</td>
	</tr>
	<tr>
		<td>Backup Created Timestamp</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Backup Modified Timestamp</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Backup Size</td>
		<td>Integer</td>
	</tr>
	<tr>
		<td>Backup Time Delta</td>
		<td>Timestamp</td>
	</tr>
	<tr>
		<td>Backup Size Delta</td>
		<td>Integer</td>
	</tr>
</tbody>
</table>
  
  
#### Method Signatures  
  
def get_subset_of_disk_usage_report(path_to_disk_usage_report, subset_root_path):  
	"""  
	Returns a DataFrame of filtered raw Wiztee data.  
	"""  
	  
	return DataFrame (Raw WizTree Data Layout)  
  
def compute_disk_usage_delta(path_to_disk_usage_report_1,path_to_disk_usage_report_path_2):  
	"""  
	Returns a DataFrame of added, removed and modifie files between the two reports. Returned layout is "Disk Usage Delta" layout.  
	"""  
	return DataFrame (Disk Usage Delta Layout)  
  
def identify_delete_prospect_paths(DataFrame (Disk Usage Delta Layout),path_to_delete_suppression_list):  
	"""  
	Returns a DataFrame of strings that meet delete prospect criteria.  
	"""  
	return DataFrame (Delete Path Suggestions Layout)  
	
def identify_backup_prospect_paths(DataFrame (Disk Usage Delta Layout),path_to_backup_suppression_list):  
	"""  
	Returns a DataFrame of strings that meet backup prospect criteria.  
	"""  
	return DataFrame (Backup Path Suggestions Layout)  
  
  
  
#### Comments on Orchestration
SchTasks leaves much to be desired. I have used Prefect for this and other projects.  
TODO say more  
  
#### Test Cases  
We proceed with out testing plan by using user-generated test data sets, for which we define the expected output.
TODO define test data sets
  
#### Potential Next Steps  
Enhance granularity of analysis by inspecting file attributes.
  
#### Project Status  
This project is currently in development.  
  
#### Test Results  
TODO <a href="https://hdickie.github.io/Data_Profile_Tester/pages/test_results.html">Test Result Report</a>  
  
#### Test Coverage  
<a href="https://hdickie.github.io/Data_Profile_Tester/htmlcov/index.html">Test Coverage Report</a>
  
