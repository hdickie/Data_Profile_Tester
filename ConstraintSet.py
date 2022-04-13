import logging
import pandas as pd
import traceback
import csv
import datetime

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(message)s')


def create_test_constraint_sets_map_from_xlsx(xlsx_path):

	# Constraint_Set_Name	Constraint_Set_Id	Expected Result	Primary Data Set Name	Secondary Data Set Name
	constraint_set_def_df = pd.read_excel(xlsx_path, sheet_name="Constraint Set Definitions")

	# Constraint_Set_Name	Constraint_Set_Id	Constraint_Name	Constraint_Type	Column_List	Element	Lower_Bound	Upper_Bound	Warn_or_Fail
	constraint_def_df = pd.read_excel(xlsx_path, sheet_name="Constraint Definitions")

	# Data Set Name	Description	Data Set Path
	data_set_def_df = pd.read_excel(xlsx_path, sheet_name="Data Set Definitions")

	data_set_name_to_df_map = {}
	for i in range(0, data_set_def_df.shape[0]):
		Data_Set_Name = data_set_def_df.iloc[i, 0]
		Description = data_set_def_df.iloc[i, 1]
		Data_Set_Path = data_set_def_df.iloc[i, 2]
		data_set_name_to_df_map[Data_Set_Name] = pd.read_csv(Data_Set_Path)

	constraint_set_id_to_Constraint_Set_Object_map = {}
	constraint_set_id_to_Expected_Result_map = {}
	for i in range(0, constraint_set_def_df.shape[0]):
		Constraint_Set_Name = constraint_set_def_df.iloc[i, 0]
		Constraint_Set_Id = constraint_set_def_df.iloc[i, 1]
		Expected_Result = constraint_set_def_df.iloc[i, 2]
		Primary_Data_Set_Name = constraint_set_def_df.iloc[i, 3]
		Secondary_Data_Set_Name = constraint_set_def_df.iloc[i, 4]

		# todo if None

		if Secondary_Data_Set_Name == 'None':
			constraint_set_id_to_Constraint_Set_Object_map[Constraint_Set_Id] = ConstraintSet(Constraint_Set_Name,
																							  Constraint_Set_Id,
																							  data_set_name_to_df_map[
																								  Primary_Data_Set_Name],
																							  None)
		else:
			constraint_set_id_to_Constraint_Set_Object_map[Constraint_Set_Id] = ConstraintSet(Constraint_Set_Name,
																						  Constraint_Set_Id,
																						  data_set_name_to_df_map[
																							  Primary_Data_Set_Name],
																						  data_set_name_to_df_map[
																							  Secondary_Data_Set_Name])

	for i in range(0, constraint_def_df.shape[0]):
		Constraint_Set_Name = constraint_def_df.iloc[i, 0]
		Constraint_Set_Id = constraint_def_df.iloc[i, 1]
		Constraint_Name = constraint_def_df.iloc[i, 2]
		Constraint_Type = constraint_def_df.iloc[i, 3]
		Column_List = constraint_def_df.iloc[i, 4]
		Element = constraint_def_df.iloc[i, 5]
		Lower_Bound = constraint_def_df.iloc[i, 6]
		Upper_Bound = constraint_def_df.iloc[i, 7]
		Warn_or_Fail = constraint_def_df.iloc[i, 8]

		print(str(Constraint_Set_Id)+":"+Constraint_Set_Name)

		try:
			constraint_set_id_to_Constraint_Set_Object_map[Constraint_Set_Id].addConstraint(Constraint_Name,
																						Constraint_Type,
																						Column_List,
																						Element,
																						Lower_Bound,
																						Upper_Bound,
																						Warn_or_Fail)
		except Exception as e:
			print(e)

	return constraint_set_id_to_Constraint_Set_Object_map

class ConstraintSet:
	"""
	ConstraintSet docstring


	#todo make this real
	>>> df = pd.DataFrame(columns=['first_name', 'last_name'])
    >>> df = df.append(pd.Series({'first_name':'Rick','last_name':'Sanchez'}), ignore_index=True)
    >>> df = df.append(pd.Series({'first_name':'Stanford', 'last_name':'Pines'}), ignore_index=True)
    >>> find_name(df, 'Stanford')
      first_name last_name
    1   Stanford     Pines

	"""


	def __str__(self):

		running_str = "Constraint Set #" + str(self.constraint_set_id) + ": " + str(self.constraint_set_name) + "\n"
		running_str += '\n'
		running_str += "Primary Data Frame Details:\n"
		running_str += str(self.df.head(1)) + "\n"
		running_str += "\nSecondary Data Frame Details:\n"
		try:
			running_str += str(self.relative_df.head(1)) + "\n"
		except:
			running_str += "None" + "\n"
		running_str += '\n'

		af_lines=""
		rf_lines=""
		acc_lines=""
		acnc_lines=""
		rcc_lines=""
		rcnc_lines=""

		for key in self.constraints.keys():
			if self.constraints[key]['constraint_type'] == 'Absolute File':
				af_lines += str(self.constraints[key]['args']['constraint_id']).ljust(5)
				af_lines += str(self.constraints[key]['args']['constraint_name']).ljust(40)
				af_lines += "".ljust(40)
				af_lines += str(self.constraints[key]['args']['lb_cnt']).ljust(8)
				af_lines += str(self.constraints[key]['args']['ub_cnt']).ljust(8)
				af_lines += str(self.constraints[key]['args']['warn_or_fail'])
			elif self.constraints[key]['constraint_type'] == 'Relative File':
				rf_lines += str(self.constraints[key]['args']['constraint_id']).ljust(5)
				rf_lines += str(self.constraints[key]['args']['constraint_name']).ljust(40)
				rf_lines += str(self.constraints[key]['args']['lb_ratio']).ljust(8)
				rf_lines += str(self.constraints[key]['args']['ub_ratio']).ljust(8)
				rf_lines += str(self.constraints[key]['args']['warn_or_fail'])
			elif self.constraints[key]['constraint_type'] == 'Absolute Column Cardinality':
				acc_lines += str(self.constraints[key]['args']['constraint_id']).ljust(5)
				acc_lines += str(self.constraints[key]['args']['constraint_name']).ljust(40)
				acc_lines += str(self.constraints[key]['args']['column_name']).ljust(40)
				acc_lines += str(self.constraints[key]['args']['lb_cnt']).ljust(8)
				acc_lines += str(self.constraints[key]['args']['ub_cnt']).ljust(8)
				acc_lines += str(self.constraints[key]['args']['warn_or_fail'])
			elif self.constraints[key]['constraint_type'] == 'Absolute Column Null Count':
				acnc_lines += str(self.constraints[key]['args']['constraint_id']).ljust(5)
				acnc_lines += str(self.constraints[key]['args']['constraint_name']).ljust(40)
				acnc_lines += str(self.constraints[key]['args']['column_name']).ljust(40)
				acnc_lines += str(self.constraints[key]['args']['lb_cnt']).ljust(8)
				acnc_lines += str(self.constraints[key]['args']['ub_cnt']).ljust(8)
				acnc_lines += str(self.constraints[key]['args']['warn_or_fail'])
			elif self.constraints[key]['constraint_type'] == 'Relative Column Cardinality':
				rcc_lines += str(self.constraints[key]['args']['constraint_id']).ljust(5)
				rcc_lines += str(self.constraints[key]['args']['constraint_name']).ljust(40)
				rcc_lines += str(self.constraints[key]['args']['lb_ratio']).ljust(8)
				rcc_lines += str(self.constraints[key]['args']['ub_ratio']).ljust(8)
				rcc_lines += str(self.constraints[key]['args']['warn_or_fail'])
			elif self.constraints[key]['constraint_type'] == 'Relative Column Null Count':
				rcnc_lines += str(self.constraints[key]['args']['constraint_id']).ljust(5)
				rcnc_lines += str(self.constraints[key]['args']['constraint_name']).ljust(40)
				rcnc_lines += str(self.constraints[key]['args']['lb_ratio']).ljust(8)
				rcnc_lines += str(self.constraints[key]['args']['ub_ratio']).ljust(8)
				rcnc_lines += str(self.constraints[key]['args']['warn_or_fail'])

		running_str += "Absolute File Constraints" + "\n"
		if af_lines == "":
			running_str += "None" + "\n"
		else:
			running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "".ljust(40,'.') + "Lb_Cnt".ljust(8,'.') + "Ub_Cnt".ljust(8,'.') + "Warn_or_Fail" + '\n'
			running_str += af_lines + "\n"
		running_str += '\n'

		running_str += "Relative File Constraints" + "\n"
		if rf_lines == "":
			running_str += "None" + "\n"
		else:
			running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "Lb_ratio".ljust(8,'.') + "Ub_ratio".ljust(8,'.') + "Warn_or_Fail" + '\n'
			running_str += rf_lines + "\n"
		running_str += '\n'

		running_str += "Absolute Column Cardinality" + "\n"
		if acc_lines == "":
			running_str += "None" + "\n"
		else:
			running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "Column_Name".ljust(40,'.') + "Lb_Cnt".ljust(8,'.') + "Ub_Cnt".ljust(8,'.') + "Warn_or_Fail" + '\n'
			running_str += acc_lines + "\n"
		running_str += '\n'

		running_str += "Relative Column Cardinality" + "\n"
		if rcc_lines == "":
			running_str += "None" + "\n"
		else:
			running_str += rcc_lines + "\n"
		running_str += '\n'

		running_str += "Absolute Column Null Count" + "\n"
		if acnc_lines == "":
			running_str += "None" + "\n"
		else:
			running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "Column_Name".ljust(40,'.') + "Lb_Cnt".ljust(8,'.') + "Ub_Cnt".ljust(8,'.') + "Warn_or_Fail" + '\n'
			running_str += acnc_lines + "\n"
		running_str += '\n'

		running_str += "Relative Column Null Count" + "\n"
		if rcnc_lines == "":
			running_str += "None" + "\n"
		else:
			running_str += rcnc_lines + "\n"
		running_str += '\n'

		#running_str += "Constraint Name Map"
		#running_str += str(self.constraint_name_map)

		return running_str

	def __init__(self, constraint_set_name, constraint_set_id, df, relative_df):


		#todo input parameter validation

		self.constraint_set_name = constraint_set_name
		self.constraint_set_id = constraint_set_id
		self.df = df.copy()

		if relative_df is not None:
			self.relative_df = relative_df.copy()

		self.memoized_values = {}
		self.history = {}
		self.constraint_name_map = {}

		absolute_file_constraint_initial_data = {'constraint_id': [],
												 'constraint_name': [],
												 'lb_cnt': [],
												 'ub_cnt': [],
												 "warn_or_fail": []}
		self.absolute_file_constraints = pd.DataFrame(data=absolute_file_constraint_initial_data)

		relative_file_constraint_initial_data = {'constraint_id': [],
												 'constraint_name': [],
												 'lb_ratio': [],
												 'ub_ratio': [],
												 "warn_or_fail": []}
		self.relative_file_constraints = pd.DataFrame(data=relative_file_constraint_initial_data)

		absolute_dimension_cross_product_constraint_initial_data = {'constraint_id': [],
																	'constraint_name': [],
																	'dimension_column_index_list': [],
																	'lb_cnt': [],
																	'ub_cnt': [],
																	"warn_or_fail": []}
		self.absolute_dimension_cross_product_constraints = pd.DataFrame(data=absolute_dimension_cross_product_constraint_initial_data)

		relative_dimension_cross_product_constraint_initial_data = {'constraint_id': [],
																	'constraint_name': [],
																	'dimension_column_index_list': [],
																	'lb_cnt': [],
																	'ub_cnt': [],
																	"warn_or_fail": []}
		self.relative_dimension_cross_product_constraints = pd.DataFrame(data=relative_dimension_cross_product_constraint_initial_data)

		absolute_column_cardinality_constraints_initial_data = {'constraint_id': [],
																'constraint_name': [],
																'column_name': [],
																'lb_cnt': [],
																'ub_cnt': [],
																"warn_or_fail": []}
		self.absolute_column_cardinality_constraints = pd.DataFrame(data=absolute_column_cardinality_constraints_initial_data)


		relative_column_cardinality_constraints_initial_data = {'constraint_id': [],
												 'constraint_name': [],
												 'column_name': [],
												 'lb_ratio': [],
												 'ub_ratio': [],
												 "warn_or_fail": []}
		self.relative_column_cardinality_constraints = pd.DataFrame(data=relative_column_cardinality_constraints_initial_data)

		absolute_column_null_count_constraints_initial_data = {'constraint_id': [],
															   'constraint_name': [],
															   'column_name': [],
															   'lb_ratio': [],
															   'ub_ratio': [],
															   "warn_or_fail": []}
		self.absolute_column_null_count_constraints = pd.DataFrame(
			data=absolute_column_null_count_constraints_initial_data)


		relative_column_null_count_constraints_initial_data = {'constraint_id': [],
																'constraint_name': [],
																'column_name': [],
																'lb_ratio': [],
																'ub_ratio': [],
																"warn_or_fail": []}
		self.relative_column_null_count_constraints = pd.DataFrame(
			data=relative_column_null_count_constraints_initial_data)

		#todo ???
		self.absolute_measure_constraints = pd.DataFrame()

		#todo ??
		self.relative_measure_constraints = pd.DataFrame()

		#todo ???
		self.absolute_dimension_cross_product_element_measure_constraints = pd.DataFrame()

		#todo ???
		self.relative_dimension_cross_product_element_measure_constraints = pd.DataFrame()

		column_cardinality_constraints_initial_data = {'constraint_id': [],
											   		   'constraint_name': [],
														'column_name': [],
														"warn_or_fail": []}
		self.column_cardinality_constraints = pd.DataFrame(data=column_cardinality_constraints_initial_data)

		column_null_count_constraints_initial_data = {'constraint_id': [],
											   'constraint_name': [],
											   'column_name': [],
											   "warn_or_fail": []}
		self.column_not_null_constraints = pd.DataFrame(data=column_null_count_constraints_initial_data)


		mutually_exclusive_constraints_initial_data = {'constraint_id': [],
											 'constraint_name': [],
											 'column_name': [],
											 "warn_or_fail": []}
		self.mutually_exclusive_constraints = pd.DataFrame(data=mutually_exclusive_constraints_initial_data)

		self.column_data_type_constraints = pd.DataFrame()
		column_data_type_constraints_initial_data = {'constraint_id': [],
													   'constraint_name': [],
													   'column_name': [],
													   'column_data_type': [],
													   "warn_or_fail": []}
		self.column_data_type_constraints = pd.DataFrame(data=column_data_type_constraints_initial_data)

		data_layout_constraints_initial_data = {'constraint_id': [],
													 'constraint_name': [],
													 'column_data_type_list': [],
													 "warn_or_fail": []}
		self.data_layout_constraints = pd.DataFrame(data=data_layout_constraints_initial_data)

		self.column_name_constraints = pd.DataFrame()
		column_name_constraints_initial_data = {"constraint_id": [],
					 "constraint_name": [],
					 "column_index": [],
					 "goal_column_name": [],
					 "warn_or_fail": []}
		self.column_name_constraints = pd.DataFrame(data=column_name_constraints_initial_data)

		header_constraints_initial_data = {"constraint_id": [],
												"constraint_name": [],
												"columns_names": [],
												"warn_or_fail": []}
		self.header_constraints = pd.DataFrame(data=header_constraints_initial_data)


		self.constraints = {}

	def addAbsoluteColumnCardinalityConstraint(self,constraint_name, column_index,lb_cnt,ub_cnt,warn_or_fail):
		column_name = self.df.columns[column_index]

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "column_name": column_name,
					 "lb_cnt":lb_cnt,
					 "ub_cnt":ub_cnt,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Absolute Column Cardinality", args_dict)
		self.absolute_column_cardinality_constraints.loc[len(self.absolute_column_cardinality_constraints.index)] = [new_constraint_id,
																				   constraint_name,
																				   column_name,
																				   lb_cnt,
																				   ub_cnt,
																				   warn_or_fail]

	def addAbsoluteColumnNullCountConstraint(self, constraint_name, column_index, lb_cnt, ub_cnt, warn_or_fail):
		column_name = self.df.columns[column_index]

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "column_name": column_name,
					 "lb_cnt":lb_cnt,
					 "ub_cnt":ub_cnt,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Absolute Column Null Count", args_dict)
		self.absolute_column_null_count_constraints.loc[len(self.absolute_column_null_count_constraints.index)] = [new_constraint_id,
																			   constraint_name,
																			   column_name,
																			   lb_cnt,
																			   ub_cnt,
																			   warn_or_fail]

	def addAbsoluteDimensionCrossProductCardinalityConstraint(self, Constraint_Name, Column_List, Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductCardinalityConstraint()

	def addAbsoluteDimensionCrossProductRowCountConstraint(self,constraint_name, dimension_column_index_list,lb_cnt,ub_cnt,warn_or_fail):
		pass #todo addAbsoluteDimensionCrossProductRowCountConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureCardinalityConstraint(self, Constraint_Name, Column_List, Element,Lower_Bound, Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureCardinalityConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureMaxConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureMaxConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureMeanConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureMeanConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureMedianConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureMedianConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureMinConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureMinConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureModeConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureModeConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureNullCountConstraint(self, Constraint_Name, Column_List,Element,Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureNullCountConstraint()

	def addAbsoluteDimensionCrossProductElementMeasureSumConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementMeasureSumConstraint()

	def addAbsoluteDimensionCrossProductElementRowCountConstraint(self, Constraint_Name,Column_List,Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo addAbsoluteDimensionCrossProductElementRowCountConstraint()

	def addAbsoluteFileConstraint(self,constraint_name, lb_cnt,ub_cnt,warn_or_fail):
		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id":new_constraint_id,
					"constraint_name":constraint_name,
					 "lb_cnt":lb_cnt,
					 "ub_cnt":ub_cnt,
					 "warn_or_fail":warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Absolute File", args_dict)
		self.absolute_file_constraints.loc[len(self.absolute_file_constraints.index)] = [new_constraint_id,
																						 constraint_name,
																						 lb_cnt,
																						 ub_cnt,
																						 warn_or_fail]

	def addBoundedOverlapConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound, Warn_or_Fail):
		pass  #todo addBoundedOverlapConstraint()

	def addColumnDataTypeConstraint(self,constraint_name, column_index,data_type,warn_or_fail):
		column_name = self.df.columns[column_index]

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "column_name": column_name,
					 "data_type": data_type,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Column Data Type", args_dict)
		self.column_data_type_constraints.loc[len(self.column_data_type_constraints.index)] = [new_constraint_id,
																									 constraint_name,
																									 column_name,
																									 data_type,
																									 warn_or_fail]

	def addColumnNameConstraint(self,constraint_name, column_index,goal_column_name,warn_or_fail):

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "column_index": column_index,
					 "goal_column_name": goal_column_name,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Column Name", args_dict)
		self.column_name_constraints.loc[len(self.column_name_constraints.index)] = [new_constraint_id,
																					 constraint_name,
																					 column_index,
																					 goal_column_name,
																					 warn_or_fail]

	def addConstraint(self,Constraint_Name,Constraint_Type,Column_List,Element,Lower_Bound,Upper_Bound,Warn_or_Fail):
		if Constraint_Type == "Absolute File Row Count":
			self.addAbsoluteFileConstraint(Constraint_Name, Lower_Bound,Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Column Cardinality":
			self.addAbsoluteColumnCardinalityConstraint(Constraint_Name,Column_List,Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Column Null Count":
			self.addAbsoluteColumnNullCountConstraint(Constraint_Name,Column_List,Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Cardinality":
			self.addAbsoluteDimensionCrossProductCardinalityConstraint(Constraint_Name,Column_List,Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Row Count":
			self.addAbsoluteDimensionCrossProductElementRowCountConstraint(Constraint_Name,Column_List,Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Cardinality":
			self.addAbsoluteDimensionCrossProductElementMeasureCardinalityConstraint(Constraint_Name, Column_List, Element,Lower_Bound, Upper_Bound, Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Null Count":
			self.addAbsoluteDimensionCrossProductElementMeasureNullCountConstraint(Constraint_Name, Column_List,Element,Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative File Row Count":
			self.addRelativeFileConstraint(Constraint_Name, Lower_Bound,Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Column Cardinality":
			self.addRelativeColumnCardinalityConstraint(Constraint_Name,Column_List, Lower_Bound,Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Column Null Counts":
			self.addRelativeColumnNullCountConstraint(Constraint_Name, Column_List, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Cardinality":
			self.addRelativeDimensionCrossProductCardinalityConstraint(Constraint_Name, Column_List, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Row Count":
			self.addRelativeDimensionCrossProductElementRowCountConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Cardinality":
			self.addRelativeDimensionCrossProductElementMeasureCardinalityConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Null Count":
			self.addRelativeDimensionCrossProductElementMeasureNullCountConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Min":
			self.addAbsoluteDimensionCrossProductElementMeasureMinConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Max":
			self.addAbsoluteDimensionCrossProductElementMeasureMaxConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Sum":
			self.addAbsoluteDimensionCrossProductElementMeasureSumConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Mean":
			self.addAbsoluteDimensionCrossProductElementMeasureMeanConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Median":
			self.addAbsoluteDimensionCrossProductElementMeasureMedianConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Absolute Dimension Cross Product Element Measure Mode":
			self.addAbsoluteDimensionCrossProductElementMeasureModeConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Min":
			self.addRelativeDimensionCrossProductElementMeasureMinConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Max":
			self.addRelativeDimensionCrossProductElementMeasureMaxConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Sum":
			self.addRelativeDimensionCrossProductElementMeasureSumConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Mean":
			self.addRelativeDimensionCrossProductElementMeasureMeanConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Relative Dimension Cross Product Element Measure Median":
			self.addRelativeDimensionCrossProductElementMeasureMedianConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Bounded Overlap":
			self.addBoundedOverlapConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Layout":
			self.addLayoutConstraint(Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound,Warn_or_Fail)
		elif Constraint_Type == "Column Name":
			self.addColumnNameConstraint(Constraint_Name, Column_List, Element, Warn_or_Fail)
		else:
			logging.debug('unknown constraint type:'+Constraint_Type)

	def addConstraintToConstraintMap(self,new_constraint_id, constraint_type,args_dict):
		#logging.debug("enter addConstraintToConstraintMap()")
		# constraints[constraint_id] = {"constraint_type":"<constraint type>","args":{<args dict>}}
		#logging.debug("new_constraint_id:"+str(new_constraint_id))
		#logging.debug("args_dict[constraint_name]:"+str(args_dict['constraint_name']))

		# constraints.update()
		self.constraints[new_constraint_id] = {"constraint_type": constraint_type, "args":args_dict}
		#logging.debug("self.constraint_name_map["+args_dict["constraint_name"]+"] = "+str(new_constraint_id))
		self.constraint_name_map[args_dict["constraint_name"]] = new_constraint_id
		#logging.debug("exit addConstraintToConstraintMap()")

	def addDataLayoutConstraint(self,constraint_name, data_type_list,warn_or_fail):
		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "data_type_list": data_type_list,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Data Layout", args_dict)
		self.data_layout_constraints.loc[len(self.data_layout_constraints.index)] = [new_constraint_id,
																							   constraint_name,
																							   data_type_list,
																							   warn_or_fail]


	def addHeaderConstraint(self,constraint_name, column_names,warn_or_fail):
		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "header_list": column_names,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Header", args_dict)
		self.header_constraints.loc[len(self.header_constraints.index)] = [new_constraint_id,
																					 constraint_name,
																					 column_names,
																					 warn_or_fail]


	def addLayoutConstraint(self, Constraint_Name, Column_List, Element, Lower_Bound, Upper_Bound, Warn_or_Fail):
		pass #todo addLayoutConstraint()


	def addRelativeColumnCardinalityConstraint(self,constraint_name, column_index,lb_ratio,ub_ratio,warn_or_fail):
		column_name = self.df.columns[column_index]

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "column_name": column_name,
					 "lb_ratio": lb_ratio,
					 "ub_ratio": ub_ratio,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Relative Column Cardinality", args_dict)
		self.relative_column_cardinality_constraints.loc[len(self.relative_column_cardinality_constraints.index)] = [
			new_constraint_id,
			constraint_name,
			column_name,
			lb_ratio,
			ub_ratio,
			warn_or_fail]

	def addRelativeColumnNullCountConstraint(self, constraint_name, column_index, lb_ratio, ub_ratio, warn_or_fail):
		column_name = self.df.columns[column_index]

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "column_name": column_name,
					 "lb_ratio": lb_ratio,
					 "ub_ratio": ub_ratio,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Relative Column Null Count", args_dict)
		self.relative_column_null_count_constraints.loc[len(self.relative_column_null_count_constraints.index)] = [new_constraint_id,
																			   constraint_name,
																			   column_name,
																			   lb_ratio,
																			   ub_ratio,
																			   warn_or_fail]

	def addRelativeDimensionCrossProductCardinalityConstraint(self, Constraint_Name, Column_List, Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductCardinalityConstraint()

	def addRelativeDimensionCrossProductConstraint(self,constraint_name, dimension_column_index_list,lb_ratio,ub_ratio,warn_or_fail):
		pass #todo addRelativeDimensionCrossProductConstraint()

	def addRelativeDimensionCrossProductElementMeasureCardinalityConstraint(self, Column_List, Element,Lower_Bound, Upper_Bound, Warn_or_Fail):
		pass  #todo addRelativeDimensionCrossProductElementMeasureCardinalityConstraint()

	def addRelativeDimensionCrossProductElementMeasureMaxConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductElementMeasureMaxConstraint()

	def addRelativeDimensionCrossProductElementMeasureMeanConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductElementMeasureMeanConstraint()

	def addRelativeDimensionCrossProductElementMeasureMedianConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductElementMeasureMedianConstraint()

	def addRelativeDimensionCrossProductElementMeasureMinConstraint(self,Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductElementMeasureMinConstraint()

	def addRelativeDimensionCrossProductElementMeasureSumConstraint(self, Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductElementMeasureSumConstraint()

	def addRelativeDimensionCrossProductElementRowCountConstraint(self, Constraint_Name, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo addRelativeDimensionCrossProductElementRowCountConstraint()

	def addRelativeFileRowCountConstraint(self,constraint_name, lb_ratio,ub_ratio,warn_or_fail):

		new_constraint_id = self.getNewConstraintId()
		args_dict = {"constraint_id": new_constraint_id,
					 "constraint_name": constraint_name,
					 "lb_ratio": lb_ratio,
					 "ub_ratio": ub_ratio,
					 "warn_or_fail": warn_or_fail}
		self.addConstraintToConstraintMap(new_constraint_id, "Relative File", args_dict)
		self.relative_file_constraints.loc[len(self.relative_file_constraints.index)] = [new_constraint_id,
																						 constraint_name,
																						 lb_ratio,
																						 ub_ratio,
																						 warn_or_fail]

	def checkAbsoluteColumnCardinalityConstraint(self, column_name,lb_cnt,ub_cnt,warn_or_fail):

		column_index = self.df.columns.get_loc(column_name)

		try:
			if column_name+' Column Cardinality' not in self.memoized_values.keys():
				self.memoized_values[column_name+' Column Cardinality'] = len(self.df.iloc[:,column_index].unique())

			if lb_cnt <= self.memoized_values[column_name+' Column Cardinality'] and self.memoized_values[column_name+' Column Cardinality'] <= ub_cnt:
				return 0
			elif warn_or_fail == 0:
				#logging.debug("Warning in checkAbsoluteColumnCardinalityConstraint") #todo make more specific
				pass
			elif warn_or_fail == 1:
				#logging.debug("Failure in checkAbsoluteColumnCardinalityConstraint") #todo make more specific
				pass
				return 1
			else:
				logging.error("what is happening in checkAbsoluteColumnCardinalityConstraint()") #todo make more specific
				raise ValueError
				return 1

		except Exception as e:
			logging.error("uncaught exception in checkAbsoluteColumnCardinalityConstraint()")
			traceback.print_tb()
			return -1

		return 1

	def checkAbsoluteColumnNullCountConstraint(self, column_name, lb_cnt, ub_cnt, warn_or_fail):
		try:
			if column_name+' Column Null Count' not in self.memoized_values.keys():
				self.memoized_values[column_name+' Column Null Count'] = sum(self.df[column_name].isnull())

			if lb_cnt <= self.memoized_values[column_name+' Column Null Count'] and self.memoized_values[column_name+' Column Null Count'] <= ub_cnt:
				return 0
			elif warn_or_fail == 0:
				logging.warning("Warning in checkAbsoluteColumnNullCountConstraint") #todo make more specific
			elif warn_or_fail == 1:
				logging.error("Error in checkAbsoluteColumnNullCountConstraint") #todo make more specific
				return 1
			else:
				logging.error("what is happening in checkAbsoluteColumnNullCountConstraint()") #todo make more specific
				raise ValueError
				return 1

		except Exception as e:
			logging.error("uncaught exception in checkAbsoluteColumnNullCountConstraint()")
			traceback.print_tb()
			return -1
		return 1

	def checkAbsoluteDimensionCrossProductCardinalityConstraint(self, Column_List, Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductCardinalityConstraint()

	def checkAbsoluteDimensionCrossProductConstraint(self, dimension_column_index_list,lb_cnt,ub_cnt,warn_or_fail):
		pass #todo checkAbsoluteDimensionCrossProductConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureCardinalityConstraint(self, Column_List, Element,Lower_Bound, Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureCardinalityConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureMaxConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureMaxConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureMeanConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureMeanConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureMedianConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureMedianConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureMinConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureMinConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureModeConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureModeConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureNullCountConstraint(self, Column_List,Element,Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureNullCountConstraint()

	def checkAbsoluteDimensionCrossProductElementMeasureSumConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementMeasureSumConstraint()

	def checkAbsoluteDimensionCrossProductElementRowCountConstraint(self,Column_List,Lower_Bound, Upper_Bound,Warn_or_Fail):
		pass #todo checkAbsoluteDimensionCrossProductElementRowCountConstraint()

	def checkAbsoluteFileRowCountConstraint(self,lb_cnt,ub_cnt,warn_or_fail):
		try:
			if 'Primary Row Count' not in self.memoized_values.keys():
				self.memoized_values['Primary Row Count'] = self.df.shape[0]

			if lb_cnt <= self.memoized_values['Primary Row Count'] and self.memoized_values['Primary Row Count'] <= ub_cnt:
				return 0
			elif warn_or_fail == 0:
				logging.warning("Warning in checkAbsoluteFileRowCountConstraint") #todo make more specific
			elif warn_or_fail == 1:
				logging.error("Error in checkAbsoluteFileRowCountConstraint") #todo make more specific
				return 1
			else:
				logging.error("what is happening in checkAbsoluteFileCcheckAbsoluteFileRowCountConstraintonstraint()") #todo make more specific
				raise ValueError
				return 1

		except Exception as e:
			logging.error("uncaught exception in checkAbsoluteFileRowCountConstraint()")
			traceback.print_tb()
			return -1

	def checkAllConstraints(self,printResults=True,outputFolder=None):
		for constraint_id in self.constraints.keys():
			self.history[constraint_id] = self.checkConstraintById(constraint_id)

		if printResults:
			self.showResults()

		if outputFolder is not None:
			self.writeResultsToCSV(outputFolder+'//Data_Profile_Test_Results_'+datetime.datetime.now().strftime("%Y%m%d_%H%M%S")+'.txt')

	def checkBoundedOverlapConstraint(self, Column_List, Element, Lower_Bound, Upper_Bound, Warn_or_Fail):
		pass  #todo checkBoundedOverlapConstraint()

	def checkColumnDataTypeConstraint(self, column_name, data_type, warn_or_fail):
		column_index = self.df.columns.get_loc(column_name)
		#logging.debug("enter checkColumnDataTypeConstraint()")
		#logging.debug("column_name:"+str(column_name))
		#logging.debug("goal data_type:" + str(data_type))
		#logging.debug("actual data_type:" + str(self.df.dtypes[column_index]))

		if self.df.dtypes[column_index] == data_type: #todo i think i need some exception handling here
			return 0
		elif warn_or_fail == 0:
			logging.error("Warning in checkColumnDataTypeConstraint()") #todo make more specific
			return 0
		else:
			return 1

	def checkColumnNameConstraint(self, column_index, goal_column_name, warn_or_fail):
		test_result = self.df.columns[column_index] == goal_column_name
		#todo add exception handling

		if test_result == 0:
			return 0
		elif warn_or_fail == 0:
			logging.warning("warning in checkColumnNameConstraint()")
			return 0
		else:
			return 1

	def checkConstraintById(self, constraint_id):

		#logging.debug("enter checkConstraintById()")
		error_ind = 0

		current_constraint = self.constraints[constraint_id]
		current_args = current_constraint["args"]

		#logging.debug("current_constraint:"+str(current_constraint))
		#logging.debug("current_args:"+str(current_args))

		constraint_name = current_args["constraint_name"]
		if current_constraint["constraint_type"] == "Absolute File Row Count":
			test_result = self.checkAbsoluteFileConstraint(current_args['lb_cnt'],current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative File Row Count":
			test_result = self.checkRelativeFileConstraint(current_args['lb_ratio'],current_args['ub_ratio'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Cardinality":
			test_result = self.checkAbsoluteDimensionCrossProductConstraint(current_args['dimension_column_index_list'],current_args['lb_cnt'],current_args['ub_cnt '],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Column Cardinality":
			test_result = self.checkAbsoluteColumnCardinalityConstraint(current_args["column_name"], current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Column Cardinality":
			test_result = self.checkRelativeColumnCardinalityConstraint(current_args["column_name"], current_args['lb_ratio'], current_args['ub_ratio'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Column Null Count":
			test_result = self.checkAbsoluteColumnNullCountConstraint(current_args["column_name"], current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Column Null Count":
			test_result = self.checkRelativeColumnNullCountConstraint(current_args["column_name"], current_args['lb_ratio'], current_args['ub_ratio'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Cardinality":
			test_result = self.checkRelativeDimensionCrossProductCardinalityConstraint(	current_args['dimension_column_index_list'],current_args['lb_ratio'],current_args['ub_ratio'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Row Count":
			test_result = self.checkAbsoluteDimensionCrossProductElementRowCountConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Cardinality":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureCardinalityConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Null Count":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureNullCountConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Row Count":
			test_result = self.checkRelativeDimensionCrossProductElementRowCountConstraint(current_args['Column_List'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Cardinality":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureCardinalityConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Null Count":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureNullCountConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Min":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureMinConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Max":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureMaxConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Sum":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureSumConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Mean":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureMeanConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Median":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureMedianConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Absolute Dimension Cross Product Element Measure Mode":
			test_result = self.checkAbsoluteDimensionCrossProductElementMeasureModeConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Min":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureMinConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Max":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureMaxConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Sum":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureSumConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Mean":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureMeanConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Relative Dimension Cross Product Element Measure Median":
			test_result = self.checkRelativeDimensionCrossProductElementMeasureMedianConstraint(current_args['Column_List'],current_args['Element'],current_args['lb_cnt'], current_args['ub_cnt'],current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Bounded Overlap":
			pass #todo

		elif current_constraint["constraint_type"] == "Column Data Type":
			test_result = self.checkColumnDataTypeConstraint(current_args["column_name"],current_args["data_type"], current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Layout":
			test_result = self.checkDataLayoutConstraint(current_args["data_type_list"], current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Column Name":
			test_result = self.checkColumnNameConstraint(current_args["column_index"], current_args["goal_column_name"], current_args['warn_or_fail'])

		elif current_constraint["constraint_type"] == "Header":
			test_result = self.checkHeaderConstraint(current_args["header_list"], current_args['warn_or_fail'])

		else:
			logging.error("Constraint Type not recognized. The value was:\'"+str(current_constraint["constraint_type"])+'\'')
			raise ValueError

		#logging.debug("test_result:"+str(test_result))
		#if test_result == 1 and current_args['warn_or_fail'] == 1:
		#	logging.error("FAIL")
		#elif test_result == 1 and current_args['warn_or_fail'] == 0:
		#	logging.debug("WARN")
		#elif test_result == 0:
		#	logging.debug("PASS")
		#else:
		#	logging.error("what the fuck is happening")

		if error_ind:
			return -1

		#logging.debug("exit checkConstraintById")
		return test_result

	def checkConstraintByName(self,constraint_name):
		#logging.debug("enter checkConstraintByName()")
		#logging.debug(str('constraint_name_map:')+str(self.constraint_name_map))
		#logging.debug(str('constraint_name_map keys:') + str(self.constraint_name_map.keys()))
		#logging.debug(str('constraint_name_map values:') + str(self.constraint_name_map.values()))
		#for k in self.constraint_name_map.keys():
		#	logging.debug("|->"+str(k).ljust(5)+str(self.constraint_name_map[k]).ljust(30)+"<-|")
		constraint_id = self.constraint_name_map[constraint_name]
		#logging.debug("exit checkConstraintByName()")
		return self.checkConstraintById(constraint_id)











	def checkHeaderConstraint(self, column_names, warn_or_fail):
		running_result = 0
		for i in range(0,len(self.df.columns)):
			try:
				running_result += int(self.df.columns[i] == column_names[i])
			except:
				running_result += 1

		if running_result == 0:
			return 0
		elif warn_or_fail == 0:
			logging.warning("warning in checkHeaderConstraint()")
			return 0
		else:
			return 1

	def checkLayoutConstraint(self, data_type_list, warn_or_fail):
		#logging.debug("enter checkDataLayoutConstraint()")
		running_result = 0
		for i in range(0,len(self.df.columns)): #todo what if supplie layout has incorrect number of columns?
			cname = self.df.columns[i]
			running_result += self.checkColumnDataTypeConstraint(cname, data_type_list[i], 1)
			logging.debug("running_result:"+str(running_result))

		if running_result == 0:
			return 0
		elif warn_or_fail == 0:
			logging.warning("Warning in checkDataLayoutConstraint()")
			return 0
		else:
			return 1

	def checkRelativeColumnCardinalityConstraint(self, column_name,lb_ratio,ub_ratio,warn_or_fail):

		column_index = self.df.columns.get_loc(column_name)

		try:
			if column_name + ' Column Cardinality Ratio' not in self.memoized_values.keys():
				self.memoized_values[column_name + ' Column Cardinality Ratio'] = len(self.df.iloc[:, column_index].unique())

			if lb_ratio <= self.memoized_values[column_name + ' Column Cardinality Ratio'] and self.memoized_values[
				column_name + ' Column Cardinality Ratio'] <= ub_ratio:
				return 0
			elif warn_or_fail == 0:
				logging.warning("Warning in checkRelativeColumnCardinalityConstraint")  # todo make more specific
			elif warn_or_fail == 1:
				logging.error("Error in checkRelativeColumnCardinalityConstraint")  # todo make more specific
				return 1
			else:
				logging.error(
					"what is happening in checkRelativeColumnCardinalityConstraint()")  # todo make more specific
				raise ValueError
				return 1

		except Exception as e:
			logging.error("uncaught exception in checkRelativeColumnCardinalityConstraint()")
			traceback.print_tb()
			return -1

		return 1

	def checkRelativeColumnNullCountConstraint(self, column_name, lb_ratio, ub_ratio, warn_or_fail):
		try:
			if column_name + ' Column Null Count Ratio' not in self.memoized_values.keys():
				try:
					self.memoized_values[column_name + ' Column Null Count Ratio'] = sum(self.df[column_name].isnull())/sum(self.relative_df[column_name].isnull())
				except ZeroDivisionError:
					self.memoized_values[column_name + ' Column Null Count Ratio'] = float('inf')

			if lb_ratio <= self.memoized_values[column_name + ' Column Null Count Ratio'] and self.memoized_values[
				column_name + ' Column Null Count Ratio'] <= ub_ratio:
				return 0
			elif warn_or_fail == 0:
				logging.warning("Warning in checkRelativeColumnNullCountConstraint")  # todo make more specific
			elif warn_or_fail == 1:
				logging.error("Error in checkRelativeColumnNullCountConstraint")  # todo make more specific
				return 1
			else:
				logging.error(
					"what is happening in checkRelativeColumnNullCountConstraint()")  # todo make more specific
				raise ValueError
				return 1

		except Exception as e:
			logging.error("uncaught exception in checkRelativeColumnNullCountConstraint()")
			traceback.print_exc(e) #todo this is not correct i think. also correct this everywhere else in this code
			return -1
		return 1

	def checkRelativeDimensionCrossProductCardinalityConstraint(self, dimension_column_index_list,lb_ratio,ub_ratio,warn_or_fail):
		pass #todo checkRelativeDimensionCrossProductCardinalityConstraint()

	def checkRelativeDimensionCrossProductElementMeasureMaxConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkRelativeDimensionCrossProductElementMeasureMaxConstraint()

	def checkRelativeDimensionCrossProductElementMeasureMeanConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkRelativeDimensionCrossProductElementMeasureMeanConstraint()

	def checkRelativeDimensionCrossProductElementMeasureMedianConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkRelativeDimensionCrossProductElementMeasureMedianConstraint()

	def checkRelativeDimensionCrossProductElementMeasureMinConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkRelativeDimensionCrossProductElementMeasureMinConstraint()

	def checkRelativeDimensionCrossProductElementMeasureSumConstraint(self, Column_List, Element, Lower_Bound,Upper_Bound, Warn_or_Fail):
		pass #todo checkRelativeDimensionCrossProductElementMeasureSumConstraint()

	def checkRelativeFileConstraint(self, lb_ratio,ub_ratio,warn_or_fail):
		try:
			if 'Row Count Ratio' not in self.memoized_values.keys():
				self.memoized_values['Row Count Ratio'] = self.df.shape[0] / self.relative_df.shape[0]

			if lb_ratio <= self.memoized_values['Row Count Ratio'] and self.memoized_values['Row Count Ratio'] <= ub_ratio:
				return 0
			elif warn_or_fail == 0:
				logging.warning("Warning in checkRelativeFileConstraint") #todo make more specific
			elif warn_or_fail == 1:
				logging.error("Error in checkRelativeFileConstraint") #todo make more specific
				return 1
			else:
				logging.error("what is happening in checkRelativeFileConstraint()") #todo make more specific
				raise ValueError
				return 1

		except Exception as e:
			logging.error("uncaught exception in checkAbsoluteFileConstraint()")
			traceback.print_tb()
			return -1
		return 1

	def getNewConstraintId(self):
		return len(self.constraints.keys()) + 1


	#def checkMutuallyExclusiveConstraint(self, column_name, warn_or_fail):

	#	column_index = self.df.columns.get_loc(column_name)

	#	logging.debug("column_index:"+str(column_index))
	#	primary_column = self.df.iloc[:,column_index]
	#	secondary_column = self.relative_df.iloc[:, column_index]

	#	test_result = (len(set(primary_column).intersection(set(primary_column))) == 0)
	#	if test_result == 0:
	#		return 0
	#	elif warn_or_fail == 0:
	#		logging.warning("warning in checkMutuallyExclusiveConstraint")
	#		return 1
	#	else:
	#		return 1

	#def addMutuallyExclusiveConstraint(self,constraint_name, column_index,warn_or_fail):
	#	column_name = self.df.columns[column_index]

	#	new_constraint_id = self.getNewConstraintId()
	#	args_dict = {"constraint_id": new_constraint_id,
	#				 "constraint_name": constraint_name,
	#				 "column_name": column_name,
	#				 "warn_or_fail": warn_or_fail}
	#	self.addConstraintToConstraintMap(new_constraint_id, "Mutually Exclusive", args_dict)
	#	self.mutually_exclusive_constraints.loc[len(self.mutually_exclusive_constraints.index)] = [new_constraint_id,constraint_name,column_name,warn_or_fail]


	def showResults(self):
		print("ID".ljust(5) + "Constraint Name".ljust(52)+"Result")
		for constraint_id in self.constraints.keys():
			print(str(constraint_id).ljust(5)+str(self.constraints[constraint_id]['args']['constraint_name']).ljust(50,'.')+": "+str(self.history[constraint_id]) )

	def writeResultsToCSV(self,path):
		result_df = pd.DataFrame(columns=["constraint_id", 'constraint_name', 'result'])
		for constraint_id in self.constraints.keys():
			result_df.loc[len(result_df.index)] = [constraint_id, self.constraints[constraint_id]['args']['constraint_name'], self.history[constraint_id]]

		with open(path,'w') as f:
			writer = csv.writer(f)

			writer.writerow(['constraint_id','constraint_name', 'result'])
			for index, row in result_df.iterrows():
				writer.writerow([row['constraint_id'],row['constraint_name'],row['result']])


if __name__ == "__main__":
	import doctest
	doctest.testmod(verbose=True)

