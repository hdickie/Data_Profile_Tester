import logging
import pandas as pd
import traceback
import csv
import datetime
import statistics

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s ' + 'ConstraintSet' + ' %(levelname)s| %(message)s')

global stack_depth
stack_depth = 0

global print_logs
print_logs = True


# if you want to add new log levels: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility

def debug(obj):
    if print_logs:
        logging.debug(''.ljust(stack_depth * 2, '.') + str(obj))


def error(obj):
    if print_logs:
        logging.error(''.ljust(stack_depth * 2, '.') + str(obj))


def info(obj):
    if print_logs:
        logging.info(''.ljust(stack_depth * 2, '.') + str(obj))


def warning(obj):
    if print_logs:
        logging.warning(''.ljust(stack_depth * 2, '.') + str(obj))


def critical(obj):
    if print_logs:
        logging.critical(''.ljust(stack_depth * 2, '.') + str(obj))


def create_test_constraint_sets_map_from_xlsx(xlsx_path):
    global stack_depth
    global print_logs

    print_logs = True

    debug("ENTER create_test_constraint_sets_map_from_xlsx()")
    stack_depth += 1

    # Constraint_Set_Name	Constraint_Set_Id	Expected Result	Primary Data Set Name	Secondary Data Set Name
    constraint_set_def_df = pd.read_excel(xlsx_path, sheet_name="Constraint Set Definitions")

    # Constraint_Set_Name	Constraint_Set_Id	Constraint_Name	constraint_type	Dimension_Index_List	Element	lower_bound	upper_bound	Warn_or_Fail
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
    for i in range(0, constraint_set_def_df.shape[0]):
        Constraint_Set_Name = constraint_set_def_df.iloc[i, 0]
        Constraint_Set_Id = constraint_set_def_df.iloc[i, 1]
        Primary_Data_Set_Name = constraint_set_def_df.iloc[i, 2]
        Secondary_Data_Set_Name = constraint_set_def_df.iloc[i, 3]

        # logging.debug("Constraint_Set_Name:"+Constraint_Set_Name)
        # logging.debug("Constraint_Set_Id:" + str(Constraint_Set_Id))
        # logging.debug("Primary_Data_Set_Name:" + Primary_Data_Set_Name)
        # logging.debug("Secondary_Data_Set_Name:" + Secondary_Data_Set_Name)

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
        Expected_Result = constraint_def_df.iloc[i, 3]
        constraint_type = constraint_def_df.iloc[i, 4]
        Dimension_Index_List = constraint_def_df.iloc[i, 5]
        Element = constraint_def_df.iloc[i, 6]
        Measure_Index = constraint_def_df.iloc[i, 7]
        lower_bound = constraint_def_df.iloc[i, 8]
        upper_bound = constraint_def_df.iloc[i, 9]
        Warn_or_Fail = constraint_def_df.iloc[i, 10]

        # print(str(Constraint_Set_Id)+":"+Constraint_Set_Name)

        try:
            constraint_set_id_to_Constraint_Set_Object_map[Constraint_Set_Id].addConstraint(Constraint_Name,
                                                                                            Expected_Result,
                                                                                            constraint_type,
                                                                                            Dimension_Index_List,
                                                                                            Element,
                                                                                            Measure_Index,
                                                                                            lower_bound,
                                                                                            upper_bound,
                                                                                            Warn_or_Fail)
        except Exception as e:
            error(e)
            stack_depth -= 1
            raise e

    stack_depth -= 1
    debug("EXIT create_test_constraint_sets_map_from_xlsx()")
    print_logs = True
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

        running_str = "\n\nConstraint Set #" + str(self.constraint_set_id) + ": " + str(self.constraint_set_name) + "\n"
        running_str += str(len(self.constraint_type_to_constraint_list_map.keys())) + " types of tests have been defined."
        running_str += '\n'
        running_str += "Primary Data Frame Details:\n"
        try:
            running_str += str(self.df.head(1)) + "\n"
        except:
            running_str += "None" + "\n"

        running_str += "\nSecondary Data Frame Details:\n"
        try:
            running_str += str(self.relative_df.head(1)) + "\n"
        except:
            running_str += "None" + "\n"
        running_str += '\n'

        running_str += 'Constraint_Type'.ljust(62)
        running_str += 'Constraint_Name'.ljust(52)
        running_str += 'Constraint_Id'.ljust(15)
        running_str += 'Expected_Result'.ljust(17)
        running_str += 'Dimension_Index_List'.ljust(22)
        running_str += 'Measure_Index'.ljust(15)
        running_str += 'Lower_Bound'.ljust(13)
        running_str += 'Upper_Bound'.ljust(13)
        running_str += 'Warn_or_Fail'.ljust(13)

        running_str += '\n'
        for constraint_type in self.constraint_type_to_constraint_list_map.keys():
            for curr_constr in self.constraint_type_to_constraint_list_map[constraint_type]:
                running_str += constraint_type.ljust(62)
                running_str += str(curr_constr['constraint_name']).ljust(52)
                running_str += str(curr_constr['constraint_id']).ljust(15)
                running_str += str(curr_constr['expected_result']).ljust(17)
                running_str += str(curr_constr['dimension_index_list']).ljust(22)
                running_str += str(curr_constr['measure_index']).ljust(15)
                running_str += str(curr_constr['lower_bound']).ljust(13)
                running_str += str(curr_constr['upper_bound']).ljust(13)
                running_str += str(curr_constr['warn_or_fail']).ljust(13)

                running_str += '\n'

        # af_lines=""
        # rf_lines=""
        # acc_lines=""
        # acnc_lines=""
        # rcc_lines=""
        # rcnc_lines=""

        # Constraint_Name	constraint_type	Dimension_Index_List	Element	lower_bound	upper_bound Expected_Result	Warn_or_Fail
        # for key in self.constraint_id_to_args_dict_map.keys():
        #	if self.constraint_id_to_args_dict_map[key]['constraint_type'] == 'Absolute File Row Count':
        #		af_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_type']).ljust(40)
        #		af_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_id']).ljust(5)
        #		af_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_name']).ljust(40)
        #		af_lines += "".ljust(40)
        #		af_lines += str(self.constraint_id_to_args_dict_map[key]['args']['lower_bound']).ljust(8)
        #		af_lines += str(self.constraint_id_to_args_dict_map[key]['args']['upper_bound']).ljust(8)
        #		#af_lines += str(self.constraint_id_to_args_dict_map[key]['expected_result']['upper_bound']).ljust(8)
        #		af_lines += str(self.constraint_id_to_args_dict_map[key]['args']['warn_or_fail'])
        #	elif self.constraint_id_to_args_dict_map[key]['constraint_type'] == 'Relative File Row Count':
        #		rf_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_type']).ljust(40)
        #		rf_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_id']).ljust(5)
        #		rf_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_name']).ljust(40)
        #		rf_lines += "".ljust(40)
        #		rf_lines += str(self.constraint_id_to_args_dict_map[key]['args']['lower_bound']).ljust(8)
        #		rf_lines += str(self.constraint_id_to_args_dict_map[key]['args']['upper_bound']).ljust(8)
        #		# rf_lines += str(self.constraint_id_to_args_dict_map[key]['expected_result']['upper_bound']).ljust(8)
        #		rf_lines += str(self.constraint_id_to_args_dict_map[key]['args']['warn_or_fail'])
        #	elif self.constraint_id_to_args_dict_map[key]['constraint_type'] == 'Absolute Column Cardinality':
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_type']).ljust(40)
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_id']).ljust(5)
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_name']).ljust(40)
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['dimension_index_list']).ljust(40)
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['lower_bound']).ljust(8)
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['upper_bound']).ljust(8)
        #		# acc_lines += str(self.constraint_id_to_args_dict_map[key]['expected_result']['upper_bound']).ljust(8)
        #		acc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['warn_or_fail'])
        #	elif self.constraint_id_to_args_dict_map[key]['constraint_type'] == 'Absolute Column Null Count':
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_type']).ljust(40)
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_id']).ljust(5)
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_name']).ljust(40)
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['dimension_index_list']).ljust(40)
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['lower_bound']).ljust(8)
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['upper_bound']).ljust(8)
        #		# acnc_lines += str(self.constraint_id_to_args_dict_map[key]['expected_result']['upper_bound']).ljust(8)
        #		acnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['warn_or_fail'])
        #	elif self.constraint_id_to_args_dict_map[key]['constraint_type'] == 'Relative Column Cardinality':
        #		rcc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_type']).ljust(40)
        #		rcc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_id']).ljust(5)
        #		rcc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_name']).ljust(40)
        #		rcc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['lower_bound']).ljust(8)
        #		rcc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['upper_bound']).ljust(8)
        #		# rcc_lines += str(self.constraint_id_to_args_dict_map[key]['expected_result']['upper_bound']).ljust(8)
        #		rcc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['warn_or_fail'])
        #	elif self.constraint_id_to_args_dict_map[key]['constraint_type'] == 'Relative Column Null Count':
        #		rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_type']).ljust(40)
        #		rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_id']).ljust(5)
        #		rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['constraint_name']).ljust(40)
        #		rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['lower_bound']).ljust(8)
        #		rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['upper_bound']).ljust(8)
        #		# rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['expected_result']['upper_bound']).ljust(8)
        #		rcnc_lines += str(self.constraint_id_to_args_dict_map[key]['args']['warn_or_fail'])

        # running_str += "Absolute File Constraints" + "\n"
        # if af_lines == "":
        #	running_str += "None" + "\n"
        # else:
        #	running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "".ljust(40,'.') + "lower_bound".ljust(8,'.') + "upper_bound".ljust(8,'.') + "Warn_or_Fail" + '\n'
        #	running_str += af_lines + "\n"
        # running_str += '\n'

        # running_str += "Relative File Constraints" + "\n"
        # if rf_lines == "":
        #	running_str += "None" + "\n"
        # else:
        #	running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "lower_bound".ljust(8,'.') + "upper_bound".ljust(8,'.') + "Warn_or_Fail" + '\n'
        #	running_str += rf_lines + "\n"
        # running_str += '\n'

        # running_str += "Absolute Column Cardinality" + "\n"
        # if acc_lines == "":
        #	running_str += "None" + "\n"
        # else:
        #	running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "column_name".ljust(40,'.') + "lower_bound".ljust(8,'.') + "upper_bound".ljust(8,'.') + "Warn_or_Fail" + '\n'
        #	running_str += acc_lines + "\n"
        # running_str += '\n'

        # running_str += "Relative Column Cardinality" + "\n"
        # if rcc_lines == "":
        #	running_str += "None" + "\n"
        # else:
        #	running_str += rcc_lines + "\n"
        # running_str += '\n'

        # running_str += "Absolute Column Null Count" + "\n"
        # if acnc_lines == "":
        #	running_str += "None" + "\n"
        # else:
        #	running_str += "ID".ljust(5,'.') + "Name".ljust(40,'.') + "column_name".ljust(40,'.') + "lower_bound".ljust(8,'.') + "upper_bound".ljust(8,'.') + "Warn_or_Fail" + '\n'
        #	running_str += acnc_lines + "\n"
        # running_str += '\n'

        # running_str += "Relative Column Null Count" + "\n"
        # if rcnc_lines == "":
        #	running_str += "None" + "\n"
        # else:
        #	running_str += rcnc_lines + "\n"
        # running_str += '\n'

        # running_str += "Constraint Name Map"
        # running_str += str(self.constraint_name_map)

        return running_str

    def __init__(self, constraint_set_name, constraint_set_id, df, relative_df):
        # todo input parameter validation

        self.constraint_set_name = constraint_set_name
        self.constraint_set_id = constraint_set_id
        self.df = df.copy()

        if relative_df is not None:
            self.relative_df = relative_df.copy()

        self.memoized_values = {}
        self.test_results = {}
        self.constraint_name_map = {}
        self.constraint_id_to_args_dict_map = {}
        self.constraint_type_to_constraint_list_map = {}

    # def addAbsoluteFileRowCountConstraint(self,constraint_name, lower_bound,upper_bound,warn_or_fail):
    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id":new_constraint_id,
    #				"constraint_name":constraint_name,
    #				 "lower_bound":lower_bound,
    #				 "upper_bound":upper_bound,
    #				 "warn_or_fail":warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Absolute File", args_dict)
    #	self.absolute_file_constraints.loc[len(self.absolute_file_constraints.index)] = [new_constraint_id,
    #																					 constraint_name,
    #																					 lower_bound,
    #																					 upper_bound,
    #																					 warn_or_fail]

    # def addColumnDataTypeConstraint(self,constraint_name, column_index,data_type,warn_or_fail):
    #	column_name = self.df.columns[column_index]

    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "column_name": column_name,
    #				 "data_type": data_type,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Column Data Type", args_dict)
    #	self.column_data_type_constraints.loc[len(self.column_data_type_constraints.index)] = [new_constraint_id,
    #																								 constraint_name,
    #																								 column_name,
    #																								 data_type,
    #																								 warn_or_fail]

    # def addColumnNameConstraint(self,constraint_name, column_index,goal_column_name,warn_or_fail):

    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "column_index": column_index,
    #				 "goal_column_name": goal_column_name,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Column Name", args_dict)
    #	self.column_name_constraints.loc[len(self.column_name_constraints.index)] = [new_constraint_id,
    #																				 constraint_name,
    #																				 column_index,
    #																				 goal_column_name,
    #																				 warn_or_fail]

    # def addAbsoluteColumnConstraint(self,Constraint_Name,Expected_Result ,Dimension_Index_List,Fun,lower_bound, upper_bound,Warn_or_Fail):
    #	new_constraint_id = self.getNewConstraintId()
    #
    #	if Fun == 'cardinality':
    #		constraint_type = 'Absolute Column Cardinality'
    #	elif Fun == 'null count':
    #		constraint_type = 'Absolute Column Null Count'
    #	else:
    #		try:
    #			pass #compute input Function
    #		except Exception as e:
    #			error(e)
    #			raise e
    #
    #	constraint_def_dict = {'constraint_name': Constraint_Name,
    #										   'expected_result': Expected_Result,
    #										   'constraint_type': constraint_type,
    #										   'Dimension_Index_List': Dimension_Index_List,
    #										   'element': None,
    #										   'lower_bound': lower_bound,
    #										   'upper_bound': upper_bound,
    #										   'warn_or_fail': Warn_or_Fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, constraint_def_dict)

    def addConstraint(self, Constraint_Name, Expected_Result, constraint_type, Dimension_Index_List, Element,
                      Measure_Index, lower_bound, upper_bound, Warn_or_Fail):
        global stack_depth
        global print_logs
        debug("ENTER addConstraint()")
        stack_depth += 1

        try:
            if not pd.isna(Measure_Index):
                try:
                    int(Measure_Index)
                except Exception as e:
                    error("Measure Index was not an integer:"+str(Measure_Index))
                    raise e

                try:
                    self.df.iloc[:, Measure_Index]
                except IndexError as e:
                    error("Measure_Index caused subscript out of bounds error")
                    raise e

            if Warn_or_Fail != 0 and Warn_or_Fail != 1:
                error("Warn_or_Fail was not 0 or 1")
                raise ValueError

        except Exception as e:
            error("Exception caught during input parameter validation in addConstraint()")
            error(str(constraint_type)+" "+str(Constraint_Name)+" will not be added to Constraint Set # "+str(self.constraint_set_id))
            error(e)
            stack_depth -= 1
            return


        new_constraint_id = self.getNewConstraintId()
        constraint_type_lower = constraint_type.lower()
        if not pd.isna(Measure_Index):
            Measure_Index = int(Measure_Index)

        if constraint_type_lower == "absolute file row count" \
                or constraint_type_lower == "absolute dimension cross product element row count" \
                or constraint_type_lower == "relative file row count" \
                or constraint_type_lower == "relative dimension cross product element row count":
            Fun = 'count'
        elif constraint_type_lower == "absolute column cardinality" \
                or constraint_type_lower == "absolute dimension cross product cardinality" \
                or constraint_type_lower == "absolute dimension cross product element measure cardinality" \
                or constraint_type_lower == "relative column cardinality" \
                or constraint_type_lower == "relative dimension cross product cardinality" \
                or constraint_type_lower == "relative dimension cross product element measure cardinality":
            Fun = 'cardinality'
        elif constraint_type_lower == "absolute column null count" \
                or constraint_type_lower == "absolute dimension cross product element measure null count" \
                or constraint_type_lower == "relative column null count" \
                or constraint_type_lower == "relative dimension cross product element measure null count":
            Fun = 'null count'
        elif constraint_type_lower == "absolute dimension cross product element measure min" \
                or constraint_type_lower == "relative dimension cross product element measure min":
            Fun = 'min'
        elif constraint_type_lower == "absolute dimension cross product element measure max" \
                or constraint_type_lower == "relative dimension cross product element measure max":
            Fun = 'max'
        elif constraint_type_lower == "absolute dimension cross product element measure mean" \
                or constraint_type_lower == "relative dimension cross product element measure mean":
            Fun = 'mean'
        elif constraint_type_lower == "absolute dimension cross product element measure median" \
                or constraint_type_lower == "relative dimension cross product element measure median":
            Fun = 'median'
        elif constraint_type_lower == "absolute dimension cross product element measure mode":
            Fun = 'mode'
        elif constraint_type_lower == "absolute dimension cross product element measure sum" \
                or constraint_type_lower == "relative dimension cross product element measure sum":
            Fun = 'sum'
        elif constraint_type_lower == "bounded overlap" or \
             constraint_type_lower == "layout" or \
             constraint_type_lower == "header" or \
             constraint_type_lower == "column data type" or \
             constraint_type_lower == "column name":
            Fun = constraint_type_lower
        else:
            debug('unknown constraint type:' + constraint_type_lower)
            debug("We will proceed interpreting constraint type as a python data frame column aggregation function")
            Fun = constraint_type

        constraint_def_dict = {'constraint_name': Constraint_Name,
                               'constraint_id': new_constraint_id,
                               'expected_result': Expected_Result,
                               'constraint_type': constraint_type_lower,
                               'fun': Fun,
                               'dimension_index_list': Dimension_Index_List,
                               'element': Element,
                               'measure_index': Measure_Index,
                               'lower_bound': lower_bound,
                               'upper_bound': upper_bound,
                               'warn_or_fail': Warn_or_Fail}

        self.constraint_id_to_args_dict_map[new_constraint_id] = {"constraint_type": constraint_type,
                                                                  "args": constraint_def_dict}
        self.constraint_name_map[constraint_def_dict["constraint_name"]] = new_constraint_id
        debug(constraint_type)

        if constraint_type in self.constraint_type_to_constraint_list_map.keys():
            self.constraint_type_to_constraint_list_map[constraint_type] += [constraint_def_dict]
        else:
            self.constraint_type_to_constraint_list_map[constraint_type] = [constraint_def_dict]
        debug(self.constraint_type_to_constraint_list_map[constraint_type])

        stack_depth -= 1
        debug("EXIT addConstraint()")

    # def addDataLayoutConstraint(self,constraint_name, data_type_list,warn_or_fail):
    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "data_type_list": data_type_list,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Data Layout", args_dict)
    #	self.data_layout_constraints.loc[len(self.data_layout_constraints.index)] = [new_constraint_id,
    #																						   constraint_name,
    #																						   data_type_list,
    #																						   warn_or_fail]

    # def addHeaderConstraint(self,constraint_name, column_names,warn_or_fail):
    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "header_list": column_names,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Header", args_dict)
    #	self.header_constraints.loc[len(self.header_constraints.index)] = [new_constraint_id,
    #																				 constraint_name,
    #																				 column_names,
    #																				 warn_or_fail]

    # def addRelativeColumnConstraint(self,constraint_name, column_index,Fun,lower_bound,upper_bound,warn_or_fail):
    #	column_name = self.df.columns[column_index]

    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "column_name": column_name,
    #				 "lower_bound": lower_bound,
    #				 "upper_bound": upper_bound,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Relative Column Cardinality", args_dict)
    #	self.relative_column_cardinality_constraints.loc[len(self.relative_column_cardinality_constraints.index)] = [
    #		new_constraint_id,
    #		constraint_name,
    #		column_name,
    #		lower_bound,
    #		upper_bound,
    #		warn_or_fail]

    # def addRelativeFileRowCountConstraint(self,constraint_name, lower_bound,upper_bound,warn_or_fail):
    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "lower_bound": lower_bound,
    #				 "upper_bound": upper_bound,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Relative File", args_dict)
    #	self.relative_file_constraints.loc[len(self.relative_file_constraints.index)] = [new_constraint_id,
    #																					 constraint_name,
    #																					 lower_bound,
    #																					 upper_bound,
    #																					 warn_or_fail]

    # def checkAbsoluteColumnConstraint(self, column_name, Fun, lower_bound, upper_bound, warn_or_fail):
    #	memo_key = 'Primary' + column_name + ' Column ' + Fun
    #	if memo_key not in self.memoized_values.keys():
    #		if Fun.lower() == 'sum':
    #			self.memoized_values[memo_key] = sum(self.df[column_name])
    #		elif Fun.lower() == 'null count':
    #			self.memoized_values[memo_key] = sum(self.df[column_name].isnull())
    #		elif Fun.lower() == 'cardinality':
    #			self.memoized_values[memo_key] = len(self.df[column_name].unique())
    #
    #	if lower_bound <= self.memoized_values[memo_key] and self.memoized_values[memo_key] <= upper_bound:
    #		return 0
    #	elif warn_or_fail == 0:
    #		logging.warning("Warning in checkAbsoluteColumnConstraint")
    #	elif warn_or_fail == 1:
    #		error("Error in checkAbsoluteColumnConstraint")
    #		return 1
    #	else:
    #		error("what is happening in checkAbsoluteColumnConstraint()")
    #		raise ValueError
    #		return 1
    #	return 1

    # def checkRelativeColumnConstraint(self, column_name, Fun, lower_bound, upper_bound, warn_or_fail):
    #	primary_memo_key = 'Primary' + column_name + ' Column ' + Fun
    #	if primary_memo_key not in self.memoized_values.keys():
    #		if Fun.lower() == 'sum':
    #			self.memoized_values[primary_memo_key] = sum(self.df[column_name])
    #		elif Fun.lower() == 'null count':
    #			self.memoized_values[primary_memo_key] = sum(self.df[column_name].isnull())
    #		elif Fun.lower() == 'cardinality':
    #			self.memoized_values[primary_memo_key] = len(self.df[column_name].unique())

    #	secondary_memo_key = 'Secondary' + column_name + ' Column ' + Fun
    #	if secondary_memo_key not in self.memoized_values.keys():
    #		if Fun.lower() == 'sum':
    #			self.memoized_values[secondary_memo_key] = sum(self.df[column_name])
    #		elif Fun.lower() == 'null count':
    #			self.memoized_values[secondary_memo_key] = sum(self.df[column_name].isnull())
    #		elif Fun.lower() == 'cardinality':
    #			self.memoized_values[secondary_memo_key] = len(self.df[column_name].unique())
    #
    #	memo_key = column_name + ' Column ' + Fun + ' Ratio'
    #	if memo_key not in self.memoized_values.keys():
    #		try:
    #			self.memoized_values[memo_key] = self.memoized_values[primary_memo_key]/self.memoized_values[secondary_memo_key]
    #		except ZeroDivisionError:
    #			self.memoized_values[memo_key] = float('inf')
    #
    #	if lower_bound <= self.memoized_values[memo_key] and self.memoized_values[memo_key] <= upper_bound:
    #		return 0
    #	elif warn_or_fail == 0:
    #		logging.warning("Warning in checkRelativeColumnConstraint")
    #	elif warn_or_fail == 1:
    #		error("Error in checkRelativeColumnConstraint")
    #		return 1
    #	else:
    #		error("what is happening in checkRelativeColumnConstraint()")
    #		raise ValueError
    #		return 1
    #	return 1

    # def checkAbsoluteDimensionCrossProductElementMeasureConstraint(self, Dimension_Index_List,
    #																  Dimension_Cross_Product_Element,
    #																  Measure_Column_Index, Fun, lower_bound, upper_bound,
    #																  Warn_or_Fail):
    #	memo_key = 'Primary '+str(Dimension_Index_List)+' '+str(Dimension_Cross_Product_Element)+' '+str(Measure_Column_Index)+' '+str(Fun)
    #	if memo_key not in self.memoized_values.keys():
    #		dimension_column_names_list = []
    #		for column_index in Dimension_Index_List.split(','):
    #			dimension_column_names_list += [self.df.columns[column_index]]

    #		element_values_list = []
    #		for element_value in Dimension_Cross_Product_Element.split(','):
    #			dimension_column_names_list += [element_value]

    #		measure_column_name = self.df.columns[Measure_Column_Index]

    #		try:
    #			result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[Fun].reset_index()
    #		except Exception as e:
    #			logging.debug(e)
    #			return -1

    # selecting the Dimension_Cross_Product_Element row from the pivot table output by the aggregation
    #		for i in range(0, len(dimension_column_names_list)):
    #			if i == 0:
    #				sel_vec = result_set_df.loc[result_set_df[dimension_column_names_list[i]] == element_values_list[i]]
    #			else:
    #				sel_vec = sel_vec & (
    #					result_set_df.loc[result_set_df[dimension_column_names_list[i]] == element_values_list[i]])
    #	else:
    #		#value is memoized
    #		if lower_bound <= self.memoized_values[memo_key] and self.memoized_values[memo_key] <= upper_bound:
    #			return 0
    #		elif Warn_or_Fail == 0:
    #			logging.warning("Warning in checkAbsoluteDimensionCrossProductElementMeasureConstraint")
    #		elif Warn_or_Fail == 1:
    #			error("Error in checkAbsoluteDimensionCrossProductElementMeasureConstraint")
    #			return 1
    #		else:
    #			error("what is happening in checkAbsoluteDimensionCrossProductElementMeasureConstraint()")
    #			raise ValueError
    #			return 1
    #		return 1

    def checkAllConstraints(self, printResults=True, outputFolder=None):
        global stack_depth
        global print_logs
        debug("ENTER checkAllConstraints()")
        stack_depth += 1
        for constraint_id in self.constraint_id_to_args_dict_map.keys():
            debug("Passing constraint_id to checkConstraintById():"+str(type(constraint_id)))
            debug(str(constraint_id))
            self.test_results[constraint_id] = self.checkConstraintById(constraint_id)

        if printResults:
            self.showResults()

        if outputFolder is not None:
            self.writeResultsToCSV(outputFolder + '//Data_Profile_Test_Results_' + datetime.datetime.now().strftime(
                "%Y%m%d_%H%M%S") + '.txt')
        stack_depth -= 1
        debug("EXIT checkAllConstraints")

    # def checkColumnDataTypeConstraint(self, column_name, data_type, warn_or_fail):
    #	column_index = self.df.columns.get_loc(column_name)
    #	#logging.debug("enter checkColumnDataTypeConstraint()")
    #	#logging.debug("column_name:"+str(column_name))
    #	#logging.debug("goal data_type:" + str(data_type))
    #	#logging.debug("actual data_type:" + str(self.df.dtypes[column_index]))

    #	if self.df.dtypes[column_index] == data_type:
    #		return 0
    #	elif warn_or_fail == 0:
    #		error("Warning in checkColumnDataTypeConstraint()")
    #		return 0
    #	else:
    #		return 1

    # def checkColumnNameConstraint(self, column_index, goal_column_name, warn_or_fail):
    #	test_result = self.df.columns[column_index] == goal_column_name
    #
    #	if test_result == 0:
    #		return 0
    #	elif warn_or_fail == 0:
    #		logging.warning("warning in checkColumnNameConstraint()")
    #		return 0
    #	else:
    #		return 1

    def checkAbsoluteConstraint(self, Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
                                Measure_Index, lower_bound, upper_bound, Warn_or_Fail):
        global stack_depth
        global print_logs
        debug("ENTER checkAbsoluteConstraint()")
        debug('Expected_Result:' + str(Expected_Result))
        debug('constraint_type:' + str(constraint_type))
        debug('Fun:' + str(Fun))
        debug('Dimension_Index_List:' + str(Dimension_Index_List))
        debug('Element:' + str(Element))
        debug('Measure_Index:' + str(Measure_Index))
        debug('lower_bound:' + str(lower_bound))
        debug('upper_bound:' + str(upper_bound))
        debug('Warn_or_Fail:' + str(Warn_or_Fail))

        #debug(self)

        stack_depth += 1
        memo_key = 'Primary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
            Fun)

        debug("memo_key:" + str(memo_key))
        debug("Dimension_Index_List:" + str(Dimension_Index_List))
        debug("Dimension_Index_List is.na : " + str(pd.isna(Dimension_Index_List)))

        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")
            if not pd.isna(Dimension_Index_List):
                debug("Dimension_Index_List is not NaN")
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [self.df.columns[int(column_index)]]

            if not pd.isna(Element):
                debug("Element is not NaN")
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            if not pd.isna(Measure_Index):
                debug("Measure_Index is not NaN")
                debug('Measure_Index:' + str(Measure_Index))
                measure_column_name = self.df.columns[Measure_Index]

            debug("Fun:" + str(Fun))
            try:
                # todo if branch on Fun to avoid exceptions
                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index) and Fun == 'count':  # File Row Count Case
                    debug("Computing result for File Row Count Case")
                    result_value = self.df.shape[0]
                    debug('SET result_value = ' + str(result_value))
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index) and Fun == 'cardinality':  # Dimension Cross Product Cardinality Case
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    result_value = self.df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]
                    debug('SET result_value = ' + str(result_value))
                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # single column case
                    if Fun == 'cardinality':
                        result_value = self.df.iloc[:, Measure_Index].nunique()
                    elif Fun == 'null count':
                        result_value = sum(self.df.iloc[:, Measure_Index].isnull())
                    elif Fun == 'min':
                        result_value = min(self.df.iloc[:, Measure_Index])
                    elif Fun == 'mean':
                        result_value = statistics.mean(self.df.iloc[:, Measure_Index])
                    elif Fun == 'median':
                        result_value = statistics.median(self.df.iloc[:, Measure_Index])
                    elif Fun == 'mode':
                        result_value = statistics.mode(self.df.iloc[:, Measure_Index])
                    elif Fun == 'max':
                        result_value = max(self.df.iloc[:, Measure_Index])
                    elif Fun == 'max':
                        result_value = max(self.df.iloc[:, Measure_Index])
                    else:
                        pass
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # single column case
                    if Fun == 'cardinality':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'nunique'].reset_index()
                    elif Fun == 'mode':
                        result_value = -1  # todo
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'min'].reset_index()
                    elif Fun == 'min':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'min'].reset_index()
                    elif Fun == 'mean':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'mean'].reset_index()
                    elif Fun == 'median':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'median'].reset_index()
                    elif Fun == 'mode':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'mode'].reset_index()
                    elif Fun == 'max':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()
                    else:
                        result_value = -1  # todo
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(Measure_Index):  # Absolute Dimension Cross Product Element Row Count
                    result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                else:
                    debug("Unmapped parameter combination ZZZ")
                    debug('Dimension_Index_List:' + str(pd.isna(Dimension_Index_List)))
                    debug('Element:' + str(pd.isna(Element)))
                    debug('Measure_Index:' + str(pd.isna(Measure_Index)))
                    debug('Fun:' + str(Fun))
                    result_value = -1  # todo

            except Exception as e:
                if Expected_Result == 'ERR':
                    stack_depth -= 1
                    debug("EXIT checkAbsoluteConstraint()")
                    return 0
                debug(e)
                stack_depth -= 1
                debug("EXIT checkAbsoluteConstraint()")
                return -1

            # selecting the Dimension_Cross_Product_Element row from the pivot table output by the aggregation
            if len(dimension_column_names_list) > 0 and not pd.isna(Element):
                for i in range(0, len(dimension_column_names_list)):
                    debug("iterating sel vec. current column:"+str(dimension_column_names_list[i]))
                    debug("whole list:"+str(dimension_column_names_list))
                    if i == 0:
                        sel_vec = result_set_df.loc[result_set_df[dimension_column_names_list[i]] == element_values_list[i]]
                    else:
                        sel_vec = sel_vec & (
                            result_set_df.loc[result_set_df[dimension_column_names_list[i]] == element_values_list[i]])
                    debug("....sel_vec:" + str(sel_vec))

                debug("A")
                debug("RECORDING MEMOIZED VALUE:")
                debug("B")
                debug("memo_key".ljust(15, '.') + ":" + str(memo_key))
                debug("C")
                result_value = result_set_df[sel_vec, len(result_set_df.columns) - 1]
                debug("result_value".ljust(15, '.') + ":" + str(result_value))
            else:
                debug("D")
                debug("RECORDING MEMOIZED VALUE:")
                debug("memo_key".ljust(15, '.') + ":" + str(memo_key))
                debug("E")
                debug("result_value".ljust(15, '.') + ":" + str(result_value))
                debug("F")
                self.memoized_values[memo_key] = result_value
                debug("G")

        # value is memoized
        initial_result = int(lower_bound <= self.memoized_values[memo_key] and self.memoized_values[memo_key] <= upper_bound)
        debug("initial_result:" + str(initial_result))
        if (initial_result == 0 and Expected_Result == 'PASS') or (initial_result == 1 and Expected_Result == 'FAIL'):
            stack_depth -= 1
            debug("EXIT checkAbsoluteConstraint()")
            return 0
        elif Warn_or_Fail == 0:
            warning(
                "Warning in checkAbsoluteConstraint")  # todo make more specific
        elif Warn_or_Fail == 1:
            error("Error in checkAbsoluteConstraint")  # todo make more specific
            stack_depth -= 1
            debug("EXIT checkAbsoluteConstraint()")
            return 1
        else:
            error("Unmapped constraint results")
            error("initial_result:"+str(initial_result))
            error("Expected_Result:" + str(Expected_Result))
            error("Warn_or_Fail:" + str(Warn_or_Fail))
            stack_depth -= 1
            debug("EXIT checkAbsoluteConstraint()")
            raise ValueError
            return 1
        stack_depth -= 1
        debug("EXIT checkAbsoluteConstraint()")
        return 1

    def checkRelativeConstraint(self, Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
                                Measure_Index, lower_bound, upper_bound, Warn_or_Fail):
        global stack_depth
        global print_logs
        debug("ENTER checkRelativeConstraint()")
        stack_depth += 1
        memo_key = 'Primary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
            Fun)
        primary_memo_key = memo_key

        debug("memo_key:" + str(memo_key))
        debug("Dimension_Index_List:" + str(Dimension_Index_List))
        debug("Dimension_Index_List is.na : " + str(pd.isna(Dimension_Index_List)))

        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")
            if not pd.isna(Dimension_Index_List):
                debug("Dimension_Index_List is not NaN")
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [self.df.columns[int(column_index)]]

            if not pd.isna(Element):
                debug("Element is not NaN")
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            if not pd.isna(Measure_Index):
                debug("Measure_Index is not NaN")
                debug('Measure_Index:' + str(Measure_Index))
                measure_column_name = self.df.columns[Measure_Index]

            debug("Fun:" + str(Fun))
            try:
                # todo if branch on Fun to avoid exceptions
                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index) and Fun == 'count':  # File Row Count Case
                    debug("Computing result for File Row Count Case")
                    result_value = self.df.shape[0]
                    debug('SET result_value = ' + str(result_value))
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index) and Fun == 'cardinality':  # Dimension Cross Product Cardinality Case
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    result_value = self.df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]
                    debug('SET result_value = ' + str(result_value))
                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # single column case
                    if Fun == 'cardinality':
                        result_value = self.df.iloc[:, Measure_Index].nunique()
                    elif Fun == 'null count':
                        result_value = sum(self.df.iloc[:, Measure_Index].isnull())
                    elif Fun == 'min':
                        result_value = min(self.df.iloc[:, Measure_Index])
                    elif Fun == 'mean':
                        result_value = statistics.mean(self.df.iloc[:, Measure_Index])
                    elif Fun == 'median':
                        result_value = statistics.median(self.df.iloc[:, Measure_Index])
                    elif Fun == 'mode':
                        result_value = statistics.mode(self.df.iloc[:, Measure_Index])
                    elif Fun == 'max':
                        result_value = max(self.df.iloc[:, Measure_Index])
                    elif Fun == 'max':
                        result_value = max(self.df.iloc[:, Measure_Index])
                    else:
                        pass
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # single column case
                    if Fun == 'cardinality':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'nunique'].reset_index()
                    elif Fun == 'mode':
                        result_value = -1  # todo
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'min'].reset_index()
                    elif Fun == 'min':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'min'].reset_index()
                    elif Fun == 'mean':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'mean'].reset_index()
                    elif Fun == 'median':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'median'].reset_index()
                    elif Fun == 'mode':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'mode'].reset_index()
                    elif Fun == 'max':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()
                    else:
                        result_value = -1  # todo
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(Measure_Index):  # Relative Dimension Cross Product Element Row Count
                    result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                else:
                    debug("Unmapped parameter combination ZZZ")
                    debug('Dimension_Index_List:' + str(pd.isna(Dimension_Index_List)))
                    debug('Element:' + str(pd.isna(Element)))
                    debug('Measure_Index:' + str(pd.isna(Measure_Index)))
                    debug('Fun:' + str(Fun))
                    result_value = -1  # todo

            except Exception as e:
                if Expected_Result == 'ERR':
                    stack_depth -= 1
                    debug("EXIT checkAbsoluteConstraint()")
                    return 0
                debug(e)
                stack_depth -= 1
                debug("EXIT checkAbsoluteConstraint()")
                return -1

            # selecting the Dimension_Cross_Product_Element row from the pivot table output by the aggregation
            if len(dimension_column_names_list) > 0 and not pd.isna(Element):
                for i in range(0, len(dimension_column_names_list)):
                    debug("iterating sel vec. current column:" + str(dimension_column_names_list[i]))
                    debug("whole list:" + str(dimension_column_names_list))
                    if i == 0:
                        sel_vec = result_set_df.loc[
                            result_set_df[dimension_column_names_list[i]] == element_values_list[i]]
                    else:
                        sel_vec = sel_vec & (
                            result_set_df.loc[result_set_df[dimension_column_names_list[i]] == element_values_list[i]])
                    debug("....sel_vec:" + str(sel_vec))

                debug("A")
                debug("RECORDING MEMOIZED VALUE:")
                debug("B")
                debug("memo_key".ljust(15, '.') + ":" + str(memo_key))
                debug("C")
                result_value = result_set_df[sel_vec, len(result_set_df.columns) - 1]
                debug("result_value".ljust(15, '.') + ":" + str(result_value))
            else:
                debug("D")
                debug("RECORDING MEMOIZED VALUE:")
                debug("memo_key".ljust(15, '.') + ":" + str(memo_key))
                debug("E")
                debug("result_value".ljust(15, '.') + ":" + str(result_value))
                debug("F")
                self.memoized_values[memo_key] = result_value
                debug("G")






        memo_key = 'Secondary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
            Fun)
        secondary_memo_key = memo_key

        debug("memo_key:" + str(memo_key))
        debug("Dimension_Index_List:" + str(Dimension_Index_List))
        debug("Dimension_Index_List is.na : " + str(pd.isna(Dimension_Index_List)))

        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")
            if not pd.isna(Dimension_Index_List):
                debug("Dimension_Index_List is not NaN")
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [self.relative_df.columns[int(column_index)]]

            if not pd.isna(Element):
                debug("Element is not NaN")
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            if not pd.isna(Measure_Index):
                debug("Measure_Index is not NaN")
                debug('Measure_Index:' + str(Measure_Index))
                measure_column_name = self.relative_df.columns[Measure_Index]

            debug("Fun:" + str(Fun))
            try:
                # todo if branch on Fun to avoid exceptions
                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index) and Fun == 'count':  # File Row Count Case
                    debug("Computing result for File Row Count Case")
                    result_value = self.relative_df.shape[0]
                    debug('SET result_value = ' + str(result_value))
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index) and Fun == 'cardinality':  # Dimension Cross Product Cardinality Case
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    result_value = self.relative_df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]
                    debug('SET result_value = ' + str(result_value))
                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # single column case
                    if Fun == 'cardinality':
                        result_value = self.relative_df.iloc[:, Measure_Index].nunique()
                    elif Fun == 'null count':
                        result_value = sum(self.relative_df.iloc[:, Measure_Index].isnull())
                    elif Fun == 'min':
                        result_value = min(self.relative_df.iloc[:, Measure_Index])
                    elif Fun == 'mean':
                        result_value = statistics.mean(self.relative_df.iloc[:, Measure_Index])
                    elif Fun == 'median':
                        result_value = statistics.median(self.relative_df.iloc[:, Measure_Index])
                    elif Fun == 'mode':
                        result_value = statistics.mode(self.relative_df.iloc[:, Measure_Index])
                    elif Fun == 'max':
                        result_value = max(self.relative_df.iloc[:, Measure_Index])
                    elif Fun == 'max':
                        result_value = max(self.relative_df.iloc[:, Measure_Index])
                    else:
                        pass
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(Measure_Index):  # single column case
                    if Fun == 'cardinality':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'nunique'].reset_index()
                    elif Fun == 'mode':
                        result_value = -1  # todo
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'min'].reset_index()
                    elif Fun == 'min':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'min'].reset_index()
                    elif Fun == 'mean':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'mean'].reset_index()
                    elif Fun == 'median':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'median'].reset_index()
                    elif Fun == 'mode':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'mode'].reset_index()
                    elif Fun == 'max':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()
                    else:
                        result_value = -1  # todo
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(Measure_Index):  # Relative Dimension Cross Product Element Row Count
                    result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                else:
                    debug("Unmapped parameter combination ZZZ")
                    debug('Dimension_Index_List:' + str(pd.isna(Dimension_Index_List)))
                    debug('Element:' + str(pd.isna(Element)))
                    debug('Measure_Index:' + str(pd.isna(Measure_Index)))
                    debug('Fun:' + str(Fun))
                    result_value = -1  # todo

            except Exception as e:
                if Expected_Result == 'ERR':
                    stack_depth -= 1
                    debug("EXIT checkAbsoluteConstraint()")
                    return 0
                debug(e)
                stack_depth -= 1
                debug("EXIT checkAbsoluteConstraint()")
                return -1

            # selecting the Dimension_Cross_Product_Element row from the pivot table output by the aggregation
            if len(dimension_column_names_list) > 0 and not pd.isna(Element):
                for i in range(0, len(dimension_column_names_list)):
                    debug("iterating sel vec. current column:" + str(dimension_column_names_list[i]))
                    debug("whole list:" + str(dimension_column_names_list))
                    if i == 0:
                        sel_vec = result_set_df.loc[
                            result_set_df[dimension_column_names_list[i]] == element_values_list[i]]
                    else:
                        sel_vec = sel_vec & (
                            result_set_df.loc[result_set_df[dimension_column_names_list[i]] == element_values_list[i]])
                    debug("....sel_vec:" + str(sel_vec))

                debug("A")
                debug("RECORDING MEMOIZED VALUE:")
                debug("B")
                debug("memo_key".ljust(15, '.') + ":" + str(memo_key))
                debug("C")
                result_value = result_set_df[sel_vec, len(result_set_df.columns) - 1]
                debug("result_value".ljust(15, '.') + ":" + str(result_value))
            else:
                debug("D")
                debug("RECORDING MEMOIZED VALUE:")
                debug("memo_key".ljust(15, '.') + ":" + str(memo_key))
                debug("E")
                debug("result_value".ljust(15, '.') + ":" + str(result_value))
                debug("F")
                self.memoized_values[memo_key] = result_value
                debug("G")

        try:
            initial_result_value = int(self.memoized_values[primary_memo_key]) / int(self.memoized_values[secondary_memo_key])
        except ZeroDivisionError:
            initial_result_value = float('inf')

        initial_result = (float(lower_bound) <= initial_result_value and initial_result_value <= float(upper_bound))
        if (initial_result == 0 and Expected_Result == 'PASS') or (initial_result == 1 and Expected_Result == 'FAIL'):
            stack_depth -= 1
            debug("EXIT checkRelativeConstraint()")
            return 0
        elif Warn_or_Fail == 0:
            warning("Warning in checkRelativeConstraint")  # todo make more specific
        elif Warn_or_Fail == 1:
            error("Error in checkRelativeConstraint")  # todo make more specific
            stack_depth -= 1
            debug("EXIT checkRelativeConstraint()")
            return 1
        else:
            error(''.ljust(stack_depth * 2,
                           '.') + "what is happening in checkRelativeConstraint()")  # todo make more specific
            stack_depth -= 1
            debug("EXIT checkRelativeConstraint()")
            raise ValueError
            return 1
        stack_depth -= 1
        debug("EXIT checkRelativeConstraint()")
        return 1

    def checkConstraintById(self, constraint_id):
        global stack_depth
        global print_logs
        debug("ENTER checkConstraintById()")
        stack_depth += 1
        error_ind = 0

        debug("Satan is real")
        debug(self)
        debug("constraint_set_id:" + str(self.constraint_set_id))
        debug("constraint_id:" + str(constraint_id))
        debug('type(constraint_id)'+str(type(constraint_id)))
        debug("XXX*")
        current_constraint = self.constraint_id_to_args_dict_map[constraint_id]

        current_args = current_constraint["args"]

        constraint_type = current_constraint["constraint_type"]
        constraint_name = current_args["constraint_name"]

        # logging.debug("current_constraint:"+str(current_constraint))
        # logging.debug("current_args:"+str(current_args))

        input__expected_result = current_args['expected_result']
        input__constraint_type = current_args['constraint_type']
        input__fun = current_args['fun']
        input__dimension_index_list = current_args['dimension_index_list']
        input__element = current_args['element']
        input__measure_index = current_args['measure_index']
        input__lower_bound = current_args['lower_bound']
        input__upper_bound = current_args['upper_bound']
        input__warn_or_fail = current_args['warn_or_fail']

        debug("..input__fun:" + str(input__fun))

        if 'file row count' in constraint_type:
            input__fun = 'count'
            input__dimension_index_list = None
            input__element = None
            input__measure_index = None
        elif 'column' in constraint_type:
            input__dimension_index_list = None
            input__element = None
        elif 'dimension cross product element measure' in constraint_type:
            pass
        elif 'dimension cross product element' in constraint_type:
            input__fun = 'count'
        elif 'dimension cross product' in constraint_type:
            input__fun = 'count'
            input__dimension_index_list = None
        else:
            pass

        if 'absolute' in current_constraint["constraint_type"].lower():
            debug("Checking absolute constraint")
            test_result = self.checkAbsoluteConstraint(input__expected_result,
                                                       input__constraint_type,
                                                       input__fun,
                                                       input__dimension_index_list,
                                                       input__element,
                                                       input__measure_index,
                                                       input__lower_bound,
                                                       input__upper_bound,
                                                       input__warn_or_fail)

        elif 'relative' in current_constraint["constraint_type"].lower():
            debug("Checking relative constraint")
            test_result = self.checkRelativeConstraint(input__expected_result,
                                                       input__constraint_type,
                                                       input__fun,
                                                       input__dimension_index_list,
                                                       input__element,
                                                       input__measure_index,
                                                       input__lower_bound,
                                                       input__upper_bound,
                                                       input__warn_or_fail)

        elif current_constraint["constraint_type"] == "Bounded Overlap":
            pass  # todo

        elif current_constraint["constraint_type"] == "Column Data Type":
            # test_result = self.checkColumnDataTypeConstraint(current_args["column_name"],current_args["data_type"], current_args['warn_or_fail'])
            pass
        elif current_constraint["constraint_type"] == "Layout":
            # test_result = self.checkDataLayoutConstraint(current_args["data_type_list"], current_args['warn_or_fail'])
            pass
        elif current_constraint["constraint_type"] == "Column Name":
            # test_result = self.checkColumnNameConstraint(current_args["column_index"], current_args["goal_column_name"], current_args['warn_or_fail'])
            pass
        elif current_constraint["constraint_type"] == "Header":
            # test_result = self.checkHeaderConstraint(current_args["header_list"], current_args['warn_or_fail'])
            pass
        else:
            stack_depth -= 1
            debug("EXIT checkConstraintById()")
            error("Constraint Type not recognized. The value was:\'" + str(
                current_constraint["constraint_type"]) + '\'')
            raise ValueError

        # logging.debug("test_result:"+str(test_result))
        # if test_result == 1 and current_args['warn_or_fail'] == 1:
        #	error("FAIL")
        # elif test_result == 1 and current_args['warn_or_fail'] == 0:
        #	logging.debug("WARN")
        # elif test_result == 0:
        #	logging.debug("PASS")
        # else:
        #	error("what the fuck is happening")

        stack_depth -= 1
        debug("EXIT checkConstraintById()")

        if error_ind:
            return -1

        # logging.debug("exit checkConstraintById")
        try:
            return test_result
        except:
            return -1

    def checkConstraintByName(self, constraint_name):
        debug("enter checkConstraintByName()")
        constraint_id = self.constraint_name_map[constraint_name]
        debug("exit checkConstraintByName()")
        return self.checkConstraintById(constraint_id)

        # def checkHeaderConstraint(self, column_names, warn_or_fail):
        #	running_result = 0
        #	for i in range(0,len(self.df.columns)):
        #		try:
        #			running_result += int(self.df.columns[i] == column_names[i])
        #		except:
        #			running_result += 1

        #	if running_result == 0:
        #		return 0
        #	elif warn_or_fail == 0:
        #		logging.warning("warning in checkHeaderConstraint()")
        #		return 0
        #	else:
        #		return 1

        # def checkLayoutConstraint(self, data_type_list, warn_or_fail):
        # logging.debug("enter checkDataLayoutConstraint()")
        running_result = 0

    #	for i in range(0,len(self.df.columns)):
    #		cname = self.df.columns[i]
    #		running_result += self.checkColumnDataTypeConstraint(cname, data_type_list[i], 1)
    #		logging.debug("running_result:"+str(running_result))

    #	if running_result == 0:
    #		return 0
    #	elif warn_or_fail == 0:
    #		logging.warning("Warning in checkDataLayoutConstraint()")
    #		return 0
    #	else:
    #		return 1

    # def checkRelativeColumnCardinalityConstraint(self, column_name,lower_bound,upper_bound,warn_or_fail):

    #	column_index = self.df.columns.get_loc(column_name)

    #	try:
    #		if column_name + ' Column Cardinality Ratio' not in self.memoized_values.keys():
    #			self.memoized_values[column_name + ' Column Cardinality Ratio'] = len(self.df.iloc[:, column_index].unique())

    #		if lower_bound <= self.memoized_values[column_name + ' Column Cardinality Ratio'] and self.memoized_values[
    #			column_name + ' Column Cardinality Ratio'] <= upper_bound:
    #			return 0
    #		elif warn_or_fail == 0:
    #			logging.warning("Warning in checkRelativeColumnCardinalityConstraint")
    #		elif warn_or_fail == 1:
    #			error("Error in checkRelativeColumnCardinalityConstraint")
    #			return 1
    #		else:
    #			error(
    #				"what is happening in checkRelativeColumnCardinalityConstraint()")
    #			raise ValueError
    #			return 1

    #	except Exception as e:
    #		error("uncaught exception in checkRelativeColumnCardinalityConstraint()")
    #		traceback.print_tb()
    #		return -1
    #	return 1

    def checkRelativeFileConstraint(self, lower_bound, upper_bound, warn_or_fail):
        global stack_depth
        global print_logs
        debug("ENTER checkRelativeFileConstraint()")
        stack_depth += 1
        try:
            if 'Row Count Ratio' not in self.memoized_values.keys():
                self.memoized_values['Row Count Ratio'] = self.df.shape[0] / self.relative_df.shape[0]

            if lower_bound <= self.memoized_values['Row Count Ratio'] and self.memoized_values[
                'Row Count Ratio'] <= upper_bound:
                stack_depth -= 1
                debug("EXIT checkRelativeFileConstraint()")
                return 0
            elif warn_or_fail == 0:
                warning(''.ljust(stack_depth * 2,
                                 '.') + "Warning in checkRelativeFileConstraint")  # todo make more specific
            elif warn_or_fail == 1:
                error(
                    "Error in checkRelativeFileConstraint")  # todo make more specific
                stack_depth -= 1
                debug("EXIT checkRelativeFileConstraint()")
                return 1
            else:
                error(''.ljust(stack_depth * 2,
                               '.') + "what is happening in checkRelativeFileConstraint()")  # todo make more specific
                stack_depth -= 1
                debug("EXIT checkRelativeFileConstraint()")
                raise ValueError
                return 1

        except Exception as e:
            error("uncaught exception in checkAbsoluteFileConstraint()")
            traceback.print_tb()
            debug("EXIT checkRelativeFileConstraint()")
            stack_depth -= 1
            return -1
        stack_depth -= 1
        debug("EXIT checkRelativeFileConstraint()")
        return 1

    def getNewConstraintId(self):
        return len(self.constraint_id_to_args_dict_map.keys()) + 1

    # def checkMutuallyExclusiveConstraint(self, column_name, warn_or_fail):

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

    # def addMutuallyExclusiveConstraint(self,constraint_name, column_index,warn_or_fail):
    #	column_name = self.df.columns[column_index]

    #	new_constraint_id = self.getNewConstraintId()
    #	args_dict = {"constraint_id": new_constraint_id,
    #				 "constraint_name": constraint_name,
    #				 "column_name": column_name,
    #				 "warn_or_fail": warn_or_fail}
    #	self.addConstraintToConstraintMap(new_constraint_id, "Mutually Exclusive", args_dict)
    #	self.mutually_exclusive_constraints.loc[len(self.mutually_exclusive_constraints.index)] = [new_constraint_id,constraint_name,column_name,warn_or_fail]

    def showResults(self):
        # print("ID".ljust(5) + "Constraint Name".ljust(52)+"Result")
        for constraint_id in self.constraint_id_to_args_dict_map.keys():
            # print(str(constraint_id).ljust(5)+str(self.constraint_id_to_args_dict_map[constraint_id]['args']['constraint_name']).ljust(50,'.')+": "+str(self.test_results[constraint_id]) )
            pass

    def writeResultsToCSV(self, path):
        result_df = pd.DataFrame(columns=["constraint_id", 'constraint_name', 'result'])
        for constraint_id in self.constraint_id_to_args_dict_map.keys():
            result_df.loc[len(result_df.index)] = [constraint_id,
                                                   self.constraint_id_to_args_dict_map[constraint_id]['args'][
                                                       'constraint_name'], self.test_results[constraint_id]]

        with open(path, 'w') as f:
            writer = csv.writer(f)

            writer.writerow(['constraint_id', 'constraint_name', 'result'])
            for index, row in result_df.iterrows():
                writer.writerow([row['constraint_id'], row['constraint_name'], row['result']])


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True)
