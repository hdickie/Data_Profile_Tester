import logging
import pandas as pd
import traceback
import csv
import datetime
import statistics

logger = logging.getLogger()
logger.setLevel(logging.INFO)
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

    print_logs = False

    debug("ENTER create_test_constraint_sets_map_from_xlsx()")
    stack_depth += 1

    # Constraint_Set_Name	Constraint_Set_Id	Expected Result	Primary Data Set Name	Secondary Data Set Name
    constraint_set_def_df = pd.read_excel(xlsx_path, sheet_name="Constraint Set Definitions")

    # Constraint_Set_Name	Constraint_Set_Id	Constraint_Name	constraint_type	Dimension_Index_List	Element	lower_bound	upper_bound	Warn_or_Fail
    constraint_def_df = pd.read_excel(xlsx_path, sheet_name="Constraint Definitions")
    debug("constraint_def_df:\n"+constraint_def_df.to_string())

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

        debug("Constraint_Set_Name:"+Constraint_Set_Name)
        debug("Constraint_Set_Id:" + str(Constraint_Set_Id))
        debug("Primary_Data_Set_Name:" + Primary_Data_Set_Name)
        debug("Secondary_Data_Set_Name:" + Secondary_Data_Set_Name)

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

        debug('Constraint_Set_Id:'+str(Constraint_Set_Id))
        debug("Constraint_Set_Name:"+str(Constraint_Set_Name))
        debug("constraint_def_df:\n"+str(constraint_def_df.to_string()))

        try:
            debug(constraint_set_id_to_Constraint_Set_Object_map)
            debug(Constraint_Set_Id)
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
        return running_str

    def __init__(self, constraint_set_name, constraint_set_id, df, relative_df):
        # todo input parameter validation
        logging.info("Initializing new ConstraintSet: "+constraint_set_name+" : #"+str(constraint_set_id))

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
        self.expected_result_to_constraint_list_map = {}

    def addConstraint(self, Constraint_Name, Expected_Result, constraint_type, Dimension_Index_List, Element,
                      Measure_Index, lower_bound, upper_bound, Warn_or_Fail):
        global stack_depth
        global print_logs
        debug("ENTER addConstraint()")
        stack_depth += 1

        #todo add check that element and dimension cross product have same rank

        try:
            error_msg = ""
            error_flag = False
            if constraint_type.lower().strip() == 'absolute file row count' \
                    or constraint_type.lower().strip() == 'relative file row count':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:"+str(Dimension_Index_List)+'\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                if not pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected N/A for Measure_Index. Got:" + str(Measure_Index) + '\n'

                #Parameters that should be defined
                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"


            elif constraint_type.lower().strip() == 'absolute column cardinality' \
                    or constraint_type.lower().strip() == 'absolute column null count' \
                    or constraint_type.lower().strip() == 'relative column cardinality' \
                    or constraint_type.lower().strip() == 'relative column null count':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:" + str(Dimension_Index_List) + '\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                # Parameters that should be defined
                if not pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected N/A for Measure_Index. Got:" + str(Measure_Index) + '\n'

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"

            elif constraint_type.lower().strip() == 'absolute dimension cross product cardinality' \
                    or constraint_type.lower().strip() == 'relative dimension cross product cardinality':
                #Parameters that should not be defined
                if not pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected N/A for Measure_Index. Got:" + str(Measure_Index) + '\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                # Parameters that should be defined
                if pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected not N/A for Dimension_Index_List, but got N/A.\n"

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"

            elif constraint_type.lower().strip() == 'absolute dimension cross product element measure cardinality' \
                    or constraint_type.lower().strip() == 'absolute dimension cross product element measure null count' \
                    or constraint_type.lower().strip() == 'absolute dimension cross product element measure min' \
                    or constraint_type.lower().strip() == 'absolute dimension cross product element measure mean' \
                    or constraint_type.lower().strip() == 'absolute dimension cross product element measure median' \
                    or constraint_type.lower().strip() == 'absolute dimension cross product element measure mode' \
                    or constraint_type.lower().strip() == 'absolute dimension cross product element measure max' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure cardinality' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure null count' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure min' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure mean' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure median' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure mode' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element measure max':

                # Parameters that should be defined
                if pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected not N/A for Dimension_Index_List, but got N/A.\n"

                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"

                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"
                else:
                    try:
                        int(Measure_Index)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Measure_Index to int. Measure_Index:" + str(Measure_Index) + "\n"

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"

            elif constraint_type.lower().strip() == 'absolute layout' \
                    or constraint_type.lower().strip() == 'absolute header':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:"+str(Dimension_Index_List)+"\n"

                if not pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected N/A for Measure_Index. Got:"+str(Measure_Index)+"\n"

                if not pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Lower_Bound. Got:" + str(lower_bound) + '\n'
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Upper_Bound. Got:" + str(upper_bound) + '\n'
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                # Parameters that should be defined
                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"


            elif constraint_type.lower().strip() == 'absolute column name' \
                    or constraint_type.lower().strip() == 'absolute data type':
                # Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:" + str(Dimension_Index_List) + '\n'

                if not pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Lower_Bound. Got:" + str(lower_bound) + '\n'
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for upper_bound. Got:" + str(upper_bound) + '\n'
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                #Parameters that should be defined
                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"

                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"
                else:
                    try:
                        int(Measure_Index)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Measure_Index to int. Measure_Index:" + str(Measure_Index) + "\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"

            elif constraint_type.lower().strip() == 'relative layout' \
                    or constraint_type.lower().strip() == 'relative header':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:"+str(Dimension_Index_List)+'\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                if not pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected N/A for Measure_Index. Got:" + str(Measure_Index) + '\n'

                if not pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Lower_Bound. Got:" + str(lower_bound) + '\n'
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Upper_Bound. Got:" + str(upper_bound) + '\n'
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                # Parameters that should be defined
                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"

            elif constraint_type.lower().strip() == 'relative column data type' \
                    or constraint_type.lower().strip() == 'relative column name':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:"+str(Dimension_Index_List)+'\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                if not pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Lower_Bound. Got:" + str(lower_bound) + '\n'
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Upper_Bound. Got:" + str(upper_bound) + '\n'
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                # Parameters that shoudl be defined
                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"
                else:
                    try:
                        int(Measure_Index)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Measure_Index to int. Measure_Index:" + str(Measure_Index) + "\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"

            elif constraint_type.lower().strip() == 'bounded overlap':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:"+str(Dimension_Index_List)+'\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                # Parameters that should be defined
                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"
                else:
                    try:
                        int(Measure_Index)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Measure_Index to int. Measure_Index:" + str(Measure_Index) + "\n"

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"
                else:
                    try:
                        int(lower_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast lower_bound to int. Lower_Bound:"+str(lower_bound)+"\n"

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"
                else:
                    try:
                        int(upper_bound)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast upper_bound to int. upper_bound:"+str(upper_bound)+"\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"
                else:
                    try:
                        bool(Warn_or_Fail)
                    except:
                        error_flag = True
                        error_msg += "Failed to cast Warn_or_Fail to bool. Warn_or_Fail:"+str(Warn_or_Fail)+"\n"
            else:
                error_msg = "Unknown constraint type"

            if error_flag:
                raise ValueError
        except Exception as e:
            error(error_msg)
            error(e)
            stack_depth -= 1
            return


        # try:
        #     # generic input validation
        #     if not pd.isna(Measure_Index):
        #         try:
        #             int(Measure_Index)
        #         except Exception as e:
        #             error("Measure Index was not an integer:"+str(Measure_Index))
        #             raise e
        #
        #         try:
        #             self.df.iloc[:, Measure_Index]
        #         except IndexError as e:
        #             error("Measure_Index caused subscript out of bounds error")
        #             raise e
        #
        #     if Warn_or_Fail != 0 and Warn_or_Fail != 1:
        #         error("Warn_or_Fail was not 0 or 1")
        #         raise ValueError
        #
        # except Exception as e:
        #     error("Exception caught during input parameter validation in addConstraint()")
        #     error(str(constraint_type)+" "+str(Constraint_Name)+" will not be added to Constraint Set # "+str(self.constraint_set_id))
        #     error(e)
        #     stack_depth -= 1
        #     return


        new_constraint_id = self.getNewConstraintId()
        constraint_type_lower = constraint_type.lower()

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
             constraint_type_lower == "absolute layout" or \
             constraint_type_lower == "absolute header" or \
                constraint_type_lower == "relative header" or \
                constraint_type_lower == "relative layout" or \
                constraint_type_lower == "absolute column data type" or \
                constraint_type_lower == "relative column data type" or \
                constraint_type_lower == "relative column name" or \
                constraint_type_lower == "absolute column name":
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

        if constraint_type in self.constraint_type_to_constraint_list_map.keys():
            self.constraint_type_to_constraint_list_map[constraint_type] += [constraint_def_dict]
        else:
            self.constraint_type_to_constraint_list_map[constraint_type] = [constraint_def_dict]

        if Expected_Result in self.expected_result_to_constraint_list_map.keys():
            self.expected_result_to_constraint_list_map[Expected_Result] += [constraint_def_dict]
        else:
            self.expected_result_to_constraint_list_map[Expected_Result] = [constraint_def_dict]

        stack_depth -= 1
        debug("EXIT addConstraint()")

    def checkAllConstraints(self, printResults=True, outputFolder=None):
        global stack_depth
        global print_logs
        debug("ENTER checkAllConstraints()")
        stack_depth += 1
        for constraint_id in self.constraint_id_to_args_dict_map.keys():
            debug(str(constraint_id))
            self.test_results[constraint_id] = self.checkConstraintById(constraint_id)

        if printResults:
            self.showResults()

        if outputFolder is not None:
            self.writeResultsToCSV(outputFolder + '//Data_Profile_Test_Results_' + datetime.datetime.now().strftime(
                "%Y%m%d_%H%M%S") + '.txt')
        stack_depth -= 1
        debug("EXIT checkAllConstraints")

    def checkAbsoluteConstraint(self, Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
                                Measure_Index, lower_bound, upper_bound, Warn_or_Fail):
        global stack_depth
        global print_logs
        debug("ENTER checkAbsoluteConstraint()")
        stack_depth += 1
        debug('Expected_Result'.ljust(17)+'Constraint_Type'.ljust(52)+ \
              'Dimension_Index_List'.ljust(22)+ 'Element'.ljust(10)+ 'Measure_Index'.ljust(15)+ \
              'Lower_Bound'.ljust(13)+ 'Upper_Bound'.ljust(13)+ 'Warn_or_Fail'.ljust(13))
        debug(str(Expected_Result).ljust(17) + str(constraint_type).ljust(52) + \
               str(Dimension_Index_List).ljust(22) + str(Element).ljust(10)+ str(Measure_Index).ljust(15) + \
              str(lower_bound).ljust(13) + str(upper_bound).ljust(13) + str(Warn_or_Fail).ljust(13))

        # debug('Expected_Result:' + str(Expected_Result))
        # debug('constraint_type:' + str(constraint_type))
        # debug('Fun:' + str(Fun))
        # debug('Dimension_Index_List:' + str(Dimension_Index_List))
        # debug('Element:' + str(Element))
        # debug('Measure_Index:' + str(Measure_Index))
        # debug('lower_bound:' + str(lower_bound))
        # debug('upper_bound:' + str(upper_bound))
        # debug('Warn_or_Fail:' + str(Warn_or_Fail))

        #debug(self)


        memo_key = 'Primary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
            Fun)

        debug("memo_key:" + str(memo_key))
        debug("")
        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")

            debug("Dimension_Index_List is NaN:" + str(pd.isna(Dimension_Index_List)))
            if not pd.isna(Dimension_Index_List):
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [self.df.columns[int(column_index)]]

            debug("Element is NaN.............:"+str(pd.isna(Element)))
            if not pd.isna(Element):
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            debug("Measure_Index is NaN.......:"+str(pd.isna(Measure_Index)))
            if not pd.isna(Measure_Index):
                debug('Measure_Index..............:' + str(Measure_Index))
                measure_column_name = self.df.columns[Measure_Index]
                debug('Measure Column Name........:' + str(measure_column_name))

            try:
                # is na ?
                # Dimension_Index_List      Element     Measure_Index       Case Names
                # Yes                       Yes         Yes                 Absolute\Relative File Row Count
                # Yes                       Yes         No                  Bounded Overlap, Relative Column Data Type, Relative Column Name, Absolute\Relative Column F(x)
                # Yes                       No          Yes                 --not valid. cant define an element if DCP is not defined.
                # Yes                       No          No                  Absolute Column Name, Absolute Column Data Type
                # No                        No          No                  Absolute\Relative Dimension Cross Product Element Measure F(x)
                # No                        No          Yes                 Absolute\Relative Dimension Cross Product Element Row Count
                # No                        Yes         No                  Absolute Header, Absolute Layout
                # No                        Yes         Yes                 Absolute\Relative Dimension Cross Product Cardinality

                #this is an invalid case in the if branch below, but I want to assert that it isnt happening here so that we still et 100% code coverage during testing
                #pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(Measure_Index):  # not valid. #todo assert that this is not happening
                #debug('Dimension_Index_List:' + str(pd.isna(Dimension_Index_List))) #todo add these to logs if a debug flag is True
                #debug('Element:' + str(pd.isna(Element)))
                #debug('Measure_Index:' + str(pd.isna(Measure_Index)))
                #debug('Fun:' + str(Fun))

                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(Measure_Index):  #Absolute\Relative File Row Count Case
                    debug("Parameter Case 000")
                    assert Fun == 'count'

                    result_value = self.df.shape[0]

                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(Measure_Index):  # Bounded Overlap, Relative Column Data Type, Relative Column Name, Absolute\Relative Column F(x)
                    debug("Parameter Case 001")
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
                    elif Fun == 'relative column data type':
                        result_value = -1 #todo
                    elif Fun == 'relative column name':
                        result_value = -1 #todo
                    elif Fun == 'bounded overlap':
                        result_value = -1 #todo
                    else:
                        pass #todo attempt literal interpretation?
                elif pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(Measure_Index):  # Absolute Column Name, Absolute Column Data Type
                    debug("Parameter Case 011")
                    if Fun == 'absolute column name':
                        result_value = -1  # todo
                    elif Fun == 'absolute column data type':
                        result_value = -1  # todo
                    else:
                        error("Parameter combination matched Absolute Column Name/Data Type case, but Fun did not match the expected value for that case.")
                        #todo put this as an assertion
                        raise ValueError
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(Measure_Index):  # Absolute\Relative Dimension Cross Product Element Measure F(x)
                    debug("Parameter Case 111")
                    if Fun == 'cardinality':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    elif Fun == 'null count':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    elif Fun == 'min':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    elif Fun == 'mean':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    elif Fun == 'median':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    elif Fun == 'mode':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    elif Fun == 'max':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                    else:
                        pass  # todo attempt literal interpretation?
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(Measure_Index):  # Absolute\Relative Dimension Cross Product Element Row Count
                    debug("Parameter Case 110")
                    result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg['max'].reset_index() #todo this is wrong
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(Measure_Index):  # Absolute Header, Absolute Layout
                    debug("Parameter Case 101")
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
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(Measure_Index):  # Dimension Cross Product Cardinality Case
                    debug("Parameter Case 100")
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    assert Fun == 'cardinality'
                    result_value = self.df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]

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

                result_value = result_set_df[sel_vec, len(result_set_df.columns) - 1]
            else:
                self.memoized_values[memo_key] = result_value
        debug('SET result_value = ' + str(self.memoized_values[memo_key]))

        # value is memoized
        initial_result = lower_bound <= self.memoized_values[memo_key] and self.memoized_values[memo_key] <= upper_bound
        if Expected_Result == 'PASS':
            debug("CHECKING IS TRUE: "+str(lower_bound)+ " <= "+str(self.memoized_values[memo_key])+" <= "+str(upper_bound))
        elif Expected_Result == 'FAIL':
            debug("CHECKING IS FALSE: " + str(lower_bound) + " <= " + str(self.memoized_values[memo_key]) + " <= " + str(upper_bound))


        if ( initial_result and Expected_Result == 'PASS') or ( not initial_result and Expected_Result == 'FAIL'):
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
        debug('Expected_Result'.ljust(17) + 'Constraint_Type'.ljust(52) + \
              'Dimension_Index_List'.ljust(22) + 'Element'.ljust(10) + 'Measure_Index'.ljust(15) + \
              'Lower_Bound'.ljust(13) + 'Upper_Bound'.ljust(13) + 'Warn_or_Fail'.ljust(13))
        debug(str(Expected_Result).ljust(17) + str(constraint_type).ljust(52) + \
              str(Dimension_Index_List).ljust(22) + str(Element).ljust(10) + str(Measure_Index).ljust(15) + \
              str(lower_bound).ljust(13) + str(upper_bound).ljust(13) + str(Warn_or_Fail).ljust(13))

        memo_key = 'Primary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
            Fun)
        primary_memo_key = memo_key

        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")

            debug("Dimension_Index_List is NaN:" + str(pd.isna(Dimension_Index_List)))
            if not pd.isna(Dimension_Index_List):
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [self.df.columns[int(column_index)]]

            debug("Element is NaN.............:" + str(pd.isna(Element)))
            if not pd.isna(Element):
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            debug("Measure_Index is NaN.......:" + str(pd.isna(Measure_Index)))
            if not pd.isna(Measure_Index):
                debug('Measure_Index..............:' + str(Measure_Index))
                measure_column_name = self.df.columns[Measure_Index]
                debug('Measure Column Name........:' + str(measure_column_name))

            try:
                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute\Relative File Row Count Case
                    assert Fun == 'count'
                    debug("Computing result for File Row Count Case")
                    result_value = self.df.shape[0]

                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Bounded Overlap, Relative Column Data Type, Relative Column Name, Absolute\Relative Column F(x)
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
                    elif Fun == 'relative column data type':
                        result_value = -1  # todo
                    elif Fun == 'relative column name':
                        result_value = -1  # todo
                    elif Fun == 'bounded overlap':
                        result_value = -1  # todo
                    else:
                        pass  # todo attempt literal interpretation?
                elif pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute Column Name, Absolute Column Data Type
                    if Fun == 'absolute column name':
                        result_value = -1  # todo
                    elif Fun == 'absolute column data type':
                        result_value = -1  # todo
                    else:
                        error(
                            "Parameter combination matched Absolute Column Name/Data Type case, but Fun did not match the expected value for that case.")
                        # todo put this as an assertion
                        raise ValueError
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute\Relative Dimension Cross Product Element Measure F(x)
                    if Fun == 'cardinality':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'null count':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'min':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'mean':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'median':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'mode':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'max':
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    else:
                        pass  # todo attempt literal interpretation?
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute\Relative Dimension Cross Product Element Row Count
                    result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                        'max'].reset_index()  # todo this is wrong
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute Header, Absolute Layout
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
                        result_set_df = self.df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index):  # Dimension Cross Product Cardinality Case
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    result_value = self.df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]

                    assert Fun == 'cardinality'
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

        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")

            debug("Dimension_Index_List is NaN:" + str(pd.isna(Dimension_Index_List)))
            if not pd.isna(Dimension_Index_List):
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [self.df.columns[int(column_index)]]

            debug("Element is NaN.............:" + str(pd.isna(Element)))
            if not pd.isna(Element):
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            debug("Measure_Index is NaN.......:" + str(pd.isna(Measure_Index)))
            if not pd.isna(Measure_Index):
                debug('Measure_Index..............:' + str(Measure_Index))
                measure_column_name = self.df.columns[Measure_Index]
                debug('Measure Column Name........:' + str(measure_column_name))

            try:
                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute\Relative File Row Count Case
                    assert Fun == 'count'
                    debug("Computing result for File Row Count Case")
                    result_value = self.relative_df.shape[0]

                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Bounded Overlap, Relative Column Data Type, Relative Column Name, Absolute\Relative Column F(x)
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
                    elif Fun == 'relative column data type':
                        result_value = -1  # todo
                    elif Fun == 'relative column name':
                        result_value = -1  # todo
                    elif Fun == 'bounded overlap':
                        result_value = -1  # todo
                    else:
                        pass  # todo attempt literal interpretation?
                elif pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute Column Name, Absolute Column Data Type
                    if Fun == 'absolute column name':
                        result_value = -1  # todo
                    elif Fun == 'absolute column data type':
                        result_value = -1  # todo
                    else:
                        error(
                            "Parameter combination matched Absolute Column Name/Data Type case, but Fun did not match the expected value for that case.")
                        # todo put this as an assertion
                        raise ValueError
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute\Relative Dimension Cross Product Element Measure F(x)
                    if Fun == 'cardinality':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'null count':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'min':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'mean':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'median':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'mode':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    elif Fun == 'max':
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo this is wrong
                    else:
                        pass  # todo attempt literal interpretation?
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute\Relative Dimension Cross Product Element Row Count
                    result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                        'max'].reset_index()  # todo this is wrong
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute Header, Absolute Layout
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
                        result_set_df = self.relative_df.groupby(dimension_column_names_list)[measure_column_name].agg[
                            'max'].reset_index()  # todo
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index):  # Dimension Cross Product Cardinality Case
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    result_value = self.relative_df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]

                    assert Fun == 'cardinality'
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
        debug("RESULT: " + str(initial_result))
        if Expected_Result == 'PASS':
            debug("CHECKING IS TRUE: " + str(lower_bound) + " <= " + str(initial_result_value) + " <= " + str(upper_bound))
        elif Expected_Result == 'FAIL':
            debug("CHECKING IS FALSE: " + str(lower_bound) + " <= " + str(initial_result_value) + " <= " + str(upper_bound))

        if ( initial_result and Expected_Result == 'PASS') or ( not initial_result and Expected_Result == 'FAIL'):
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

        #debug(self)
        current_constraint = self.constraint_id_to_args_dict_map[constraint_id]

        current_args = current_constraint["args"]

        constraint_type = current_constraint["constraint_type"]
        constraint_name = current_args["constraint_name"]

        input__expected_result = current_args['expected_result']
        input__constraint_type = current_args['constraint_type']
        input__fun = current_args['fun']
        input__dimension_index_list = current_args['dimension_index_list']
        input__element = current_args['element']
        input__measure_index = current_args['measure_index']
        input__lower_bound = current_args['lower_bound']
        input__upper_bound = current_args['upper_bound']
        input__warn_or_fail = current_args['warn_or_fail']

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
