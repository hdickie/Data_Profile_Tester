import logging
import pandas as pd
import csv
import datetime
import statistics
import scipy.stats

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s ' + 'ConstraintSet' + ' %(levelname)s| %(message)s')

global stack_depth
stack_depth = 0

global print_logs
print_logs = True

#todo add self.testResults[constraint_id] to the check methods instead of just CheakAll

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
#
#
# def critical(obj):
#     if print_logs:
#         logging.critical(''.ljust(stack_depth * 2, '.') + str(obj))


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
    #debug("constraint_def_df:\n"+constraint_def_df.to_string())

    # Data Set Name	Description	Data Set Path
    data_set_def_df = pd.read_excel(xlsx_path, sheet_name="Data Set Definitions")

    data_set_name_to_df_map = {}
    for index, row in data_set_def_df.iterrows():
        Data_Set_Name = row['Data Set Name']
        Description = row['Description']
        Data_Set_Path = row['Data Set Path']

        data_set_name_to_df_map[Data_Set_Name] = pd.read_csv(Data_Set_Path)

    constraint_set_id_to_Constraint_Set_Object_map = {}
    for index, row in constraint_set_def_df.iterrows():
        debug('row:'+str(row))
        Constraint_Set_Name = row['Constraint Set Name']
        Constraint_Set_Id = row['Constraint Set Id']
        Primary_Data_Set_Name = row['Primary Data Set Name']
        Secondary_Data_Set_Name = row['Secondary Data Set Name']

        assert Primary_Data_Set_Name in data_set_name_to_df_map.keys() #if error here, failed to load primary data set

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

    for index, row in constraint_def_df.iterrows():
        Constraint_Set_Name = row['Constraint Set Name']
        Constraint_Set_Id = row['Constraint Set Id']
        Constraint_Name = row['Constraint Name']
        Expected_Result = row['Expected Result']
        constraint_type = row['Constraint Type']
        Dimension_Index_List = row['Dimension Index List']
        Element = row['Element']
        Measure_Index = row['Measure Index']
        lower_bound = row['Lower Bound']
        upper_bound = row['Upper Bound']
        Warn_or_Fail = row['Warn or Fail']

        #debug('Constraint_Set_Id:'+str(Constraint_Set_Id))
        #debug("Constraint_Set_Name:"+str(Constraint_Set_Name))
        #debug("constraint_def_df:\n"+str(constraint_def_df.to_string()))

        assert not pd.isna(Constraint_Set_Id)

        constraint_set_id_to_Constraint_Set_Object_map[Constraint_Set_Id].addConstraint(Constraint_Name,
                                                                                        Expected_Result,
                                                                                        constraint_type,
                                                                                        Dimension_Index_List,
                                                                                        Element,
                                                                                        Measure_Index,
                                                                                        lower_bound,
                                                                                        upper_bound,
                                                                                        Warn_or_Fail)

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

        assert isinstance(self.df,pd.DataFrame)
        running_str += str(self.df.head(1)) + "\n"


        running_str += "\nSecondary Data Frame Details:\n"
        assert isinstance(self.relative_df,pd.DataFrame)
        running_str += str(self.relative_df.head(1)) + "\n"
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
        debug("Constraint_Type:" + str(constraint_type))
        debug("Constraint_Name:"+str(Constraint_Name))

        #todo add check that element and dimension cross product have same rank

        #todo dimension index list and measure cannot overlap

        #todo assert than numerical columns are specified on numerical measure columns

        #todo bounded overlap params must be between 0 and 1

        try:
            error_msg = "\n"
            error_flag = False

            #Generic Validation
            if str(upper_bound).lower() == 'inf':
                lower_bound = float('inf')

            if str(upper_bound).lower() == 'inf':
                upper_bound = float('inf')

            if not pd.isna(lower_bound):
                try:
                    assert float(lower_bound) >= 0
                except:
                    error_flag = True
                    error_msg += "Lower Bound must be non-negative\n"

            if not pd.isna(upper_bound):
                try:
                    assert float(upper_bound) >= 0
                except:
                    error_flag = True
                    error_msg += "Upper Bound must be non-negative\n"

            if not pd.isna(lower_bound) and not pd.isna(upper_bound):
                try:
                    assert float(upper_bound) >= float(lower_bound)
                except:
                    error_flag = True
                    error_msg += "Lower Bound must be less than upper bound\n"

            if not pd.isna(Measure_Index):
                try:
                    Measure_Index = int(Measure_Index)
                    try:
                        assert Measure_Index <= (self.df.shape[1] - 1)
                        assert Measure_Index >= 0
                    except:
                        error_flag = True
                        error_msg += "Measure Index out of bounds\n"
                except:
                    error_flag = True
                    error_msg += "Measure Index is not an integer. Got:"+str(Measure_Index)+'\n'

            if not pd.isna(Dimension_Index_List):
                try:
                    assert len(Dimension_Index_List.split(',')) >= 2
                    for dimension_index in Dimension_Index_List.split(','):
                        try:
                            int(dimension_index)
                            try:
                                debug("checking dimension_index <= self.df.shape[1] - 1")
                                debug(str(dimension_index)+" <= "+str(self.df.shape[1] - 1))
                                assert 0 <= int(dimension_index) <= self.df.shape[1] - 1
                            except Exception as e:
                                error_flag = True
                                error_msg += "A dimension index was out of bounds. Offending value:"+str(dimension_index)+"\n"
                        except:
                            error_flag = True
                            error_msg += "A dimension index failed cast to int. Offending value:" + str(dimension_index) + '\n'
                except:
                    error_flag = True
                    error_msg += "Dimension_Index_List has fewer than 2 items. Got:"+str(Measure_Index)+'\n'

            try:
                assert Warn_or_Fail == 0 or Warn_or_Fail == 1
            except:
                error_flag = True
                error_msg += "Warn_or_Fail was not 0 or 1. Got:"+str(Warn_or_Fail)+"\n"

            #Test Type Validation
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

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"



            elif constraint_type.lower().strip() == 'absolute column cardinality' \
                    or constraint_type.lower().strip() == 'absolute column null count' \
                    or constraint_type.lower().strip() == 'relative column cardinality' \
                    or constraint_type.lower().strip() == 'relative column null count' \
                    or constraint_type.lower().strip() == 'relative column min' \
                    or constraint_type.lower().strip() == 'relative column median' \
                    or constraint_type.lower().strip() == 'relative column mean' \
                    or constraint_type.lower().strip() == 'relative column mode' \
                    or constraint_type.lower().strip() == 'relative column max' \
                    or constraint_type.lower().strip() == 'absolute column min' \
                    or constraint_type.lower().strip() == 'absolute column median' \
                    or constraint_type.lower().strip() == 'absolute column mean' \
                    or constraint_type.lower().strip() == 'absolute column mode' \
                    or constraint_type.lower().strip() == 'absolute column max':
                #Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:" + str(Dimension_Index_List) + '\n'

                if not pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected N/A for Element. Got:" + str(Element) + '\n'

                # Parameters that should be defined
                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"


                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"


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

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"

            elif constraint_type.lower().strip() == 'absolute dimension cross product element row count' \
                    or constraint_type.lower().strip() == 'relative dimension cross product element row count':
                #Parameters that should not be defined
                if not pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected N/A for Measure_Index. Got:" + str(Measure_Index) + '\n'

                # Parameters that should be defined
                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"

                if pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected not N/A for Dimension_Index_List, but got N/A.\n"

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"

                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"

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

                try:
                    float(min(self.df.iloc[:, Measure_Index]))
                except:
                    error_flag = True
                    error_msg += "Measure column in primary df failed cast to float.\n"

                if 'relative' in constraint_type.lower():
                    try:
                        float(min(self.relative_df.iloc[:,Measure_Index]))
                    except:
                        error_flag = True
                        error_msg += "Measure column in relative df failed cast to float.\n"


                # Parameters that should be defined
                if pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected not N/A for Dimension_Index_List, but got N/A.\n"

                try:
                    assert len(Dimension_Index_List.split(',')) == len(Element.split(','))
                except:
                    error_flag = True
                    error_msg += "Dimension_Index_List and Element have different rank, which is an invalid test configuration.\n"

                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"



                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"

                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"


                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"


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


                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Upper_Bound. Got:" + str(upper_bound) + '\n'


                # Parameters that should be defined
                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"

                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"



            elif constraint_type.lower().strip() == 'absolute column name' \
                    or constraint_type.lower().strip() == 'absolute column data type':
                # Parameters that should not be defined
                if not pd.isna(Dimension_Index_List):
                    error_flag = True
                    error_msg += "Expected N/A for Dimension_Index_List. Got:" + str(Dimension_Index_List) + '\n'

                if not pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Lower_Bound. Got:" + str(lower_bound) + '\n'


                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for upper_bound. Got:" + str(upper_bound) + '\n'


                #Parameters that should be defined
                if pd.isna(Element):
                    error_flag = True
                    error_msg += "Expected not N/A for Element, but got N/A.\n"
                elif constraint_type.lower().strip() == 'absolute column data type':
                    try:
                        assert Element in ['bool','int','float','complex','object']
                    except:
                        error_flag = True
                        error_msg += "Input data type was not one of: bool, int, float, complex, object. Offending value:"+str(Element)+"\n"
                    # assert maps to one of these data types
                    # Data type	Description
                    # bool_	Boolean (True or False) stored as a byte
                    # int_	Default integer type (same as C long; normally either int64 or int32)
                    # intc	Identical to C int (normally int32 or int64)
                    # intp	Integer used for indexing (same as C ssize_t; normally either int32 or int64)
                    # int8	Byte (-128 to 127)
                    # int16	Integer (-32768 to 32767)
                    # int32	Integer (-2147483648 to 2147483647)
                    # int64	Integer (-9223372036854775808 to 9223372036854775807)
                    # uint8	Unsigned integer (0 to 255)
                    # uint16	Unsigned integer (0 to 65535)
                    # uint32	Unsigned integer (0 to 4294967295)
                    # uint64	Unsigned integer (0 to 18446744073709551615)
                    # float_	Shorthand for float64.
                    # float16	Half precision float: sign bit, 5 bits exponent, 10 bits mantissa
                    # float32	Single precision float: sign bit, 8 bits exponent, 23 bits mantissa
                    # float64	Double precision float: sign bit, 11 bits exponent, 52 bits mantissa
                    # complex_	Shorthand for complex128.
                    # complex64	Complex number, represented by two 32-bit floats
                    # complex128	Complex number, represented by two 64-bit floats

                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"


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


                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Upper_Bound. Got:" + str(upper_bound) + '\n'


                # Parameters that should be defined
                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"


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

                if not pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected N/A for Upper_Bound. Got:" + str(upper_bound) + '\n'


                # Parameters that shoudl be defined
                if pd.isna(Measure_Index):
                    error_flag = True
                    error_msg += "Expected not N/A for Measure_Index, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for Warn_or_Fail, but got N/A.\n"


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


                if pd.isna(lower_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"


                if pd.isna(upper_bound):
                    error_flag = True
                    error_msg += "Expected not N/A for upper_bound, but got N/A.\n"


                if pd.isna(Warn_or_Fail):
                    error_flag = True
                    error_msg += "Expected not N/A for lower_bound, but got N/A.\n"

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
                or constraint_type_lower == "relative dimension cross product element measure min" \
                or constraint_type_lower == "absolute column min" \
                or constraint_type_lower == "relative column min":
            Fun = 'min'
        elif constraint_type_lower == "absolute dimension cross product element measure max" \
                or constraint_type_lower == "relative dimension cross product element measure max" \
                or constraint_type_lower == "absolute column max" \
                or constraint_type_lower == "relative column max":
            Fun = 'max'
        elif constraint_type_lower == "absolute dimension cross product element measure mean" \
                or constraint_type_lower == "relative dimension cross product element measure mean" \
                or constraint_type_lower == "absolute column mean" \
                or constraint_type_lower == "relative column mean":
            Fun = 'mean'
        elif constraint_type_lower == "absolute dimension cross product element measure median" \
                or constraint_type_lower == "relative dimension cross product element measure median" \
                or constraint_type_lower == "absolute column median" \
                or constraint_type_lower == "relative column median":
            Fun = 'median'
        elif constraint_type_lower == "absolute dimension cross product element measure mode" \
                or constraint_type_lower == "relative dimension cross product element measure mode" \
                or constraint_type_lower == "absolute column mode" \
                or constraint_type_lower == "relative column mode":
            Fun = 'mode'
        elif constraint_type_lower == "absolute dimension cross product element measure sum" \
                or constraint_type_lower == "relative dimension cross product element measure sum" \
                or constraint_type_lower == "absolute column sum" \
                or constraint_type_lower == "relative column sum":
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
        # else:
        #     debug('unknown constraint type:' + constraint_type_lower)
        #     debug("We will proceed interpreting constraint type as a python data frame column aggregation function")
        #     Fun = constraint_type #todo implement this?


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

    def checkAllConstraints(self):
        global stack_depth
        global print_logs
        debug("ENTER checkAllConstraints()")
        stack_depth += 1
        for constraint_id in self.constraint_id_to_args_dict_map.keys():
            self.test_results[constraint_id] = self.checkConstraintById(constraint_id)

        stack_depth -= 1
        debug("EXIT checkAllConstraints")

    def calculateDataProfileStatistic(self,df,memo_key,Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
                                Measure_Index, lower_bound, upper_bound, Warn_or_Fail):
        debug("enter calculateDataProfileStatistic()")
        global stack_depth
        global print_logs

        stack_depth += 1

        dimension_column_names_list = []
        if memo_key not in self.memoized_values.keys():
            debug(memo_key + " not in memoized values")

            debug("Dimension_Index_List is NaN:" + str(pd.isna(Dimension_Index_List)))
            if not pd.isna(Dimension_Index_List):
                for column_index in str(Dimension_Index_List).split(','):
                    dimension_column_names_list += [df.columns[int(column_index)]]

            debug("Element is NaN.............:" + str(pd.isna(Element)))
            if not pd.isna(Element):
                element_values_list = []
                for element_value in Element.split(','):
                    element_values_list += [element_value]

            debug("Measure_Index is NaN.......:" + str(pd.isna(Measure_Index)))
            if not pd.isna(Measure_Index):
                debug('Measure_Index..............:' + str(Measure_Index))
                measure_column_name = df.columns[Measure_Index]
                debug('Measure Column Name........:' + str(measure_column_name))

            try:
                # is not na ?
                # Parameter Case Dimension_Index_List      Element     Measure_Index       Case Names
                # 000            No                        No          No                  Absolute\Relative File Row Count, Relative Header, Relative Layout
                # 001            No                        No          Yes                 Bounded Overlap, Relative Column Data Type, Relative Column Name, Absolute\Relative Column F(x)
                # 010            No                        Yes         No                  Absolute Header, Absolute Layout
                # 011            No                        Yes         Yes                 Absolute Column Name, Absolute Column Data Type
                # 100            Yes                       No          No                  Absolute\Relative Dimension Cross Product Cardinality
                # 101            Yes                       No          Yes
                # 110            Yes                       Yes         No                  Absolute\Relative Dimension Cross Product Element Row Count
                # 111            Yes                       Yes         Yes                 Absolute\Relative Dimension Cross Product Element Measure F(x)

                if pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute\Relative File Row Count Case, Relative Header, Relative Layout
                    debug("Parameter Case 000")
                    if  Fun == 'count':
                        result_value = df.shape[0]
                    elif Fun == 'relative layout':
                        result_value = list(df.dtypes.index)
                    elif Fun == 'relative header':
                        result_value = list(df.columns)
                    else:
                        result_value = -1

                elif pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Bounded Overlap, Relative Column Data Type, Relative Column Name, Absolute\Relative Column F(x)
                    debug("Parameter Case 001")

                    if Fun == 'cardinality':
                        result_value = df.iloc[:, Measure_Index].nunique()
                    elif Fun == 'null count':
                        result_value = sum(df.iloc[:, Measure_Index].isnull())

                    #strings can legitimately be passed to these below functions, but this is not valid input for this project
                    elif Fun == 'min':
                        result_value = min(df.loc[ pd.isna(df.iloc[:,Measure_Index]) == False, df.columns[Measure_Index]])
                        try:
                            float(result_value)
                        except:
                            result_value = -1
                    elif Fun == 'mean':
                        result_value = statistics.mean(df.loc[ pd.isna(df.iloc[:,Measure_Index]) == False, df.columns[Measure_Index]])
                        try:
                            float(result_value)
                        except:
                            result_value = -1
                    elif Fun == 'median':
                        result_value = statistics.median(df.loc[ pd.isna(df.iloc[:,Measure_Index]) == False, df.columns[Measure_Index]])
                        try:
                            float(result_value)
                        except:
                            result_value = -1
                    elif Fun == 'mode':
                        result_value = statistics.mode(df.loc[ pd.isna(df.iloc[:,Measure_Index]) == False, df.columns[Measure_Index]])
                        try:
                            float(result_value)
                        except:
                            result_value = -1
                    elif Fun == 'max':
                        result_value = max((df.loc[ pd.isna(df.iloc[:,Measure_Index]) == False, df.columns[Measure_Index]]))
                        try:
                            float(result_value)
                        except:
                            result_value = -1
                    elif Fun == 'sum':
                        result_value = sum((df.loc[ pd.isna(df.iloc[:,Measure_Index]) == False, df.columns[Measure_Index]]))
                        try:
                            float(result_value)
                        except:
                            result_value = -1
                    elif Fun == 'relative column data type':
                        result_value = self.df.dtypes.index[Measure_Index] == self.relative_df.dtypes.index[Measure_Index]
                    elif Fun == 'relative column name':
                        result_value = self.df.columns[Measure_Index] == self.relative_df.columns[Measure_Index]
                    else:
                        pass  # todo attempt literal interpretation?
                elif pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute Header, Absolute Layout
                    debug("Parameter Case 010")

                    result_value = True
                    result_value = len(Element.split(',')) == df.shape[1]

                    if result_value and Fun == 'absolute header':  # if the result_value is True part of this condition wasnt here
                        for i in range(0, len(Element.split(','))):  # , then the layout having too many columns would throw an exception
                            debug("Element.split(',')[i]:"+str(Element.split(',')[i]))
                            debug("df.columns[i]:"+str(df.columns[i]))
                            if Element.split(',')[i] != df.columns[i]:
                                result_value = False
                    elif result_value and Fun == 'absolute layout':
                        parent_data_type = {}
                        parent_data_type['int64'] = 'int'
                        parent_data_type['object'] = 'object'
                        parent_data_type['float64'] = 'float'
                        # todo add more data types to map

                        for i in range(0, len(Element.split(','))):
                            debug("Element.split(',')["+str(i)+"]:" + str(Element.split(',')[i]) )
                            debug("parent_data_type[str(df.dtypes["+str(i)+"])]:"+str(parent_data_type[str(df.dtypes[i])]))

                            if Element.split(',')[i] != parent_data_type[str(df.dtypes[i])]:
                                result_value = False

                elif pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute Column Name, Absolute Column Data Type
                    debug("Parameter Case 011")
                    if Fun == 'absolute column name':
                        result_value = df.columns[Measure_Index] == Element
                    elif Fun == 'absolute column data type':

                        # Data type	Description
                        # bool_	Boolean (True or False) stored as a byte
                        # int_	Default integer type (same as C long; normally either int64 or int32)
                        # intc	Identical to C int (normally int32 or int64)
                        # intp	Integer used for indexing (same as C ssize_t; normally either int32 or int64)
                        # int8	Byte (-128 to 127)
                        # int8	Byte (-128 to 127)
                        # int16	Integer (-32768 to 32767)
                        # int32	Integer (-2147483648 to 2147483647)
                        # int64	Integer (-9223372036854775808 to 9223372036854775807)
                        # uint8	Unsigned integer (0 to 255)
                        # uint16	Unsigned integer (0 to 65535)
                        # uint32	Unsigned integer (0 to 4294967295)
                        # uint64	Unsigned integer (0 to 18446744073709551615)
                        # float_	Shorthand for float64.
                        # float16	Half precision float: sign bit, 5 bits exponent, 10 bits mantissa
                        # float32	Single precision float: sign bit, 8 bits exponent, 23 bits mantissa
                        # float64	Double precision float: sign bit, 11 bits exponent, 52 bits mantissa
                        # complex_	Shorthand for complex128.
                        # complex64	Complex number, represented by two 32-bit floats
                        # complex128	Complex number, represented by two 64-bit floats

                        # Also
                        # str

                        debug("Evaluating absolute column data type")
                        debug(str(df.dtypes[Measure_Index]) + " ?= " + str(Element))

                        if Element == 'bool':
                            result_value = df.dtypes[Measure_Index] in ['bool']
                        elif Element == 'int':
                            result_value = df.dtypes[Measure_Index] in ['int', 'int32', 'int64', 'intc', 'intp',
                                                                             'int8',
                                                                             'int16', 'int32', 'int64', 'uint8',
                                                                             'uint16',
                                                                             'uint16', 'uint32', 'uint64']
                        elif Element == 'float':
                            result_value = df.dtypes[Measure_Index] in ['float', 'float16', 'float32', 'float64']
                        elif Element == 'complex':
                            result_value = df.dtypes[Measure_Index] in ['complex', 'complex64', 'complex128']
                        elif Element == 'str':
                            result_value = df.dtypes[Measure_Index] in ['str']
                        elif Element == 'object':
                            result_value = df.dtypes[Measure_Index] in ['object']

                    else:
                        error(
                            "Parameter combination matched Absolute Column Name/Data Type case, but Fun did not match the expected value for that case.")
                        # todo put this as an assertion
                        raise ValueError
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and pd.isna(
                        Measure_Index):  # Dimension Cross Product Cardinality Case
                    debug("Parameter Case 100")
                    debug("Computing result for Absolute Dimension Cross Product Cardinality Case")
                    assert Fun == 'cardinality'
                    result_value = df.loc[:, dimension_column_names_list].drop_duplicates().shape[0]
                elif not pd.isna(Dimension_Index_List) and pd.isna(Element) and not pd.isna(
                        Measure_Index):
                    pass
                    #This should never occur
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and pd.isna(
                        Measure_Index):  # Absolute\Relative Dimension Cross Product Element Row Count
                    debug("Parameter Case 110")

                    result_subset = df.loc[:,dimension_column_names_list]
                    for i in range(0,len(element_values_list)):
                        if isinstance(result_subset,pd.DataFrame):
                            row_sel_vec = (result_subset.loc[:,dimension_column_names_list[i]] == element_values_list[i]).values
                            result_subset = result_subset.loc[ row_sel_vec , ]

                    result_value = result_subset.shape[0]
                elif not pd.isna(Dimension_Index_List) and not pd.isna(Element) and not pd.isna(
                        Measure_Index):  # Absolute\Relative Dimension Cross Product Element Measure F(x)
                    debug("Parameter Case 111")
                    debug("Fun:" + str(Fun))

                    if Fun == 'cardinality':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list,aggfunc=lambda x: x.nunique())
                    elif Fun == 'null count':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list,aggfunc=lambda x: sum(x.isnull()))
                    elif Fun == 'min':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list, aggfunc='min')
                    elif Fun == 'sum':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list, aggfunc='sum')
                    elif Fun == 'mean':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list, aggfunc='mean')
                    elif Fun == 'median':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list, aggfunc='median')
                    elif Fun == 'mode':
                        #result_set_df = pd.pivot_table(df, columns=dimension_column_names_list,aggfunc='min')
                        result_value = result_set_df = df.groupby(dimension_column_names_list)[measure_column_name].agg(lambda x: scipy.stats.mode(x))[0].mode[0]
                        debug("mode result:"+str(result_value))
                    elif Fun == 'max':
                        result_set_df = pd.pivot_table(df, columns=dimension_column_names_list, aggfunc='max')
                    else:
                        pass  # todo attempt literal interpretation?
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
            if len(dimension_column_names_list) > 0 and not pd.isna(Element) and Fun != 'mode' \
                    and constraint_type != 'absolute dimension cross product element row count' \
                    and constraint_type != 'relative dimension cross product element row count':
                debug('df.columns:'+str(df.columns))
                debug('result_set_df:\n'+str(result_set_df))

                try:
                    intermediate_result_pd_series = result_set_df[tuple(element_values_list)]
                    debug('intermediate_result_pd_series:\n' + str(intermediate_result_pd_series))
                    debug('intermediate_result_pd_series.index:' + str(intermediate_result_pd_series.index))

                    # this might need to be branched depending on the function
                    sel_vec_for_pivot_table = intermediate_result_pd_series.index == df.columns[Measure_Index]

                    debug('sel_vec_for_pivot_table:' + str(sel_vec_for_pivot_table))
                    result_value = intermediate_result_pd_series[sel_vec_for_pivot_table][0]
                except KeyError:
                    result_value = 0 #if this is executed, its because the element was not found in the dimension cross product


                # result_value = result_set_df[tuple(element_values_list)][result_set_df.index == df.columns[Measure_Index]]
                debug('result_value:' + str(result_value))
                # debug('type(result_value):'+str(type(result_value)))
                self.memoized_values[memo_key] = result_value
            else:
                debug('memo_key:'+str(memo_key))
                self.memoized_values[memo_key] = result_value
        else:
            debug('result_value found in memoized values')

        stack_depth -= 1
        debug("exit calculateDataProfileStatistic()")
        return self.memoized_values[memo_key]

    def checkAbsoluteConstraint(self,constraint_id, Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
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
        result_value = self.calculateDataProfileStatistic(self.df,memo_key,Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
                                Measure_Index, lower_bound, upper_bound, Warn_or_Fail)

        debug('SET result_value = ' + str(self.memoized_values[memo_key]))

        # value is memoized
        debug("constraint_type:"+str(constraint_type))
        if constraint_type not in ['absolute column data type','absolute header','relative column data type','relative header', \
                                   'absolute layout', 'relative layout','absolute column name','relative column name']:
            initial_result = lower_bound <= self.memoized_values[memo_key] and self.memoized_values[memo_key] <= upper_bound
            if Expected_Result == 'PASS':
                debug("CHECKING IS TRUE: "+str(lower_bound)+ " <= "+str(self.memoized_values[memo_key])+" <= "+str(upper_bound))
            elif Expected_Result == 'FAIL':
                debug("CHECKING IS FALSE: " + str(lower_bound) + " <= " + str(self.memoized_values[memo_key]) + " <= " + str(upper_bound))

        #Tests not defined by bounds
        else:
            initial_result = self.memoized_values[memo_key]
            if Expected_Result == 'PASS':
                debug("CHECKING IS TRUE: "+str(self.memoized_values[memo_key]))
            elif Expected_Result == 'FAIL':
                debug("CHECKING IS TRUE: "+str(self.memoized_values[memo_key]))

        debug('initial_result:'+str(initial_result))
        debug('Expected_Result == PASS:'+str(Expected_Result == 'PASS'))
        debug('Expected_Result == FAIL:' + str(Expected_Result == 'FAIL'))
        if ( initial_result and Expected_Result == 'PASS') or ( not initial_result and Expected_Result == 'FAIL'):
            return_value = 0
        elif Warn_or_Fail == 0:
            warning("Warning in checkAbsoluteConstraint")  # todo make more specific
            return_value = 1
        elif Warn_or_Fail == 1:
            error("Error in checkAbsoluteConstraint")  # todo make more specific
            return_value = 1
        else:
            error("Unmapped constraint results")
            error("initial_result:"+str(initial_result))
            error("Expected_Result:" + str(Expected_Result))
            error("Warn_or_Fail:" + str(Warn_or_Fail))
            stack_depth -= 1
            debug("EXIT checkAbsoluteConstraint()")
            return_value = 1
            raise ValueError
            #return 1
        stack_depth -= 1
        debug("EXIT checkAbsoluteConstraint()")
        self.test_results[constraint_id] = return_value
        return return_value

    def checkRelativeConstraint(self,Constraint_Id, Expected_Result, constraint_type, Fun, Dimension_Index_List, Element,
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

        if constraint_type == 'bounded overlap':
            memo_key = 'Bounded Overlap ' + str( Measure_Index)
            debug("Attempting to compute bounded overlap")
            debug("numerator:"+str(len(set(self.df.iloc[:, Measure_Index]) & set(self.relative_df.iloc[:, Measure_Index] ) ) ) )
            debug(str(set(self.df.iloc[:, Measure_Index]) & set(self.relative_df.iloc[:, Measure_Index])))
            debug("denominator:" + str(len(set(self.df.iloc[:,Measure_Index])) ))
            debug(str(set(self.df.iloc[:,Measure_Index])))
            initial_result_value = len(set(self.df.loc[ pd.isna(self.df.iloc[:,Measure_Index]) == False , : self.df.columns[Measure_Index] ]) & set(self.relative_df.loc[ pd.isna(self.relative_df.iloc[:,Measure_Index]) == False , : self.relative_df.columns[Measure_Index] ])) / len(set(self.df.loc[ pd.isna(self.df.iloc[:,Measure_Index]) == False , : self.df.columns[Measure_Index] ]))
            self.memoized_values[memo_key] = initial_result_value
            debug('SET result_value = ' + str(self.memoized_values[memo_key]))

            result_value = (float(lower_bound) <= initial_result_value and initial_result_value <= float(upper_bound))

        else:
            memo_key = 'Primary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
                Fun)
            primary_memo_key = memo_key

            result_value = self.calculateDataProfileStatistic(self.df,memo_key, Expected_Result, constraint_type, Fun,
                                                              Dimension_Index_List, Element,
                                                              Measure_Index, lower_bound, upper_bound, Warn_or_Fail)

            debug('SET primary_result_value = ' + str(self.memoized_values[memo_key]))

            memo_key = 'Secondary ' + str(Dimension_Index_List) + ' ' + str(Element) + ' ' + str(Measure_Index) + ' ' + str(
                Fun)
            secondary_memo_key = memo_key

            debug("memo_key:" + str(memo_key))
            result_value = self.calculateDataProfileStatistic(self.relative_df,memo_key, Expected_Result, constraint_type, Fun,
                                                              Dimension_Index_List, Element,
                                                              Measure_Index, lower_bound, upper_bound, Warn_or_Fail)
            debug('SET secondary_result_value = ' + str(self.memoized_values[memo_key]))

        debug("constraint_type:"+str(constraint_type))
        if constraint_type != 'relative layout' and constraint_type != 'relative header' and constraint_type != 'bounded overlap' \
                and constraint_type != 'relative column data type' and constraint_type != 'relative column name':
            try:
                initial_result_value = int(self.memoized_values[primary_memo_key]) / int(self.memoized_values[secondary_memo_key])
            except ZeroDivisionError:
                initial_result_value = float('inf')

            initial_result = (float(lower_bound) <= initial_result_value and initial_result_value <= float(upper_bound))
            debug("SET initial_result = " + str(initial_result))
            debug("Expected_Result:" + str(Expected_Result))
            if Expected_Result == 'PASS':
                debug("CHECKING IS TRUE: " + str(lower_bound) + " <= " + str(initial_result_value) + " <= " + str(
                    upper_bound))
            elif Expected_Result == 'FAIL':
                debug("CHECKING IS FALSE: " + str(lower_bound) + " <= " + str(initial_result_value) + " <= " + str(
                    upper_bound))
        elif constraint_type == 'bounded overlap':
            if Expected_Result == 'PASS':
                debug("CHECKING IS TRUE: " + str(lower_bound) + " <= " + str(initial_result_value) + " <= " + str(
                    upper_bound))
            elif Expected_Result == 'FAIL':
                debug("CHECKING IS FALSE: " + str(lower_bound) + " <= " + str(initial_result_value) + " <= " + str(upper_bound))
            initial_result = (float(lower_bound) <= initial_result_value and initial_result_value <= float(upper_bound))
            debug("SET initial_result = " + str(initial_result))
        else:
            initial_result_value = self.memoized_values[primary_memo_key] == self.memoized_values[secondary_memo_key]
            debug("Expected_Result:"+str(Expected_Result))
            if Expected_Result == 'PASS':
                debug("CHECKING IS TRUE: " + str(initial_result_value))
                initial_result = initial_result_value is True
            elif Expected_Result == 'FAIL':
                debug("CHECKING IS FALSE: " + str(initial_result_value))
                initial_result = initial_result_value is False
            debug("SET initial_result = "+str(initial_result))


        debug(str(self.constraint_set_id)+" "+str(Constraint_Id))
        if ( initial_result and Expected_Result == 'PASS') or ( not initial_result and Expected_Result == 'FAIL'):
            return_value = 0
        elif Warn_or_Fail == 0:
            warning("Warning in checkRelativeConstraint")  # todo make more specific
            return_value = 1
        elif Warn_or_Fail == 1:
            error("Error in checkRelativeConstraint")  # todo make more specific
            return_value = 1
        else:
            error("what is happening in checkRelativeConstraint()")  # todo make more specific
            stack_depth -= 1
            debug("EXIT checkRelativeConstraint()")
            raise ValueError
            return 1
        stack_depth -= 1
        debug("EXIT checkRelativeConstraint()")
        self.test_results[Constraint_Id] = return_value
        return return_value

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
        #
        # if 'file row count' in constraint_type:
        #     input__fun = 'count'
        #     input__dimension_index_list = None
        #     input__element = None
        #     input__measure_index = None
        # elif 'column' in constraint_type:
        #     input__dimension_index_list = None
        #     input__element = None
        # elif 'dimension cross product element measure' in constraint_type:
        #     pass
        # elif 'dimension cross product element' in constraint_type:
        #     input__fun = 'count'
        # elif 'dimension cross product' in constraint_type:
        #     input__fun = 'count'
        #     input__dimension_index_list = None
        # else:
        #     pass

        info("BEGIN TEST: "+str(self.constraint_id_to_args_dict_map[constraint_id]['args']['constraint_name']))
        if 'absolute' in current_constraint["constraint_type"].lower():
            debug("Checking absolute constraint")
            test_result = self.checkAbsoluteConstraint(constraint_id,
                                                       input__expected_result,
                                                       input__constraint_type,
                                                       input__fun,
                                                       input__dimension_index_list,
                                                       input__element,
                                                       input__measure_index,
                                                       input__lower_bound,
                                                       input__upper_bound,
                                                       input__warn_or_fail)

        elif 'relative' in current_constraint["constraint_type"].lower() \
                or 'bounded overlap' == current_constraint["constraint_type"].lower():
            debug("Checking relative constraint")
            test_result = self.checkRelativeConstraint(constraint_id,
                                                       input__expected_result,
                                                       input__constraint_type,
                                                       input__fun,
                                                       input__dimension_index_list,
                                                       input__element,
                                                       input__measure_index,
                                                       input__lower_bound,
                                                       input__upper_bound,
                                                       input__warn_or_fail)

        # elif current_constraint["constraint_type"] == "Column Data Type":
        #     # test_result = self.checkColumnDataTypeConstraint(current_args["column_name"],current_args["data_type"], current_args['warn_or_fail'])
        #     pass
        # elif current_constraint["constraint_type"] == "Layout":
        #     # test_result = self.checkDataLayoutConstraint(current_args["data_type_list"], current_args['warn_or_fail'])
        #     pass
        # elif current_constraint["constraint_type"] == "Column Name":
        #     # test_result = self.checkColumnNameConstraint(current_args["column_index"], current_args["goal_column_name"], current_args['warn_or_fail'])
        #     pass
        # elif current_constraint["constraint_type"] == "Header":
        #     # test_result = self.checkHeaderConstraint(current_args["header_list"], current_args['warn_or_fail'])
        #     pass
        else:
            stack_depth -= 1
            debug("EXIT checkConstraintById()")
            error("Constraint Type not recognized. The value was:\'" + str(
                current_constraint["constraint_type"]) + '\'')
            raise ValueError

        if test_result == -1:
            info("ERR  "+self.constraint_id_to_args_dict_map[constraint_id]['args']['constraint_name'])
        elif test_result == 0:
            info("PASS "+self.constraint_id_to_args_dict_map[constraint_id]['args']['constraint_name'])
        elif test_result == 1:
            info("FAIL "+self.constraint_id_to_args_dict_map[constraint_id]['args']['constraint_name'])

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

    def getNewConstraintId(self):
        return len(self.constraint_id_to_args_dict_map.keys()) + 1

    def showResults(self):
        # print("ID".ljust(5) + "Constraint Name".ljust(52)+"Result")
        for constraint_id in self.constraint_id_to_args_dict_map.keys():
            # print(str(constraint_id).ljust(5)+str(self.constraint_id_to_args_dict_map[constraint_id]['args']['constraint_name']).ljust(50,'.')+": "+str(self.test_results[constraint_id]) )
            pass

    def writeResultsToCSV(self, path):
        result_df = pd.DataFrame(columns=["constraint_id", 'constraint_name', 'result'])
        for constraint_id in self.test_results.keys():
            result_df.loc[len(result_df.index)] = [constraint_id,
                                                   self.constraint_id_to_args_dict_map[constraint_id]['args'][
                                                       'constraint_name'], self.test_results[constraint_id]]

        with open(path + '//Data_Profile_Test_Results_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + '.txt', 'w') as f:
            writer = csv.writer(f)

            writer.writerow(['constraint_id', 'constraint_name', 'result'])
            for index, row in result_df.iterrows():
                writer.writerow([row['constraint_id'], row['constraint_name'], row['result']])


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True)
