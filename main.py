import pandas as pd
from ConstraintSet import ConstraintSet

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(message)s')


if __name__ == '__main__':

    constraint_set_name = 'Test 1'
    constraint_set_id = 1

    warn_or_fail = 0

    df_test1 = pd.DataFrame(columns=["unique_id",'dim1', 'dim2',"val1","val2"])
    df_test1.loc[len(df_test1.index)] = [0,'dim1_val1', 'dim_2_val1', 0, 0]
    df_test1.loc[len(df_test1.index)] = [1,'dim1_val1', 'dim_2_val1', 0, 1]
    df_test1.loc[len(df_test1.index)] = [2,'dim1_val1', 'dim_2_val1', 1, 0]
    df_test1.loc[len(df_test1.index)] = [3,'dim1_val1', 'dim_2_val1', 1, 1]

    df_test1.loc[len(df_test1.index)] = [4,'dim1_val2', 'dim_2_val1', 0, 0]
    df_test1.loc[len(df_test1.index)] = [5,'dim1_val2', 'dim_2_val1', 0, 1]
    df_test1.loc[len(df_test1.index)] = [6,'dim1_val2', 'dim_2_val1', 1, 0]
    df_test1.loc[len(df_test1.index)] = [7,'dim1_val2', 'dim_2_val1', 1, 1]

    df_test1.loc[len(df_test1.index)] = [8,'dim1_val1', 'dim_2_val2', 0, 0]
    df_test1.loc[len(df_test1.index)] = [9,'dim1_val1', 'dim_2_val2', 0, 1]
    df_test1.loc[len(df_test1.index)] = [10,'dim1_val1', 'dim_2_val2', 1, 0]
    df_test1.loc[len(df_test1.index)] = [11,'dim1_val1', 'dim_2_val2', 1, 1]

    df_test1.loc[len(df_test1.index)] = [12,'dim1_val2', 'dim_2_val2', 0, 0]
    df_test1.loc[len(df_test1.index)] = [13,'dim1_val2', 'dim_2_val2', 0, 1]
    df_test1.loc[len(df_test1.index)] = [14,'dim1_val2', 'dim_2_val2', 1, 0]
    df_test1.loc[len(df_test1.index)] = [15,'dim1_val2', 'dim_2_val2', 1, 1]

    df_test2 = pd.DataFrame(columns=["unique_id", 'dim1', 'dim2', "val1", "val2"])
    df_test2.loc[len(df_test2.index)] = [16, 'dim1_val1', 'dim_2_val1', 0, 0]
    df_test2.loc[len(df_test2.index)] = [17, 'dim1_val1', 'dim_2_val1', 0, 1]
    df_test2.loc[len(df_test2.index)] = [18, 'dim1_val1', 'dim_2_val1', 1, 0]
    df_test2.loc[len(df_test2.index)] = [19, 'dim1_val1', 'dim_2_val1', 1, 1]
    df_test2.loc[len(df_test2.index)] = [20, 'dim1_val2', 'dim_2_val1', 0, 0]
    df_test2.loc[len(df_test2.index)] = [21, 'dim1_val2', 'dim_2_val1', 0, 1]
    df_test2.loc[len(df_test2.index)] = [22, 'dim1_val2', 'dim_2_val1', 1, 0]
    df_test2.loc[len(df_test2.index)] = [23, 'dim1_val2', 'dim_2_val1', 1, 1]

    df_test2.loc[len(df_test2.index)] = [24, 'dim1_val1', 'dim_2_val2', 0, 0]
    df_test2.loc[len(df_test2.index)] = [25, 'dim1_val1', 'dim_2_val2', 0, 1]
    df_test2.loc[len(df_test2.index)] = [26, 'dim1_val1', 'dim_2_val2', 1, 0]
    df_test2.loc[len(df_test2.index)] = [27, 'dim1_val1', 'dim_2_val2', 1, 1]

    df_test2.loc[len(df_test2.index)] = [28, 'dim1_val2', 'dim_2_val2', 0, 0]
    df_test2.loc[len(df_test2.index)] = [29, 'dim1_val2', 'dim_2_val2', 0, 1]
    df_test2.loc[len(df_test2.index)] = [30, 'dim1_val2', 'dim_2_val2', 1, 0]
    df_test2.loc[len(df_test2.index)] = [31, 'dim1_val2', 'dim_2_val2', 1, 1]

    cs1 = ConstraintSet(constraint_set_name, constraint_set_id, df_test1, df_test2)

    cs1.addAbsoluteFileConstraint('1 < row count < 100', 1, 100, warn_or_fail)
    cs1.addRelativeFileConstraint('Matching Row Count', 1, 1, warn_or_fail)
    #cs1.addAbsoluteDimensionCrossProductConstraint('1 < DCM Cardinality < 100', [0,1], 1, 100, warn_or_fail)
    #cs1.addRelativeDimensionCrossProductConstraint( 'Relative DCM Cardinality = 1', [0,1], 1, 1, warn_or_fail)


    #cs1.addAbsoluteMeasureConstraint( "Absolute Measure Constraint 1", column_index, measure_calculation_function, warn_or_fail)
    #cs1.addRelativeMeasureConstraint( constraint_name, column_index, measure_calculation_function, warn_or_fail)
    #cs1.addAbsoluteDimensionCrossProductElementConstraint( constraint_name, dimension_column_index_list,measure_calculation_function,warn_or_fail)
    #cs1.addRelativeDimensionCrossProductElementConstraint( constraint_name, dimension_column_index_list,measure_calculation_function,warn_or_fail)
    cs1.addAbsoluteColumnCardinalityConstraint( "Check Absolute Column Cardinality",0, 0, 100, warn_or_fail)
    cs1.addRelativeColumnCardinalityConstraint("Check Relative Column Cardinality",0, 0, 100, warn_or_fail)
    cs1.addAbsoluteColumnNullCountConstraint("Check Absolute Column Null Count",0, 0, 0, warn_or_fail)
    cs1.addRelativeColumnNullCountConstraint("Check Relative Column Null Count",0, 1, float('inf'), warn_or_fail)

    cs1.addMutuallyExclusiveConstraint("Mutually Exclusive Unique Ids", 0, warn_or_fail)
    cs1.addColumnDataTypeConstraint( "unique_id Data Type Constraint", 0, 'int64', warn_or_fail)
    cs1.addDataLayoutConstraint( "Data Layout Constraint 1", ['int64','int64',None,None,'int64'], 0)
    cs1.addDataLayoutConstraint("Data Layout Constraint 2", ['int64', 'object', 'object', 'int64', 'int64'], warn_or_fail)
    cs1.addDataLayoutConstraint("Data Layout Constraint 3", ['int64', 'int64', 'str', 'str', 'int64'], 0)
    cs1.addColumnNameConstraint( "Column Name Constraint", 0, "unique_id", warn_or_fail)
    cs1.addHeaderConstraint( "Header Constraint", ["unique_id",'dim1', 'dim2',"val1","val2"], warn_or_fail)

    #print(cs1)

    cs1.checkAllConstraints(outputFolder='C:/sandbox/data/output/Data_Profile_Tester/')
