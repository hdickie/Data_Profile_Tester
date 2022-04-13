import unittest
from unittest import TestCase
from ConstraintSet import ConstraintSet, create_test_constraint_sets_map_from_xlsx
import pandas as pd
import logging

class TestConstraintSet(TestCase):

    @classmethod
    def setUpClass(self):
        #df_pts0 = pd.DataFrame(columns=["unique_id", 'dim1', 'dim2', "val1", "val2"])
        #df_pts1 = pd.DataFrame(columns=["unique_id", 'dim1', 'dim2', "val1", "val2"])
        #df_sts0 = pd.DataFrame(columns=["unique_id", 'dim1', 'dim2', "val1", "val2"])
        #df_sts1 = pd.DataFrame(columns=["unique_id", 'dim1', 'dim2', "val1", "val2"])
        #df_sts2 = pd.DataFrame()

        #df_pts1.loc[len(df_pts1.index)] = [0, 'dim1_val1', 'dim_2_val1', 0, 0]
        #df_pts1.loc[len(df_pts1.index)] = [1, 'dim1_val1', 'dim_2_val1', 0, 1]
        #df_pts1.loc[len(df_pts1.index)] = [2, 'dim1_val1', 'dim_2_val1', 1, 0]
        #df_pts1.loc[len(df_pts1.index)] = [3, 'dim1_val1', 'dim_2_val1', 1, 1]

        #df_pts1.loc[len(df_pts1.index)] = [4, 'dim1_val2', 'dim_2_val1', 0, 0]
        #df_pts1.loc[len(df_pts1.index)] = [5, 'dim1_val2', 'dim_2_val1', 0, 1]
        #df_pts1.loc[len(df_pts1.index)] = [6, 'dim1_val2', 'dim_2_val1', 1, 0]
        #df_pts1.loc[len(df_pts1.index)] = [7, 'dim1_val2', 'dim_2_val1', 1, 1]

        #df_pts1.loc[len(df_pts1.index)] = [8, 'dim1_val1', 'dim_2_val2', 0, 0]
        #df_pts1.loc[len(df_pts1.index)] = [9, 'dim1_val1', 'dim_2_val2', 0, 1]
        #df_pts1.loc[len(df_pts1.index)] = [10, 'dim1_val1', 'dim_2_val2', 1, 0]
        #df_pts1.loc[len(df_pts1.index)] = [11, 'dim1_val1', 'dim_2_val2', 1, 1]

        #df_pts1.loc[len(df_pts1.index)] = [12, 'dim1_val2', 'dim_2_val2', 0, 0]
        #df_pts1.loc[len(df_pts1.index)] = [13, 'dim1_val2', 'dim_2_val2', 0, 1]
        #df_pts1.loc[len(df_pts1.index)] = [14, 'dim1_val2', 'dim_2_val2', 1, 0]
        #df_pts1.loc[len(df_pts1.index)] = [15, 'dim1_val2', 'dim_2_val2', 1, 1]

        #df_sts1.loc[len(df_sts1.index)] = [16, 'dim1_val1', 'dim_2_val1', 0, 0]
        #df_sts1.loc[len(df_sts1.index)] = [17, 'dim1_val1', 'dim_2_val1', 0, 1]
        #df_sts1.loc[len(df_sts1.index)] = [18, 'dim1_val1', 'dim_2_val1', 1, 0]
        #df_sts1.loc[len(df_sts1.index)] = [19, 'dim1_val1', 'dim_2_val1', 1, 1]
        #df_sts1.loc[len(df_sts1.index)] = [20, 'dim1_val2', 'dim_2_val1', 0, 0]
        #df_sts1.loc[len(df_sts1.index)] = [21, 'dim1_val2', 'dim_2_val1', 0, 1]
        #df_sts1.loc[len(df_sts1.index)] = [22, 'dim1_val2', 'dim_2_val1', 1, 0]
        #df_sts1.loc[len(df_sts1.index)] = [23, 'dim1_val2', 'dim_2_val1', 1, 1]

        #df_sts1.loc[len(df_sts1.index)] = [24, 'dim1_val1', 'dim_2_val2', 0, 0]
        #df_sts1.loc[len(df_sts1.index)] = [25, 'dim1_val1', 'dim_2_val2', 0, 1]
        #df_sts1.loc[len(df_sts1.index)] = [26, 'dim1_val1', 'dim_2_val2', 1, 0]
        #df_sts1.loc[len(df_sts1.index)] = [27, 'dim1_val1', 'dim_2_val2', 1, 1]

        #df_sts1.loc[len(df_sts1.index)] = [28, 'dim1_val2', 'dim_2_val2', 0, 0]
        #df_sts1.loc[len(df_sts1.index)] = [29, 'dim1_val2', 'dim_2_val2', 0, 1]
        #df_sts1.loc[len(df_sts1.index)] = [30, 'dim1_val2', 'dim_2_val2', 1, 0]
        #df_sts1.loc[len(df_sts1.index)] = [31, 'dim1_val2', 'dim_2_val2', 1, 1]

        self.test_set_definitions = create_test_constraint_sets_map_from_xlsx(
            'C:/Users/HumeD/PycharmProjects/Data_Profile_Tester/test_constraints.xlsx')
        #print(self.test_set_definitions)
        print("#################################################")
        test_type_to_list_of_constraint_set_id_and_constraint_id_tuples = {}
        for constraint_set_id in self.test_set_definitions.keys():
            #print('self.test_set_definitions.keys()')
            print(self.test_set_definitions[constraint_set_id])
            print(self.test_set_definitions[constraint_set_id].constraints)
            for constraint_id in self.test_set_definitions[constraint_set_id].constraints.keys():
                #print(constraint_id)
                #print(self.test_set_definitions[constraint_set_id].constraints)
                #print(dir(self.test_set_definitions[constraint_set_id][constraint_id]))
                print(self.test_set_definitions[constraint_set_id].constraints[constraint_id])

                current_test_type = self.test_set_definitions[constraint_set_id].constraints[constraint_id]['constraint_type']

                if current_test_type not in test_type_to_list_of_constraint_set_id_and_constraint_id_tuples.keys():
                    test_type_to_list_of_constraint_set_id_and_constraint_id_tuples[current_test_type] = [(constraint_set_id,constraint_id)]
                else:
                    test_type_to_list_of_constraint_set_id_and_constraint_id_tuples[current_test_type] += [(constraint_set_id,constraint_id)]
        self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples = test_type_to_list_of_constraint_set_id_and_constraint_id_tuples
        print("#################################################")
        print(test_type_to_list_of_constraint_set_id_and_constraint_id_tuples)

        # cs1.addRelativeColumnCardinalityConstraint("Check Relative Column Cardinality", 0, 0, 100, warn_or_fail)
        # cs1.addRelativeColumnNullCountConstraint("Check Relative Column Null Count", 0, 1, float('inf'), warn_or_fail)

        #Test Scenarios #todo add constraints
        #      Primary              Secondary           Constraint Set  Expected Result   Purpose
        # 001. PTS0                 Null                cs1             Success           Expect all 0s : Test Lower Bounds (All Types)
        #cs1 = ConstraintSet('Absolute Lower Bounds: Expect Success', 1, df_pts0, None)
        #cs1.addAbsoluteFileConstraint('0 <= Row Count <= 0', 0, 0, 1)
        #cs1.addAbsoluteColumnCardinalityConstraint("0 <= Col 0 Card <= 0", 0, 0, 0, 1)
        #cs1.addAbsoluteColumnNullCountConstraint("0 <= Col 0 Null Count <= 0", 0, 0, 0, 1)
        #cs1.addAbsoluteDimensionCrossProductConstraint('0 <= DCM [0,1] Cardinality <= 0', [0, 1], 0, 0, 1)
        #todo add other constraint types
        #self.cs1 = cs1

        # 002. PTS1                 Null                CS2             Failure           Expect all 0s : Test Lower Bounds (All Types)
        #cs2 = ConstraintSet('Absolute Lower Bounds: Expect Fail', 2, df_pts1, None)
        #cs2.addAbsoluteFileConstraint('0 <= Row Count <= 0', 0, 0, 1)
        #cs2.addAbsoluteColumnCardinalityConstraint("0 <= Col 0 Card <= 0", 0, 0, 0, 1)
        #cs2.addAbsoluteColumnNullCountConstraint("0 <= Col 0 Null Count <= 0", 0, 0, 0, 1)
        #cs2.addAbsoluteDimensionCrossProductConstraint('0 <= DCM [0,1] Cardinality <= 0', [0, 1], 0, 0, 1)
        # todo add other constraint types
        #self.cs2 = cs2

        # 003. PTS1                 Null                CS2             Success           Expect all 1s : Test Upper Bounds (All Types)
        ##cs3 = ConstraintSet('Absolute Upper Bounds: Expect Success', 3, df_pts1, None)
        #cs3.addAbsoluteFileConstraint('0 <= Row Count <= +Inf', 0, float('inf'), 1)
        #cs3.addAbsoluteColumnCardinalityConstraint("0 <= Col 0 Card <= +Inf", 0, 0, float('inf'), 1)
        #cs3.addAbsoluteColumnNullCountConstraint("0 <= Col 0 Null Count <= +Inf", 0, 0, float('inf'), 1)
        #cs3.addAbsoluteDimensionCrossProductConstraint('0 <= DCM [0,1] Cardinality < +Inf', [0, 1], 0, float('inf'), 1)
        # todo add other constraint types
        #self.cs3 = cs3

        # 004. PTS0                 Null                CS2             Failure           Expect all 1s : Test Upper Bounds (All Types)
        #cs4 = ConstraintSet('Absolute Upper Bounds: Expect Failure', 4, df_pts0, None)
        #cs4.addAbsoluteFileConstraint('0 <= Row Count <= 0', 0, 0, 1)
        #cs4.addAbsoluteColumnCardinalityConstraint("0 <= Col 0 Card <= 0", 0, 0, 0, 1)
        #cs4.addAbsoluteColumnNullCountConstraint("0 <= Col 0 Null Count <= 0", 0, 0, 0, 1)
        #cs4.addAbsoluteDimensionCrossProductConstraint('0 <= DCM [0,1] Cardinality <= 0', [0, 1], 0, 0, 1)
        # todo add other constraint types
        #self.cs4 = cs4

        # 005. PTS1                 STS0                CS3             Success           Expect Relative Values : +Inf
        #cs5 = ConstraintSet('Ratio less than +Inf: Expect Success',5,df_pts1,df_sts0)
        # ...
        # todo add more
        #self.cs5 = cs5

        # 006. PTS1                 STS1                CS4             Success           Expect all 1s : Test Ratio Lower Bounds (All Types)
        #cs6 = ConstraintSet('Ratio Lower Bounds: Expect Success',6,df_pts1,df_sts1)
        # ...
        # todo add more
        #self.cs6 = cs6

        # 007. PTS1                 STS1                CS5             Failure           Expect all 1s : Test Ratio Lower Bounds (All Types)
        #cs7 = ConstraintSet('Ratio Lower Bounds: Expect Failure',7,df_pts1,df_sts1)
        # ...
        # todo add more
        #self.cs7 = cs7

        # 008. PTS1                 STS1                CS6             Success           Expect all 1s : Test Ratio Upper Bounds (All Types)
        #cs8 = ConstraintSet('Ratio Upper Bounds: Expect Success',8,df_pts1,df_sts1)
        # ...
        # todo add more
        #self.cs8 = cs8

        # 009. PTS1                 STS1                CS7             Failure           Expect all 1s : Test Ratio Upper Bounds (All Types)
        #cs9 = ConstraintSet('Ratio Upper Bounds: Expect Failure',9,df_pts1,df_sts1)
        # ...
        # todo add more
        #self.cs9 = cs9

        # 010. PTS1                 Null                CS8             Failure           Header has too few columns
        #cs10 = ConstraintSet('Header Too Few Columns',10,df_pts1,None)
        #cs10.addHeaderConstraint("Header Constraint", ["unique_id", 'dim1', 'dim2', "val1", "val2","additional_column"], 1)
        #self.cs10 = cs10

        # 011. PTS1                 Null                CS9             Failure           Header has too many columns
        #cs11 = ConstraintSet('Header Too Many Columns',11,df_pts1,None)
        #cs11.addHeaderConstraint("Header Constraint", ["unique_id", 'dim1', 'dim2', "val1"], 1)
        #self.cs11 = cs11

        # 012. PTS1                 Null                CS10            Failure           Header has incorrect values
        #cs12 = ConstraintSet('Header Incorrect Value',12,df_pts1,None)
        #cs12.addHeaderConstraint("Header Constraint", ["unique_id", 'dim1', 'dim2', "val1", "NOT THE RIGHT VALUE"], 1)
        #self.cs12 = cs12

        # 013. PTS1                 Null                CS11            Failure           Column has incorrect data type
        #cs13 = ConstraintSet('Incorrect Column Data Type',13,df_pts1,None)
        # ...
        # todo add more
        #self.cs13 = cs13

        # 014. PTS1                 STS2                CS12            Failure           Mutually exclusive constraint violated
        #cs14 = ConstraintSet('Mutually Exclusive Constraint: Expect Failure',14,df_pts1,df_sts2)
        # ...
        # todo add more
        #self.cs14 = cs14

        # 015. PTS1                 STS2                CS12            Failure           Mutually exclusive constraint satisfied
        #cs15 = ConstraintSet('Mutually Exclusive Constraint: Expect Failure',15, df_pts1, df_sts2)
        # ...
        # todo add more
        #self.cs15 = cs15

        # 016. PTS0                 Null                CS13            Error             Column Does not Exist
        #cs16 = ConstraintSet('Column Does Not Exist: Expect Error',16,df_pts0,None)
        # ...
        # todo add more
        #self.cs16 = cs16

        # 017. PST0                 Null                CS14            Error             Input boundary value is NaN
        #cs17 = ConstraintSet('Input Boundary Value is NaN: Expect Error',17,df_pts0,None)
        # ...
        # todo add more
        #self.cs17 = cs17

        # 018. PST0                 Null                CS15            Error             Warn or Fail indicator is not boolean
        #cs18 = ConstraintSet('Warn or Fail ind is not Bool',18,df_pts0,None)
        # ...
        # todo add more
        #self.cs18 = cs18

        # 019. PST0                 Null                CS3             Error             Relative test but no relative df defined
        #cs19 = ConstraintSet('Relative test define but not relative df',19,df_pts0,None)
        # ...
        # todo add more
        #self.cs19 = cs19

        # 012. PTS1                 Null                CS20            Success           Header has correct values
        #cs20 = ConstraintSet('Header Correct', 20, df_pts1, None)
        #cs20.addHeaderConstraint("Header Constraint", ["unique_id", 'dim1', 'dim2', "val1", "val2"], 0)
        #self.cs20 = cs20

        #These combinations of constraint sets and data sets are sufficient, but high code coverage is also necessary to make sure this code is solid

    def test_toString(self):
        pass

    def test_check_absolute_file_constraint(self):
        #self.assertEqual( self.cs1.checkConstraintByName('0 <= Row Count <= 0'), 0 )
        #self.assertEqual( self.cs2.checkConstraintByName('0 <= Col 0 Card <= 0'), 1 )
        #self.assertEqual( self.cs3.checkConstraintByName('0 <= Col 0 Null Count <= +Inf'), 0 )
        #self.assertEqual( self.cs4.checkConstraintByName('0 <= DCM [0,1] Cardinality <= 0'), 1 )

        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute File']
        for i in range(0,len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_constraint_set.constraints[current_tuple[1]]),1) #todo updaate this w actual expected result


    def test_check_absolute_column_cardinality_constraint(self):
        #self.assertEqual( self.cs2.checkConstraintByName("0 <= Col 0 Card <= 0"), 1)
        #self.assertEqual( self.cs3.checkConstraintByName("0 <= Col 0 Card <= +Inf"), 0 )
        #self.assertEqual( self.cs4.checkConstraintByName("0 <= Col 0 Card <= 0"), 0)

        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Cardinality']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_relative_column_cardinality_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Cardinality']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result


    def test_check_absolute_column_null_count_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Null Count']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result


    def test_check_relative_column_null_count_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Null Count']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_relative_file_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative File']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_absolute_dimension_cross_product_cardinality_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Cardinality']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_relative_dimension_cross_product_element_row_count_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Row Count']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_bounded_overlap_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Bounded Overlap']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_column_data_type_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Column Data Type']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_data_layout_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Layout']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_column_name_constraint(self):
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Column Name']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

    def test_check_header_constraint(self):
        #self.assertEqual(self.cs10.checkConstraintByName("Header Constraint"), 1)
        #self.assertEqual(self.cs11.checkConstraintByName("Header Constraint"), 1)
        #self.assertEqual(self.cs12.checkConstraintByName("Header Constraint"), 1)
        #self.assertEqual(self.cs20.checkConstraintByName("Header Constraint"), 0)
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Header']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),1)  # todo updaate this w actual expected result

if __name__ == "__main__":
    unittest.main()


# cs1.addRelativeFileConstraint('Matching Row Count', 1, 1, warn_or_fail)
# cs1.addAbsoluteDimensionCrossProductConstraint('1 < DCM Cardinality < 100', [0,1], 1, 100, warn_or_fail)
# cs1.addRelativeDimensionCrossProductConstraint( 'Relative DCM Cardinality = 1', [0,1], 1, 1, warn_or_fail)

# cs1.addAbsoluteMeasureConstraint( "Absolute Measure Constraint 1", column_index, measure_calculation_function, warn_or_fail)
# cs1.addRelativeMeasureConstraint( constraint_name, column_index, measure_calculation_function, warn_or_fail)

# cs1.addAbsoluteDimensionCrossProductElementConstraint( constraint_name, dimension_column_index_list,measure_calculation_function,warn_or_fail)
# cs1.addRelativeDimensionCrossProductElementConstraint( constraint_name, dimension_column_index_list,measure_calculation_function,warn_or_fail)

# cs1.addMutuallyExclusiveConstraint("Mutually Exclusive Unique Ids", 0, warn_or_fail)
# cs1.addColumnDataTypeConstraint("unique_id Data Type Constraint", 0, 'int64', warn_or_fail)
# cs1.addDataLayoutConstraint("Data Layout Constraint 1", ['int64', 'int64', None, None, 'int64'], 0)
# cs1.addDataLayoutConstraint("Data Layout Constraint 2", ['int64', 'object', 'object', 'int64', 'int64'], warn_or_fail)
# cs1.addDataLayoutConstraint("Data Layout Constraint 3", ['int64', 'int64', 'str', 'str', 'int64'], 0)
# cs1.addColumnNameConstraint("Column Name Constraint", 0, "unique_id", warn_or_fail)


# cs1.checkAllConstraints(outputFolder='C:/sandbox/data/output/Data_Profile_Tester/')