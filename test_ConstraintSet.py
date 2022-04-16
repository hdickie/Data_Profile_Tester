import unittest
from unittest import TestCase
from ConstraintSet import create_test_constraint_sets_map_from_xlsx
from ConstraintSet import debug, error, warning, info, critical

global stack_depth
stack_depth = 0

global print_logs
print_logs = True

class TestConstraintSet(TestCase):

    @classmethod
    def setUpClass(self):

        self.test_set_definitions = create_test_constraint_sets_map_from_xlsx(
            'C:/Users/HumeD/PycharmProjects/Data_Profile_Tester/test_constraints.xlsx')
        #print(self.test_set_definitions)
        #print("#################################################")
        test_type_to_list_of_constraint_set_id_and_constraint_id_tuples = {}
        for constraint_set_id in self.test_set_definitions.keys():
            #print('self.test_set_definitions.keys()')
            #print(self.test_set_definitions[constraint_set_id])
            #print(self.test_set_definitions[constraint_set_id])
            for constraint_id in self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map.keys():
                #print(constraint_id)
                #print(self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map)
                #print(dir(self.test_set_definitions[constraint_set_id][constraint_id]))
                #print(self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map[constraint_id])

                current_test_type = self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map[constraint_id]['constraint_type']

                if current_test_type not in test_type_to_list_of_constraint_set_id_and_constraint_id_tuples.keys():
                    test_type_to_list_of_constraint_set_id_and_constraint_id_tuples[current_test_type] = [(constraint_set_id,constraint_id)]
                else:
                    test_type_to_list_of_constraint_set_id_and_constraint_id_tuples[current_test_type] += [(constraint_set_id,constraint_id)]
        self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples = test_type_to_list_of_constraint_set_id_and_constraint_id_tuples
        #print("#################################################")
        #print(test_type_to_list_of_constraint_set_id_and_constraint_id_tuples)

    def test_toString(self):
        global stack_depth
        global print_logs
        debug("ENTER test_toString()")
        stack_depth += 1
        #for i in range(0, len(relevant_test_tuples)):
        #    current_tuple = relevant_test_tuples[i]
        #    current_constraint_set = self.test_set_definitions[current_tuple[0]]
        #    print(str)
        #print(list(self.test_set_definitions.values())[0])
        debug("ASSERT test_toString()")
        stack_depth -= 1
        self.assertEqual(1, 0)  #todo hard code correct result to assert for test_toString()
        debug("PASSED test_toString()")

    def test_showResults(self):
        global stack_depth
        global print_logs
        debug("ENTER test_showResults()")
        stack_depth += 1
        #for i in range(0, len(relevant_test_tuples)):
        #    current_tuple = relevant_test_tuples[i]
        #    current_constraint_set = self.test_set_definitions[current_tuple[0]]
        #    current_constraint_set.showResults()
        list(self.test_set_definitions.values())[0].showResults()
        debug("ASSERT test_showResults()")
        stack_depth -= 1
        self.assertEqual(1,0) #todo hard code correct result to assert for test_showResults()
        debug("PASSED test_showResults()")

    def test_check_absolute_file_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_absolute_file_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute File Row Count']
        stack_depth -= 1
        for i in range(0,len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_absolute_file_constraint()")
            #self.assertEqual(current_constraint_set.checkConstraintById(current_constraint_set.constraint_id_to_args_dict_map[current_tuple[1]]),0)
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]), 0)
        debug("PASSED test_check_absolute_file_constraint()")


    def test_check_absolute_column_cardinality_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_absolute_column_cardinality_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Cardinality']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_absolute_column_cardinality_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_absolute_column_cardinality_constraint()")

    def test_check_relative_column_cardinality_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_relative_column_cardinality_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Cardinality']
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_relative_column_cardinality_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        stack_depth -= 1
        debug("PASSED test_check_relative_column_cardinality_constraint()")


    def test_check_absolute_column_null_count_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_absolute_column_null_count_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Null Count']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_absolute_column_null_count_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_absolute_column_null_count_constraint()")


    def test_check_relative_column_null_count_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_relative_column_null_count_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Null Count']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_relative_column_null_count_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_relative_column_null_count_constraint()")

    def test_check_relative_file_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_relative_file_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative File Row Count']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_relative_file_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_relative_file_constraint()")

    def test_check_absolute_dimension_cross_product_cardinality_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_absolute_dimension_cross_product_cardinality_constraint()")
        stack_depth += 1
        debug("defined tests:"+str(self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples))
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Cardinality']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_absolute_dimension_cross_product_cardinality_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_absolute_dimension_cross_product_cardinality_constraint()")

    def test_check_relative_dimension_cross_product_element_row_count_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_relative_dimension_cross_product_element_row_count_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Row Count']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_relative_dimension_cross_product_element_row_count_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_relative_dimension_cross_product_element_row_count_constraint()")

    def test_check_bounded_overlap_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_bounded_overlap_constraint()")
        stack_depth += 1
        debug("defined tests:" + str(self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples))
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Bounded Overlap']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_bounded_overlap_constraint()")
            current_constraint_id = current_tuple[1]
            debug("..current_constraint_id:"+str(current_constraint_id))
            self.assertEqual(current_constraint_set.checkConstraintById(current_constraint_id),0)
        debug("PASSED test_check_bounded_overlap_constraint()")

    def test_check_column_data_type_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_column_data_type_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Column Data Type']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_column_data_type_constraint()")
            current_constraint_id = current_tuple[1]
            debug("current_constraint_id:"+str(current_constraint_id))
            self.assertEqual(current_constraint_set.checkConstraintById(current_constraint_id),0)
        debug("PASSED test_check_column_data_type_constraint()")

    def test_check_data_layout_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_data_layout_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Layout']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_data_layout_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_data_layout_constraint()")

    def test_check_column_name_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_column_name_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Column Name']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_column_name_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_column_name_constraint()")

    def test_check_header_constraint(self):
        global stack_depth
        global print_logs
        debug("ENTER test_check_header_constraint()")
        stack_depth += 1
        relevant_test_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Header']
        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            debug("..ASSERT test_check_header_constraint()")
            self.assertEqual(current_constraint_set.checkConstraintById(current_tuple[1]),0)
        debug("PASSED test_check_header_constraint()")

if __name__ == "__main__":
    unittest.main()

