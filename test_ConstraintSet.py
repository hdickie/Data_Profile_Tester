import unittest
from unittest import TestCase
from ConstraintSet import create_test_constraint_sets_map_from_xlsx
from ConstraintSet import debug, error, warning, info, critical
import sys, traceback

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
        expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples = {}
        for constraint_set_id in self.test_set_definitions.keys():
            for constraint_id in self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map.keys():
                current_test_type = self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map[constraint_id]['constraint_type']
                current_expected_result = self.test_set_definitions[constraint_set_id].constraint_id_to_args_dict_map[constraint_id]['args']['expected_result']

                if current_test_type not in test_type_to_list_of_constraint_set_id_and_constraint_id_tuples.keys():
                    test_type_to_list_of_constraint_set_id_and_constraint_id_tuples[current_test_type] = [(constraint_set_id,constraint_id)]
                else:
                    test_type_to_list_of_constraint_set_id_and_constraint_id_tuples[current_test_type] += [(constraint_set_id,constraint_id)]

                if current_expected_result not in expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples.keys():
                    expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples[current_expected_result] = [(constraint_set_id,constraint_id)]
                else:
                    expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples[current_expected_result] += [(constraint_set_id,constraint_id)]

        self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples = test_type_to_list_of_constraint_set_id_and_constraint_id_tuples
        self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples = expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples
        #print("#################################################")
        #print(test_type_to_list_of_constraint_set_id_and_constraint_id_tuples)

    #todo it would be good to have a test_expected_errors_method that those errors dont contaminate results for expected successes
    #
    # def test_toString(self):
    #     global stack_depth
    #     global print_logs
    #     debug("ENTER test_toString()")
    #     stack_depth += 1
    #
    #     debug("ASSERT test_toString()")
    #     stack_depth -= 1
    #     self.assertEqual(1, 0)  #todo hard code correct result to assert for test_toString()
    #     debug("PASSED test_toString()")
    #
    # def test_showResults(self):
    #     global stack_depth
    #     global print_logs
    #     debug("ENTER test_showResults()")
    #     stack_depth += 1
    #
    #     list(self.test_set_definitions.values())[0].showResults()
    #     debug("ASSERT test_showResults()")
    #     stack_depth -= 1
    #     self.assertEqual(1,0) #todo hard code correct result to assert for test_showResults()
    #     debug("PASSED test_showResults()")

    @unittest.skip('Already passed')
    def test_expected_errors(self):
        global stack_depth
        global print_logs
        print_logs = False
        debug("ENTER test_expected_errors()")
        stack_depth += 1
        relevant_test_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['ERR']

        stack_depth -= 1
        for i in range(0, len(relevant_test_tuples)):
            current_tuple = relevant_test_tuples[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            info("Checking Constraint #"+str(current_tuple[1]) )
            info(current_constraint_set.constraint_id_to_args_dict_map[current_tuple[1]]['args']['constraint_name'])

            try:
               current_constraint_set.checkConstraintById(current_tuple[1])
            except:
               return #this is the successful case

        self.assertEqual(0,1) #if this executes, there was a failure

    def execute_test_using_list_of_tuples(self,fail_tuples,success_tuples):
        global stack_depth
        global print_logs
        debug("ENTER test_absolute_file_row_count()")
        stack_depth += 1
        relevant_test_tuples__expect_success = success_tuples
        relevant_test_tuples__expect_fail = fail_tuples

        fail_flag = 0
        failed_test_tuples = []
        exception_list = []

        for i in range(0, len(relevant_test_tuples__expect_success)):
            info("")
            info("")
            current_tuple = relevant_test_tuples__expect_success[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            info("### Checking Constraint " + str(current_tuple))
            info(
                "###     Test Name: " + current_constraint_set.constraint_id_to_args_dict_map[current_tuple[1]]['args'][
                    'constraint_name'])

            try:
                test_result = current_constraint_set.checkConstraintById(current_tuple[1])
                self.assertEqual(test_result, 0)
                error("### Passed test " + str(current_tuple))
            except Exception as e:
                fail_flag += 1
                failed_test_tuples += [current_tuple]
                exception_list += [e]
                error("### Failed test " + str(current_tuple))
                error(e)
                etype, value, tb = sys.exc_info()
                info_, error_ = traceback.format_exception(etype, value, tb)[-2:]
                print(f'Exception in:\n{info_}\n{error_}')

        for i in range(0, len(relevant_test_tuples__expect_fail)):
            info("")
            info("")
            current_tuple = relevant_test_tuples__expect_fail[i]
            current_constraint_set = self.test_set_definitions[current_tuple[0]]
            info("### Checking Constraint " + str(current_tuple))
            info(
                "###     Test Name: " + current_constraint_set.constraint_id_to_args_dict_map[current_tuple[1]]['args'][
                    'constraint_name'])

            try:
                test_result = current_constraint_set.checkConstraintById(current_tuple[1])
                stack_depth -= 1
                self.assertEqual(test_result, 1)
                error("### Passed test " + str(current_tuple))
            except Exception as e:
                fail_flag += 1
                failed_test_tuples += [current_tuple]
                exception_list += [e]

                error("### Failed test " + str(current_tuple))
                error(e)
                etype, value, tb = sys.exc_info()
                info_, error_ = traceback.format_exception(etype, value, tb)[-2:]
                print(f'Exception in:\n{info_}\n{error_}')

        total_test_count = len(relevant_test_tuples__expect_success) + len(relevant_test_tuples__expect_fail)
        info("### Passed " + str((total_test_count - len(failed_test_tuples))) + " / " + str(
            total_test_count) + " tests")

        stack_depth -= 1
        self.assertEqual(fail_flag, 0)


    def test_absolute_file_row_count(self):
        info("RUNNING TEST: test_absolute_file_row_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute File Row Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_file_row_count")

    def test_absolute_column_cardinality(self):
        info("RUNNING TEST: test_absolute_column_cardinality")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Cardinality']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_column_cardinality")

    def test_absolute_column_null_count(self):
        info("RUNNING TEST: test_absolute_column_null_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Null Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_column_null_count")

    def test_absolute_dimension_cross_product_cardinality(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_cardinality")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Cardinality']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_cardinality")

    def test_absolute_dimension_cross_product_element_row_count(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_row_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Row Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_row_count")

    def test_absolute_dimension_cross_product_element_measure_cardinality(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_cardinality")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Cardinality']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_cardinality")

    def test_absolute_dimension_cross_product_element_measure_null_count(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_null_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Null Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_null_count")

    def test_absolute_dimension_cross_product_element_measure_min(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_min")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Min']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_min")

    def test_absolute_dimension_cross_product_element_measure_mean(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_mean")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Mean']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_mean")

    def test_absolute_dimension_cross_product_element_measure_median(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_median")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Median']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_median")

    def test_absolute_dimension_cross_product_element_measure_mode(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_mode")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Mode']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_mode")

    def test_absolute_dimension_cross_product_element_measure_max(self):
        info("RUNNING TEST: test_absolute_dimension_cross_product_element_measure_max")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Dimension Cross Product Element Measure Max']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_dimension_cross_product_element_measure_max")

    def test_absolute_column_data_type(self):
        info("RUNNING TEST: test_absolute_column_data_type")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Data Type']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_column_data_type")

    def test_absolute_layout(self):
        info("RUNNING TEST: test_absolute_layout")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Layout']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_layout")

    def test_absolute_column_name(self):
        info("RUNNING TEST: test_absolute_column_name")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Column Name']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples] #this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are "+str(len(fail_tuples)+len(success_tuples))+" subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples,success_tuples)
        info("FINISHED TEST: test_absolute_column_name")

    def test_absolute_header(self):
        info("RUNNING TEST: test_absolute_header")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Absolute Header']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_absolute_header")

    def test_relative_file_row_count(self):
        info("RUNNING TEST: test_relative_file_row_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative File Row Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_file_row_count")

    def test_relative_column_cardinality(self):
        info("RUNNING TEST: test_relative_column_cardinality")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Cardinality']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_column_cardinality")

    def test_relative_column_null_count(self):
        info("RUNNING TEST: test_relative_column_null_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Null Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_column_null_count")

    def test_relative_dimension_cross_product_cardinality(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_cardinality")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Cardinality']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_cardinality")

    def test_relative_dimension_cross_product_element_row_count(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_row_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Row Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_row_count")

    def test_relative_dimension_cross_product_element_measure_cardinality(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_cardinality")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Cardinality']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_cardinality")

    def test_relative_dimension_cross_product_element_measure_null_count(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_null_count")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Null Count']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_null_count")

    def test_relative_dimension_cross_product_element_measure_min(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_min")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Min']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_min")

    def test_relative_dimension_cross_product_element_measure_mean(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_mean")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Mean']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_mean")

    def test_relative_dimension_cross_product_element_measure_median(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_median")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Median']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_median")

    def test_relative_dimension_cross_product_element_measure_mode(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_mode")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Mode']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_mode")

    def test_relative_dimension_cross_product_element_measure_max(self):
        info("RUNNING TEST: test_relative_dimension_cross_product_element_measure_max")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Dimension Cross Product Element Measure Max']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_dimension_cross_product_element_measure_max")

    def test_relative_column_data_type(self):
        info("RUNNING TEST: test_relative_column_data_type")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Data Type']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_column_data_type")

    def test_relative_layout(self):
        info("RUNNING TEST: test_relative_layout")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Layout']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_layout")

    def test_relative_column_name(self):
        info("RUNNING TEST: test_relative_column_name")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Name']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_column_name")

    def test_relative_column_header(self):
        info("RUNNING TEST: test_relative_column_header")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Relative Column Header']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_relative_column_header")

    def test_bounded_overlap(self):
        info("RUNNING TEST: test_bounded_overlap")
        all_success_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['PASS']
        all_fail_tuples = self.expected_result_to_list_of_constraint_set_id_and_constraint_id_tuples['FAIL']

        test_type_tuples = self.test_type_to_list_of_constraint_set_id_and_constraint_id_tuples['Bounded Overlap']

        fail_tuples = [value for value in test_type_tuples if value in all_fail_tuples]  # this is list intersection
        success_tuples = [value for value in test_type_tuples if value in all_success_tuples]

        info("There are " + str(len(fail_tuples) + len(success_tuples)) + " subtests.")
        self.execute_test_using_list_of_tuples(fail_tuples, success_tuples)
        info("FINISHED TEST: test_bounded_overlap")





if __name__ == "__main__":
    unittest.main()

