import unittest
from file_processor import FileProcessor

class FileProcessorTest(unittest.TestCase):

    file_processor = FileProcessor()

    def test_convert_boolean_true(self):
        self.assertTrue(self.file_processor.convert_field('BOOLEAN', '1'))

    def test_convert_boolean_false(self):
        self.assertFalse(self.file_processor.convert_field('BOOLEAN', '0'))

    def test_convert_int1(self):
        self.assertEqual(self.file_processor.convert_field('INTEGER', '5'), 5)

    def test_convert_int2(self):
        self.assertEqual(self.file_processor.convert_field('INTEGER', '190'), 190)

    def test_convert_real(self):
        self.assertEqual(self.file_processor.convert_field('REAL', '34.5'), 34.5)

    def test_convert_negative_int(self):
        self.assertEqual(self.file_processor.convert_field('INTEGER', '-34'), -34)

    def test_convert_text(self):
        self.assertEqual(self.file_processor.convert_field('TEXT', 'some text'), 'some text')

    def test_parse_spec(self):
        spec_file = './specs/fileformat1.csv'
        spec_result = [{'datatype': 'TEXT', 'width': 10, 'column_name': 'name'}, {'datatype': 'BOOLEAN', 'width': 1, 'column_name': 'valid'}, {'datatype': 'INTEGER', 'width': 3, 'column_name': 'count'}]
        self.assertEqual(self.file_processor.parse_spec(spec_file), spec_result)

    def test_parse_line1(self):
        line = 'Barzane   0-12\n'
        spec = [{'datatype': 'TEXT', 'width': 10, 'column_name': 'name'}, {'datatype': 'BOOLEAN', 'width': 1, 'column_name': 'valid'}, {'datatype': 'INTEGER', 'width': 3, 'column_name': 'count'}]
        line_result = ('Barzane', False, -12)
        self.assertEqual(self.file_processor.parse_line(line, spec), line_result)

    def test_parse_line2(self):
        line = 'James     30 170.372.51\n'
        spec = [{'datatype': 'TEXT', 'width': 10, 'column_name': 'name'}, {'datatype': 'INTEGER', 'width': 3, 'column_name': 'age'},
         {'datatype': 'REAL', 'width': 5, 'column_name': 'weight'}, {'datatype': 'REAL', 'width': 4, 'column_name': 'height'},
         {'datatype': 'INTEGER', 'width': 1, 'column_name': 'gender'}]
        line_result = ('James', 30, 170.3, 72.5, True)
        self.assertEqual(self.file_processor.parse_line(line, spec), line_result)

if __name__ == '__main__':
    unittest.main()
