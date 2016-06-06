import sqlite3
import logging

logging.basicConfig(filename="sample.log", level=logging.DEBUG)

class FileProcessor():

    def __init__(self, line_limit=100):
        # number of lines to load to database all at once
        self.line_limit = line_limit

    def parse_data(self, filename):
        table_name = filename.split('/')[-1].split('_')[0]
        spec_file = './specs/' + table_name + '.csv'
        try:
            data_specs = self.parse_spec(spec_file)
        except IOError, e:
            logging.error("Spec file not found")
            # TODO: alert for processing failure for the file to be processed later
            return
        result = []
        try:
            with open(filename, 'r') as thefile:
                for line in thefile:
                    result.append(self.parse_line(line, data_specs))
                    if len(result) >= self.line_limit:
                        self.load_to_database(result, table_name, data_specs)
                        result = []
            self.load_to_database(result, table_name, data_specs)
        except IOError, e:
            logging.error("Data file not found")
            # TODO: alert for processing failure for the file to be processed later
            return
        except sqlite3.Error, e:
            logging.error("Database error, load failed")
            # TODO: alert for processing failure for the file to be processed later
            return

    def parse_spec(self, filename):
        # returns the column specs to be easily used while data file processing
        column_specs = []
        with open(filename, 'r') as thefile:
            for ind, line in enumerate(thefile):
                if ind != 0:
                    (column_name, width, datatype) = line.strip().split(',')
                    new_column = {'column_name': column_name, 'width': int(width), 'datatype': datatype}
                    column_specs.append(new_column)
        logging.debug(column_specs)
        return column_specs

    def parse_line(self, line, data_specs):
        # returns a tuple from a single line
        row = []
        current = 0
        for column_spec in data_specs:
            field = line[current:current+column_spec['width']].strip()
            current += column_spec['width']
            field = self.convert_field(column_spec['datatype'], field)
            row.append(field)
        return tuple(row)

    def convert_field(self, type, field):
        if type == 'BOOLEAN':
            return field == '1'
        elif type == 'INTEGER':
            return int(field)
        elif type == 'REAL':
            return float(field)
        else:
            return field

    def load_to_database(self, rows, table_name, specs):
        logging.debug(rows)
        sql_field_list = ''
        sql_place_holder = ''
        # generate the table specific parts of the insert statement for bulk load
        for ind, spec in enumerate(specs):
            sql_field_list += '{field_name}'.format(field_name=spec['column_name'])
            sql_place_holder += '?'
            if ind != len(specs) - 1:
                sql_field_list += ', '
                sql_place_holder += ', '
        try:
            con = self.connect_to_db()
            self.create_table(con, table_name, specs)
            insert_sql = """
            INSERT OR IGNORE INTO {table_name} ({field_list}) VALUES ({place_holder})
            """.format(table_name=table_name, field_list=sql_field_list, place_holder=sql_place_holder)
            logging.debug(insert_sql)
            # use executemany to avoid calling insert for every row
            con.executemany(insert_sql, rows)
            con.commit()
        except Exception, e:
            logging.error("Database connection or write failed!")
            logging.error(e.message)
            raise sqlite3.Error

    def connect_to_db(self):
        con = None
        retry = True
        retry_count = 0
        max_retry = 5
        # try reconnecting to db if it is unresponsive, give up after 10 secs
        # change the max based on known db connectivity issues
        while retry:
            retry_count += 1
            if retry_count > max_retry:
                break
            try:
                con = sqlite3.connect('test.db')
            except sqlite3.Error, e:
                logging.warning("Failed connecting to database, trying again!")
                sleep(2)
            else:
                retry = False
        return con

    def create_table(self, con, table_name, specs):
        spec_sql = '('
        for ind, spec in enumerate(specs):
            spec_sql += '{field} {datatype}'.format(field=spec['column_name'], datatype=spec['datatype'])
            if ind != len(specs) - 1:
                spec_sql += ', '
        spec_sql += ')'
        create_sql = """
        CREATE TABLE IF NOT EXISTS {table_name} {table_specs}
        """.format(table_name=table_name, table_specs=spec_sql)
        logging.debug(create_sql)
        con.execute(create_sql)
