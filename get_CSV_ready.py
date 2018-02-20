#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

import datetime
import time

# A constant list holds all the generic colour names
# The list is not finished and should be maintained during time
GENERIC_COLOUR_NAMES = [
    'amber', 'amethyst', 'apricot', 'aqua', 'aquamarine',
    'auburn', 'azure',
    'beige', 'black', 'blue', 'bronze', 'brown', 'buff',
    'cardinal', 'carmine', 'celadon', 'cerise', 'cerulean',
    'chartreuse', 'chocolate', 'cinnamon',
    'copper', 'coral', 'cream', 'cyan',
    'emerald',
    'gold', 'gray', 'green',
    'lemon', 'lime',
    'olive', 'orange', 'orchid',
    'pink', 'purple',
    'red', 'ruby',
    'silver',
    'tan',
    'violet',
    'white',
    'yellow'
]


class PrepareCSV:
    '''Prepare the provided CSV file to be imported to database'''

    def __init__(self):
        self.csv_file = 'auto_import.csv'
        self.data_raw = pd.read_csv(self.csv_file)
        self.data = self.data_raw.copy()

    def drop_columns(self):
        drop_columns_index = ['Certified', 'DateInStock']
        self.data.drop(columns=drop_columns_index, inplace=True)

        return None

    def rename_columns(self):
        """Rename the columns for the dataframe"""
        self.data.rename(columns={
            'DealerID': 'd_id',
            'DealerName': 'd_name',
            'Type': 'stock_type',  # Needs capitalization
            'Stock': 'stock_id',
            'VIN': 'vin',
            'Year': 'year',
            'Make': 'make',
            'Model': 'model',
            'Body': 'trim',  # Needs parsing
            'Trim': 'body_style',
            'Doors': 'doors',
            'ExtColor': 'exterior_colour',
            'IntColor': 'interior_colour',
            'EngCylinders': 'cylinders',
            'EngDisplacement': 'displacement',  # Needs parsing
            'Transmission': 'transmission_description',  # Needs parsing and split
            'Odometer': 'odometer',
            'Price': 'price',
            'MSRP': 'msrp',
            'Description': 'description',
            'EngType': 'configuration',
            'EngFuel': 'fuel_type',
            'Drivetrain': 'drivetrain',
            'ExtColorGeneric': 'exterior_colour_generic',  # Needs parsing
            'IntColorGeneric': 'interior_colour_generic',  # Needs parsing
            'PassengerCount': 'passengers'
        }, inplace=True)

        return None

    def spilt_columns(self):
        """Split the columns and get the content for the missing columns"""
        special_attentation_columns = [
            'trim',
            'displacement',
            'transmission_description',
        ]

        # Parse and clean the content of trim
        self.data[special_attentation_columns[0]] = self.data[special_attentation_columns[0]].str.replace(
            '(?:\s|^|\d)dr(?:\s)', '')
        self.data[special_attentation_columns[0]] = self.data[special_attentation_columns[0]].str.replace(
            'Sport Utility', 'Sport Utility Vehicle')
        self.data[special_attentation_columns[0]
                  ] = self.data[special_attentation_columns[0]].str.strip()
        # print(self.data[special_attentation_columns[0]])

        # Extract engine displacement
        self.data[special_attentation_columns[1]] = self.data[special_attentation_columns[1]].str.extract(
            '(\d\.\d)', expand=True)
        # print(self.data[special_attentation_columns[1]])

        # Extract transimission speed
        self.data['transmission_speeds'] = self.data[special_attentation_columns[2]].str.extract(
            '(\d+)', expand=True)
        # print(self.data['transmission_speeds'])

        # Extract transimission type
        trans_type_temp = self.data[special_attentation_columns[2]].str.extract(
            '((?:\s|^)Automatic(?:\s|$))|((?:\s|^)Manual(?:\s|$))', expand=True)
        self.data['transmission_type'] = trans_type_temp[0].fillna(
            trans_type_temp[1])
        self.data['transmission_type'] = self.data['transmission_type'].str.strip()
        # print(self.data['transmission_type'])

        # Parse and clean the content of transmission description
        self.data[special_attentation_columns[2]
                  ] = self.data[special_attentation_columns[2]].str.replace('Spd', 'Speed')
        self.data[special_attentation_columns[2]
                  ] = self.data[special_attentation_columns[2]].str.replace('w/Manual', 'Manual')
        self.data[special_attentation_columns[2]] = self.data[special_attentation_columns[2]].str.replace(
            'w/Automatic', 'Automatic')
        self.data[special_attentation_columns[2]
                  ] = self.data[special_attentation_columns[2]].str.replace('w/OD', '')
        self.data[special_attentation_columns[2]
                  ] = self.data[special_attentation_columns[2]].str.strip()
        # print(self.data[special_attentation_columns[2]])

        return None

    def check_duplicate_rows(self):
        """Check if any duplicated vehicle infomation exists"""
        duplicated_columns = pd.DataFrame()
        duplicated_columns = self.data[self.data['vin'].duplicated(keep=False)]

        return duplicated_columns

    def process_duplicate_rows(self):
        """Do something for duplicated infomation"""
        pass

    def add_vehicle_id_column(self):
        duplication = self.check_duplicate_rows()
        if not duplication.empty:
            self.process_duplicate_rows()
        else:
            print("No duplicated rows in the import file.")
            pass
        self.data['v_id'] = range(1, len(self.data) + 1)
        # self.data['v_id'] = 0

        # print(self.data['v_id'])

        return None

    def add_source_column(self):
        self.data['last_modified_by'] = 'IMPORT'

        return None

    def add_timestamp_columns(self):
        timestamp_columns = [
            'created_time',
            'last_modified_time'
        ]

        for i in range(len(timestamp_columns)):
            self.data[timestamp_columns[i]] = pd.to_datetime('now')
            # SQLite uses string for timestamp
            self.data[timestamp_columns[i]
                      ] = self.data[timestamp_columns[i]].apply(str)

        # print(self.data[timestamp_columns[1]])

        return None

    def get_colour(self, colour, colour_generic):
        """This function gets the generic colour from the
        colour column and compares with the content inside
        the colour_generic column
        """
        colour_column = colour
        gerneic_colour_column = colour_generic
        colour_content = colour_column.str.lower()
        colour_content = colour_content.str.split()

        # Iterate each row to get the generic color
        for i in range(len(colour_content)):
            # Check if the colour column is empty
            if pd.isnull(colour_column.iloc[i]):
                # Leave empty if the column is empty
                gerneic_colour_column.iloc[i] = ''
            else:
                # Check if the generic colour column is empty
                if pd.isnull(gerneic_colour_column.iloc[i]):
                    colour_temp = [
                        colour for colour in colour_content.iloc[i] if colour in GENERIC_COLOUR_NAMES]
                    # Check if there is a generic color
                    if colour_temp:
                        gerneic_colour_column.iloc[i] = colour_temp[0]
                    else:
                        # If the column is not filled and there is no
                        # common generic colour, leave empty
                        gerneic_colour_column.iloc[i] = ''
                else:  # if the column is filled, do nothing
                    pass

        return gerneic_colour_column

    def get_colour_columns(self):
        colour_columns = [
            'exterior_colour',
            'interior_colour',
            'exterior_colour_generic',
            'interior_colour_generic'
        ]

        self.data[colour_columns[2]] = self.get_colour(
            self.data[colour_columns[0]], self.data[colour_columns[2]])
        self.data[colour_columns[3]] = self.get_colour(
            self.data[colour_columns[1]], self.data[colour_columns[3]])

        # print(self.data[colour_columns[2]])

        return None

    def capitalize_columns(self):
        parse_to_capitalization_columns = [
            'd_name',
            'make',
            'exterior_colour',
            'interior_colour',
            'transmission_type',
            'transmission_description'
        ]

        for i in range(len(parse_to_capitalization_columns)):
            self.data[parse_to_capitalization_columns[i]
                      ] = self.data[parse_to_capitalization_columns[i]].str.title()

        # print(self.data[parse_to_capitalization_columns[5]])

        return None

    def all_cap_columns(self):
        parse_to_all_caps_columns = [
            'stock_type',
            'stock_id',
            'vin',
            'configuration',
            'drivetrain',
            'last_modified_by'
        ]

        for i in range(len(parse_to_all_caps_columns)):
            self.data[parse_to_all_caps_columns[i]
                      ] = self.data[parse_to_all_caps_columns[i]].str.upper()

        # print(self.data[parse_to_all_caps_columns[5]])

        return None

    def all_lower_columns(self):
        parse_to_all_lower_cases_columns = [
            'exterior_colour_generic',
            'interior_colour_generic'
        ]

        for i in range(len(parse_to_all_lower_cases_columns)):
            self.data[parse_to_all_lower_cases_columns[i]
                      ] = self.data[parse_to_all_lower_cases_columns[i]].str.lower()

        # print(self.data[parse_to_all_lower_cases_columns[1]])

        return None

    def num_columns(self):
        """Convert the number columns to the number types.
        The reason of spliting small and big integers is
        to use memory efficiently.
        """
        parse_to_small_int_columns = [
            'doors',
            'cylinders',
            'transmission_speeds',
            'passengers'
        ]

        parse_to_big_int_columns = [
            'd_id',
            'v_id',
            'odometer'
        ]

        parse_to_float_columns = [
            'displacement',
            'price',
            'msrp'
        ]

        for i in range(len(parse_to_small_int_columns)):
            self.data[parse_to_small_int_columns[i]
                      ] = self.data[parse_to_small_int_columns[i]].astype(np.int8)

        for i in range(len(parse_to_big_int_columns)):
            self.data[parse_to_big_int_columns[i]
                      ] = self.data[parse_to_big_int_columns[i]].astype(np.int32)

        for i in range(len(parse_to_float_columns)):
            # SQLite float type is np.float32
            self.data[parse_to_float_columns[i]
                      ] = self.data[parse_to_float_columns[i]].astype(np.float64)

        # print(self.data[parse_to_float_columns[2]])

        return None

    def clean_column_formats(self):
        self.capitalize_columns()
        self.all_cap_columns()
        self.all_lower_columns()
        self.num_columns()

        return None

    def prepare_CSV(self):
        """Prepare data to be imported"""
        self.drop_columns()
        self.rename_columns()
        self.spilt_columns()
        self.add_vehicle_id_column()
        self.add_source_column()
        self.add_timestamp_columns()
        self.get_colour_columns()
        self.clean_column_formats()

        # print(self.data.info())
        # print(self.data.sample(10))

        return self.data


if __name__ == "__main__":
    csv = PrepareCSV()
    csv.prepare_CSV()
