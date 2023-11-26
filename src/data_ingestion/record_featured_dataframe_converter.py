from datetime import datetime

import pandas as pd
from .abstract_dataframe_converter import AbstractDataFrameConverter


class RecordFeaturedToOrientDBConverter(AbstractDataFrameConverter):
    def convert_to_desired_format(self, input_dataframe: pd.DataFrame, classname: str,
                                  frmt: str = "orientdb") -> pd.DataFrame:
        """
        convert a DataFrame in "record_featured" format to desired format.
            supported formats: ["orientdb"]

        :param classname: class name in database
        :param input_dataframe:  input_dataframe (pd.DataFrame): The input DataFrame in "record_featured" format.
        :param frmt: desired format of output dataframe
        :return: pd.DataFrame: The DataFrame converted to the OrientDB format.
        """
        if frmt.lower() == "orientdb":
            return self._convert_to_orientdb_format(input_dataframe, classname=classname)
        elif frmt.lower() == "orientdb_timeseries":
            return self._convert_to_time_hierarchy_orientdb_format(input_dataframe)
        else:
            raise ValueError(f"Unsupported format: {frmt}")

    @staticmethod
    def _convert_to_orientdb_format(input_dataframe: pd.DataFrame, classname: str, is_edge: bool = True) -> pd.DataFrame:
        """
        Convert a DataFrame in "record_featured" format to a format suitable for OrientDB.

        :param: input_dataframe (pd.DataFrame): The input DataFrame in "record_featured" format.
        :param: classname (str): The class that  records should be stored in.

        :returns: pd.DataFrame: The DataFrame converted to the OrientDB format.
        """

        # if is_edge:
        #     orientdb_df = pd.DataFrame({
        #         '@class': classname,
        #     }.update({
        #         key: value for key, value in input_dataframe.to_dict(orient='records')
        #     }))
        # else:
        #     orientdb_df = pd.DataFrame({
        #         '@class': classname,
        #     }.update({
        #         key: value for key, value in input_dataframe.to_dict(orient='records')
        #     }))

        return input_dataframe

    @staticmethod
    def _convert_to_time_hierarchy_orientdb_format(dataframe: pd.DataFrame):
        """
        Create a time hierarchy in the database and return the formatted DataFrame.

        :param: dataframe (pd.DataFrame): The input DataFrame.

        :return: pd.DataFrame: The formatted DataFrame.
        """
        formatted_data = []

        for index, row in dataframe.iterrows():
            date_parts = row['start_time'].split(' ')
            date_part, time_part = date_parts[0].split('/'), date_parts[1].split(':')

            # Extract month and day as integers
            year = date_part[0]
            month = date_part[1]
            day = date_part[2]
            hour = time_part[0]

            year_class = f'Year_{year}'
            formatted_data.append({'@class': year_class})
            month_class = f'Month_{year}_{month}'
            formatted_data.append({'@class': month_class})
            day_class = f'Day_{year}_{month}_{day}'
            formatted_data.append({'@class': day_class})
            hour_class = f'Hour_{year}_{month}_{day}_{hour}'
            formatted_data.append({'@class': hour_class})

            linkmap_entry = {'@type': 'LINKMAP', '@field': hour, '@class': hour_class}
            formatted_data.append(linkmap_entry)

        formatted_df = pd.DataFrame(formatted_data)
        print(formatted_df.head())
        return formatted_df
