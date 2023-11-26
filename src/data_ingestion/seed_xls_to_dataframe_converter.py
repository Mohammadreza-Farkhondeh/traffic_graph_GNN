import os
from typing import Union, List, Dict

import pandas as pd

from .abstract_dataset_to_dataframe_converter import AbstractDatasetToDataFrameConverter


class XlsToDatasetConverter(AbstractDatasetToDataFrameConverter):
    """
    converter tabular data as excel in seed directory(config) to pd.DataFrame in self._data_set
    can be used to store dataset in graph_database
    """

    def __init__(self, seed_data_directory: str):
        self.seed_data_directory = seed_data_directory
        self._data_set: Union[pd.DataFrame, None] = None

    @staticmethod
    def transform(df: pd.DataFrame) -> list[dict]:
        """
        transform param df to a list of dictionaries(documents)
        :param df: the df that going to be transformed, its hardcoded...
        :return: the list of dicts derived from dataframe
        """
        transformed_data = []

        for index, row in df.iterrows():
            road_code = row['کد محور']
            road_name = row['نام محور']
            start_time = row['زمان شروع']
            end_time = row['زمان پایان']
            duration_minutes = row['مدت زمان کارکرد (دقیقه)']
            num_vehicles = row['تعداد کل وسیله نقلیه']
            avg_speed = row['سرعت متوسط']
            num_speed_violations = row['تعداد تخلف سرعت غیر مجاز']
            num_distance_violations = row['تعداد تخلف فاصله غیر مجاز']
            num_overtake_violations = row['تعداد تخلف سبقت غیر مجاز']
            estimated_count = row['تعداد برآورد شده']

            source, destination = road_name.split('-', 1)

            transformed_data.append({
                'road_code': road_code,
                'road_name': road_name,
                'start_time': start_time,
                'end_time': end_time,
                'duration_minutes': duration_minutes,
                'num_vehicles': num_vehicles,
                'avg_speed': avg_speed,
                'num_speed_violations': num_speed_violations,
                'num_distance_violations': num_distance_violations,
                'num_overtake_violations': num_overtake_violations,
                'estimated_count': estimated_count,
                'from': source.strip(),
                'to': destination.strip()
            })

        return transformed_data

    def process(self) -> pd.DataFrame:
        """
        iterate through all files in seed directory and transform Excel files
        :return: the transformed Excel files will combine in a dataFrame,
                    df will set as self._data_set and return
        """
        all_data = []

        # Iterate through all subdirectories in the specified directory
        for subdir, dirs, files in os.walk(self.seed_data_directory):
            for file in files:
                if file.endswith('.xlsx') or file.endswith('.xls'):
                    file_path = os.path.join(subdir, file)
                    df = pd.read_excel(file_path)
                    transformed_data = self.transform(df)
                    all_data.extend(transformed_data)
                    break

        combined_df = pd.DataFrame(all_data)

        self._data_set = combined_df
        return combined_df

    def convert(self) -> pd.DataFrame:
        """
        makes sure to return the transformed dataset
        :return: transformed dataset
        """
        if self._data_set is None:
            self.process()
        return self._data_set


if __name__ == "__main__":
    converter = XlsToDatasetConverter(seed_data_directory=os.getcwd().replace('src/data_ingestion', 'seed/'))
    converter.convert()
    converter.convert().to_csv('exp.csv')
