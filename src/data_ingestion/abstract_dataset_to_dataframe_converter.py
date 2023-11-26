from abc import ABC, abstractmethod
import pandas as pd


class AbstractDatasetToDataFrameConverter(ABC):

    @staticmethod
    @abstractmethod
    def transform(input_dataset) -> pd.DataFrame:
        """
        transform the input dataset to a DataFrame.

        :param input_dataset: The input dataset (can be a file, database, etc.).
        :return: The transformed DataFrame.
        """
        pass

    @abstractmethod
    def process(self) -> pd.DataFrame:
        """
        Process the dataset by converting and transforming it into a DataFrame.

        :return: The processed dataframe.
        """
        dataset = self.convert()
        return self.transform(dataset)

    @abstractmethod
    def convert(self):
        """
        convert the dataset to a format that can be processed further.

        :returns: Any: The converted dataset.
        """
        pass
