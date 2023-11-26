import pandas as pd
from abc import ABC, abstractmethod


class AbstractDataFrameConverter(ABC):
    @abstractmethod
    def convert_to_desired_format(self, input_dataframe: pd.DataFrame, classname: str,
                                  frmt: str = "orientdb") -> pd.DataFrame:
        """
        convert a DataFrame in "record_featured" format to desired format.
            supported formats: ["orientdb"]

        :param input_dataframe:  input_dataframe (pd.DataFrame): The input DataFrame in "record_featured" format.
        :param frmt: desired format of output dataframe
        :return: pd.DataFrame: The DataFrame converted to the OrientDB format.
        """
        pass
