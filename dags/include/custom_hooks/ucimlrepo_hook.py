import logging
from airflow.hooks.base import BaseHook
from ucimlrepo import fetch_ucirepo
import pandas as pd


class UCIDataFetchHook(BaseHook):
    def __init__(
        self, dataset_id: int = None, dataset_name: str = None, output_path: str = None
    ):
        super().__init__()
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.output_path = output_path
        self.dataset = None
        self.logger = logging.getLogger(__name__)

    def _fetch_dataset(self):
        if self.dataset_id:
            self.dataset = fetch_ucirepo(id=self.dataset_id)
        elif self.dataset_name:
            self.dataset = fetch_ucirepo(name=self.dataset_name)
        else:
            raise ValueError("Either dataset_id or dataset_name must be provided.")

    def get_data(self):
        try:
            if not self.dataset:
                self._fetch_dataset()

            if self.dataset:
                features_df = pd.DataFrame(self.dataset.data.features)
                ids_df = pd.DataFrame(self.dataset.data.ids)
                data_df = pd.concat([ids_df, features_df], axis=1)

                if self.output_path:
                    data_df.to_csv(self.output_path, index=False)
                    self.logger.info(f"Dataset saved to {self.output_path}")
                else:
                    raise ValueError(
                        "Output path must be provided to save the CSV file."
                    )
            else:
                raise ValueError("Dataset has not been fetched.")

        except Exception as e:
            self.logger.error(f"Error during dataset extraction: {str(e)}")
            raise
