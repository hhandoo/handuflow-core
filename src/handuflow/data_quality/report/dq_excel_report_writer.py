
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DQExcelReportWriter:
    """
    Writes combined DQ results of multiple feeds into an Excel file.
    """

    @staticmethod
    def write(results: list, output_path: str):
        df = pd.DataFrame([r.__dict__ for r in results])
        df.to_excel(output_path, index=False)
        logger.info("DQ Excel report written to %s", output_path)
