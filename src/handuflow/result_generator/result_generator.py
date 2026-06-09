# inbuilt
import os
import time
import logging
import configparser
from datetime import datetime
from io import BytesIO
from dataclasses import asdict
import shutil

# external
from openpyxl import Workbook
import pandas as pd

#internal
from handuflow.config.config_paths import cfg_get
from handuflow.exception.result_generation_exception import ResultGenerationException
from handuflow.config.run_logger import log_step
from handuflow.exception.base_exception import BaseException
from handuflow.data_movement_controller.data_class.load_result import LoadResult

class ResultGenerator():

    def __init__(
            self, 
            payload: list[dict], 
            file_hunt_path: str, 
            run_id: str, 
            config: configparser.ConfigParser, 
            system_report: pd.DataFrame, 
            load_results: list[LoadResult]
        ) -> None:
        self.payload = payload
        self.file_hunt_path = file_hunt_path
        self.run_id = run_id
        self.final_feed_status = []
        self.standard_results = []
        self.comprehensive_results = []
        self.sheets = []
        self.save_path = None
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.dashboard_cols = [
            "feed_id",
            "feed_name",
            "system_name",
            "subsystem_name",
            "category",
            "sub_category",
            "standard_checks_configured",
            "standard_checks_passed",
            "comprehensive_pre_load_configured",
            "comprehensive_pre_load_passed",
            "comprehensive_post_load_configured",
            "comprehensive_post_load_passed",
            "can_ingest",
        ]

        self.sheets.append(
            {
                "sheet_name": "System Readiness",
                "df":system_report
            }
        )
        load_df = pd.DataFrame([asdict(r) for r in load_results])
        if not load_df.empty and "feed_id" in load_df.columns:
            load_df = load_df.sort_values(by="feed_id")
        self.sheets.append({"sheet_name": "Load Report", "df": load_df})

    def run(self):
        try:
            log_step(self.logger, "result_generator", status="START", run_id=self.run_id)
            self.__segregate_results()
            self.__generate_full_feed_status()
            self.__generate_standard_results()
            self.__generate_comprehensive_results()
            self.__generate_dashboard()
            self.__generate_result_file()
            log_step(
                self.logger,
                "result_generator",
                status="OK",
                run_id=self.run_id,
                sheet_count=len(self.sheets),
                path=self.save_path,
            )
        except Exception as e:
            raise ResultGenerationException(
                message="Something went wrong while running result generator.",
                error_code="HF080",
                original_exception=e,
            )

    def __sheet_generator(self, df: pd.DataFrame, sheet_name: str):
        df = df.copy()   # ← THIS is mandatory
        df["run_id"] = self.run_id
        self.logger.info(
            "Report sheet added: name=%s rows=%s cols=%s",
            sheet_name,
            len(df),
            len(df.columns),
        )

        self.sheets.append(
            {
                "sheet_name": sheet_name,
                "df": df
            }
        )

    def __generate_full_feed_status(self):
        if not self.final_feed_status:
            return
        final_feed_status_df = pd.DataFrame(self.final_feed_status)
        col = "feed_id"
        final_feed_status_df.insert(0, col, final_feed_status_df.pop(col))
        final_feed_status_df = final_feed_status_df.sort_values(by=col)
        self.__sheet_generator(final_feed_status_df, "Feed Status (B-G)")

    def __generate_standard_results(self):
        rows_with_checks = [
            r
            for r in self.standard_results
            if r.get("standard_checks_result")
        ]
        if not rows_with_checks:
            return
        df = pd.DataFrame(rows_with_checks)
        df = df.explode("standard_checks_result").reset_index(drop=True)
        df = df.dropna(subset=["standard_checks_result"])
        df = pd.concat(
            [
                df.drop(columns=["standard_checks_result"]),
                df["standard_checks_result"].apply(pd.Series),
            ],
            axis=1,
        )
        df.insert(0, "check_number", range(1, len(df) + 1))
        self.__sheet_generator(df, "Standard Check Result")

    def __generate_comprehensive_results(self):
        rows_with_checks = [
            r
            for r in self.comprehensive_results
            if r.get("comprehensive_results")
        ]
        if not rows_with_checks:
            return
        df = pd.DataFrame(rows_with_checks)
        df = df.explode("comprehensive_results").reset_index(drop=True)
        df = df.dropna(subset=["comprehensive_results"])
        comp = df["comprehensive_results"].apply(
            lambda x: x if isinstance(x, dict) else {}
        )
        df = pd.concat(
            [
                df.drop(columns=["comprehensive_results"]),
                comp.apply(pd.Series),
            ],
            axis=1,
        )
        for col in ("load_stage", "check_name", "status", "failed_records", "severity"):
            if col not in df.columns:
                df[col] = None
        df = df.sort_values(by=["feed_id", "load_stage", "check_name"], na_position="last")
        df.insert(0, "check_number", range(1, len(df) + 1))
        self.__sheet_generator(df, "Comprehensive Check Result")

    
    def __generate_dashboard(self):
        if not self.final_feed_status:
            return
        final_feed_status_df = pd.DataFrame(self.final_feed_status)
        dashboard_df = final_feed_status_df[[c for c in self.dashboard_cols if c in final_feed_status_df.columns]]
        self.__sheet_generator(dashboard_df, "Dashboard")
        

    def __generate_file_name(self) -> str:
        output_directory = os.path.join(
            self.file_hunt_path,
            cfg_get(self.config, "outbound_directory_name"),
        )
        if os.path.exists(output_directory) == False:
            os.mkdir(output_directory)
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        return os.path.join(output_directory, f"results_{self.run_id}_{date_str}_{int(time.time())}.xlsx")

    def __normalize_excel_value(self, v):
        if isinstance(v, (list, tuple)):
            return ", ".join(map(str, v))
        if isinstance(v, BaseException):
            msg = getattr(v, "message", None) or str(v)
            return str(msg)[:8000]
        return v

    def __generate_result_file(self):
        try:
            wb = Workbook()
            default_sheet = wb.active
            if default_sheet is not None:
                wb.remove(default_sheet)

            for sheet in self.sheets:
                df = sheet["df"]

                ws = wb.create_sheet(title=sheet["sheet_name"][:31])
                ws.append(list(df.columns))
                for row in df.itertuples(index=False, name=None):
                    ws.append([self.__normalize_excel_value(v) for v in row])

            self.save_path = self.__generate_file_name()

            buffer = BytesIO()
            wb.save(buffer)
            buffer.seek(0)

            with open(self.save_path, "wb") as f:
                shutil.copyfileobj(buffer, f)

            self.logger.info(f"Excel report created: {os.path.abspath(self.save_path)}")
        except Exception as e:
            raise ResultGenerationException(
                message="Something went wrong while generating excel file.",
                error_code="HF081",
                original_exception=e,
            )



    def __segregate_results(self):
        try:
            if not self.payload:
                self.logger.warning(
                    "No feed DQ manifest entries; report will contain system and load sheets only."
                )
                return
            for status in self.payload:
                row = dict(status)
                standard_checks = row.get("standard_checks_result") or []
                if standard_checks:
                    self.standard_results.append(
                        {
                            "feed_id": row["feed_id"],
                            "standard_checks_result": standard_checks,
                        }
                    )
                comprehensive_checks = row.get("comprehensive_results") or []
                if comprehensive_checks:
                    self.comprehensive_results.append(
                        {
                            "feed_id": row["feed_id"],
                            "comprehensive_results": comprehensive_checks,
                        }
                    )
                row.pop("standard_checks_result", None)
                row.pop("comprehensive_results", None)
                row.pop("dq_traceback", None)
                self.final_feed_status.append(row)

        except Exception as e:
            raise ResultGenerationException(
                message="Something went wrong while segregating results",
                error_code="HF082",
                original_exception=e,
            )

