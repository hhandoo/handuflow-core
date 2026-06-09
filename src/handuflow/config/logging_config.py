import os
import sys
import shutil
import logging
import configparser
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

from handuflow.config.config_paths import cfg_get


class LoggingConfig:
    """Databricks-safe logging configuration"""

    LOGGER_NAME = "handuflow"

    def __init__(self, run_id: str, config: configparser.ConfigParser):
        self.level = logging.INFO
        self.run_id = run_id
        file_hunt = cfg_get(config, "file_hunt_path")
        log_dir_name = cfg_get(config, "log_directory_name", "handuflow_logs")
        self.final_log_dir = os.path.join(file_hunt, log_dir_name)
        temp = cfg_get(config, "temp_location") or cfg_get(config, "temp_log_location")
        temp = temp.replace("/dbfs", "")
        self.temp_log_dir = os.path.join(temp, log_dir_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(
            self.temp_log_dir, f"handuflow_log_{self.run_id}_{timestamp}.log"
        )
        os.makedirs(self.temp_log_dir, exist_ok=True)
        os.makedirs(self.final_log_dir, exist_ok=True)
        self.logger = logging.getLogger(self.LOGGER_NAME)

    def configure(self):
        self.logger.setLevel(self.level)
        self.logger.propagate = False
        if self.logger.handlers:
            return
        formatter = logging.Formatter(
            "[handuflow] [%(asctime)s] [%(levelname)s] - %(message)s"
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(self.level)
        self.logger.addHandler(console_handler)
        file_handler = TimedRotatingFileHandler(
            self.log_file,
            when="midnight",
            interval=1,
            backupCount=0,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(self.level)
        self.logger.addHandler(file_handler)
        # All handuflow.* modules propagate here; one file + console for the run.
        logging.getLogger("handuflow").setLevel(self.level)
        logging.getLogger("py4j").setLevel(logging.WARN)
        logging.getLogger("pyspark").setLevel(logging.WARN)
        logging.getLogger("org.apache.spark").setLevel(logging.WARN)
        self.logger.info("Log file: %s", self.log_file)


    def move_logs_to_final_location(self):
        print(f"Moving log file from {self.log_file} to {self.final_log_dir}")
        if not self.log_file or not os.path.exists(self.log_file):
            return
        for h in list(self.logger.handlers):
            h.flush()
            h.close()
            self.logger.removeHandler(h)

        dst = os.path.join(self.final_log_dir, os.path.basename(self.log_file))

        shutil.copy2(self.log_file, dst)
        return dst

    def write_run_summary(
        self,
        *,
        run_status: str,
        errors: list[dict] | None = None,
        extra_lines: list[str] | None = None,
        load_results: list | None = None,
        dq_summary: list[dict] | None = None,
    ) -> None:
        """Append a human-readable run summary to the active log file."""
        lines = [
            "== RUN SUMMARY ==",
            f"status={run_status}",
            f"log_file={self.log_file}",
        ]
        if extra_lines:
            lines.extend(extra_lines)
        if errors:
            lines.append(f"phase_errors={len(errors)}")
            for idx, err in enumerate(errors, start=1):
                lines.append(
                    f"  [{idx}] phase={err.get('phase')} "
                    f"type={err.get('error_type')} error={err.get('error')}"
                )
        else:
            lines.append("phase_errors=0")
        if load_results is not None:
            ok = sum(1 for r in load_results if getattr(r, "success", False))
            lines.append(
                f"loads total={len(load_results)} succeeded={ok} "
                f"failed={len(load_results) - ok}"
            )
            for r in load_results:
                if getattr(r, "success", False):
                    continue
                lines.append(
                    f"  load_fail feed_id={getattr(r, 'feed_id', '?')} "
                    f"target={getattr(r, 'target_table_path', '')} "
                    f"error={getattr(r, 'exception_if_any', '')}"
                )
        if dq_summary:
            lines.append(f"dq_feeds={len(dq_summary)}")
            for row in dq_summary:
                lines.append(
                    f"  dq feed_id={row.get('feed_id')} can_ingest={row.get('can_ingest')} "
                    f"standard_passed={row.get('standard_checks_passed')} "
                    f"pre_load_passed={row.get('comprehensive_pre_load_passed')} "
                    f"post_load_passed={row.get('comprehensive_post_load_passed')}"
                )
        lines.append("== END RUN SUMMARY ==")
        for line in lines:
            self.logger.info(line)
        for h in self.logger.handlers:
            h.flush()
