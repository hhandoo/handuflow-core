# inbuilt
import logging
import configparser

# external
from pyspark.sql import SparkSession
import pandas as pd

# internal
from handuflow.validation.validator import Validator
from handuflow.validation.validation_context import ValidationContext
from handuflow.config.config_paths import cfg_get
from handuflow.validation.validation_rules.validate_master_specs import ValidateMasterSpecs
from handuflow.validation.validation_rules.enforce_master_specs_structure import (
    EnforceMasterSpecsStructure,
)
from handuflow.validation.validation_rules.validate_feed_specs_json import ValidateFeedSpecsJSON
from handuflow.validation.validation_rules.primary_key import PrimaryKey
from handuflow.validation.validation_rules.column_exists_in_selection import (
    ColumnExistsInSelection,
)
from handuflow.validation.validation_rules.enforce_standard_checks import EnforceStandardChecks
from handuflow.validation.validation_rules.standard_check_structure_check import (
    StandardCheckStructureCheck,
)
from handuflow.validation.validation_rules.comprehensive_checks_dependency_dataset_check import (
    ComprehensiveChecksDependencyDatasetCheck,
)
from handuflow.validation.validation_rules.partition_keys_check import PartitionKeysCheck
from handuflow.validation.validation_rules.composite_keys_check import CompositeKeysCheck
from handuflow.validation.validation_rules.vacuum_hours_check import VacuumHoursCheck
from handuflow.exception.validation_error import ValidationError


class SystemLaunchValidator:

    def __init__(
        self,
        file_hunt_path: str,
        spark: SparkSession,
        config: configparser.ConfigParser,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.spark = spark
        self.config = config

        self.context = ValidationContext(
            spark=spark,
            file_hunt_path=file_hunt_path,
            master_spec_name=cfg_get(
                self.config, "master_spec_name", "master_specs.xlsx", section="FILES"
            ),
        )

    def __init_rules(self):
        self.rules = [
            ValidateMasterSpecs(),
            EnforceMasterSpecsStructure(),
            ValidateFeedSpecsJSON(),
            PrimaryKey(),
            ColumnExistsInSelection(),
            PartitionKeysCheck(),
            CompositeKeysCheck(),
            VacuumHoursCheck(),
            EnforceStandardChecks(),
            StandardCheckStructureCheck(),
            ComprehensiveChecksDependencyDatasetCheck(),
        ]

    def run(self):
        try:
            self.__init_rules()
            validator = Validator(self.rules, fail_fast=True)
            return validator.validate(self.context)
        except Exception as e:
            raise ValidationError(
                message="Something went wrong in system validation",
                error_code="HF013",
                original_exception=e,
            )

    def get_validated_master_specs(self) -> pd.DataFrame:
        opt_df = self.context.get_master_specs()
        opt_df = opt_df[opt_df["is_active"] == True]
        return opt_df
