# inbuilt
import configparser

# internal
from handuflow.config.config_paths import runtime_mode


class CatalogResolver:
    """
    Resolves table identifiers for local Hive metastore vs Databricks Unity Catalog.

    Local / dev:  schema.table  (set target_unity_catalog to 'local' or 'testing')
    Unity Catalog: catalog.schema.table
    """

    LOCAL_CATALOG_ALIASES = frozenset({"", "local", "testing"})

    def __init__(
        self,
        target_unity_catalog: str,
        *,
        config: configparser.ConfigParser | None = None,
    ) -> None:
        self.target_unity_catalog = (target_unity_catalog or "").strip()
        self._runtime = runtime_mode(config) if config else None

    @property
    def is_local(self) -> bool:
        if self._runtime == "unity_catalog":
            return False
        if self._runtime == "local":
            return True
        return self.target_unity_catalog.lower() in self.LOCAL_CATALOG_ALIASES

    def qualified_table(self, schema: str, table: str, catalog: str | None = None) -> str:
        catalog = (catalog or self.target_unity_catalog).strip()
        if self.is_local or catalog.lower() in self.LOCAL_CATALOG_ALIASES:
            return f"{schema}.{table}"
        return f"{catalog}.{schema}.{table}"

    def bronze_schema(self, catalog: str | None = None) -> str:
        catalog = (catalog or self.target_unity_catalog).strip()
        if self.is_local or catalog.lower() in self.LOCAL_CATALOG_ALIASES:
            return "bronze"
        return f"{catalog}.bronze"

    def staging_schema(self, catalog: str | None = None) -> str:
        catalog = (catalog or self.target_unity_catalog).strip()
        if self.is_local or catalog.lower() in self.LOCAL_CATALOG_ALIASES:
            return "staging"
        return f"{catalog}.staging"

    def target_table(
        self, schema: str, table: str, catalog: str | None = None
    ) -> str:
        return self.qualified_table(schema, table, catalog)
