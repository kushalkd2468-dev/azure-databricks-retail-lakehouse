"""Environment/config helpers shared by all notebooks."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    catalog: str
    landing_path: str
    checkpoint_path: str

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"


def configure_adls_oauth(spark, dbutils, storage_account: str, scope: str) -> None:
    """Authenticate Spark to ADLS Gen2 with a service principal stored in a secret scope."""
    host = f"{storage_account}.dfs.core.windows.net"
    tenant = dbutils.secrets.get(scope, "sp-tenant-id")
    spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{host}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{host}",
                   dbutils.secrets.get(scope, "sp-client-id"))
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{host}",
                   dbutils.secrets.get(scope, "sp-client-secret"))
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{host}",
                   f"https://login.microsoftonline.com/{tenant}/oauth2/token")


def init_environment(spark, dbutils, catalog: str, storage_account: str, scope: str) -> Config:
    """Return paths for the run.

    * storage_account set  -> ADLS Gen2 (abfss://) via service principal OAuth
    * storage_account empty -> Unity Catalog Volumes (handy for Databricks Free/trial)
    """
    if storage_account:
        configure_adls_oauth(spark, dbutils, storage_account, scope)
        base = f"abfss://%s@{storage_account}.dfs.core.windows.net/retail"
        return Config(catalog, base % "landing", base % "checkpoints")
    return Config(
        catalog,
        f"/Volumes/{catalog}/landing/raw/retail",
        f"/Volumes/{catalog}/landing/checkpoints/retail",
    )
