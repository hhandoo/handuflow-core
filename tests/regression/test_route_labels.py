"""Unit tests for schema route labels."""

from handuflow.system_shared.route_labels import feed_route_label, table_schema_name


def test_table_schema_name_local_two_part():
    assert table_schema_name("demo.test") == "demo"
    assert table_schema_name("silver.t_iso_language_codes") == "silver"


def test_table_schema_name_unity_catalog_three_part():
    assert table_schema_name("my_catalog.silver.t_iso_language_codes") == "silver"


def test_feed_route_label():
    assert (
        feed_route_label(
            source_table_name="demo.test",
            target_schema_name="silver",
        )
        == "demo-silver"
    )
    assert (
        feed_route_label(
            source_table_name="demo.test",
            target_table_path="silver.t_iso_language_codes",
        )
        == "demo-silver"
    )
    assert (
        feed_route_label(
            source_table_name="demo.src",
            target_schema_name="demo",
        )
        == "demo-demo"
    )
