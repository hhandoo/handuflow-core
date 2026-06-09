# `data_flow_diagram_generator/` package

Lineage tree diagram generation from validated master specs.

---

## `data_flow_diagram_generator/data_flow_diagram_generator.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Generate matplotlib lineage tree PNG from feed dependencies. |

### Class: `DataFlowDiagramGenerator`

| Method | Description |
|--------|-------------|
| `run(validated_specs)` | Build and save diagram to configured output path |

### Behavior

1. Parse `source_table` → `target_table` edges from master specs
2. Layout as directed tree (networkx + matplotlib)
3. Color nodes by medallion layer (bronze / silver / gold)
4. Save PNG to `data_flow_diagram_path` from config

**Dependencies:** `config.config_paths`

**Called by:** `Orchestrator._finalize_run()`

**Requires:** `matplotlib`, `networkx` (optional runtime dependencies)
