# FusionCAS Details

This document provides **internal details** for the `fusioncas/` subdirectory.
For the project-level overview and installation model (replace `rdma-core/providers/mlx5`), see the outer README:

- `providers/mlx5/readme.md`

## Directory Details

```
fusioncas/
├── agent/        # Fusa-Agent implementation (`test_agent`, RPC control logic)
├── config/       # JSON configs for different experiments
├── exp_log/      # Raw logs and processed outputs
├── include/      # Headers used by SMART/Fusa runtime
├── scripts/      # Experiment orchestration and extraction scripts
├── smart/        # Core runtime implementation
├── smart_ht/     # Hash-table/RACE-related integration
├── test/         # Main experiment executables
└── util/         # Utility components
```

## Build Notes

Build from `fusioncas/`:

```bash
mkdir -p build
cd build
cmake ..
make -j
```

Recommended practice in this prototype:

- memory node: `-O3`
- compute nodes: `-O0` for debugging/analysis in contention studies

Compilation flags are controlled mainly in:

- top-level `CMakeLists.txt`
- `test/CMakeLists.txt`

## Agent Details (`agent/test_agent`)

Run in `build/`.

### Client mode (compute node)

```bash
./agent/test_agent client
```

### Server mode (memory node)

```bash
./agent/test_agent server [threshold] [enable_flag] [pcie_atomic_flag] [core_num] [sequencer_flag]
```

Example:

```bash
./agent/test_agent server 80000 enable enable 4
```

Arguments:

1. `threshold`: initial contention threshold (default `100000`)
2. `enable_flag`: `enable` or `disable`
3. `pcie_atomic_flag`: `enable` or `disable`
4. `core_num`: application server worker count (default `1`)
5. `sequencer_flag`: optional `enable`/`disable`

## Configuration Details

### Commonly tuned fields

- `machine_num`, `compute_node.machine_id`, `port`
- `compute_node.ycsb_path` (and staged paths in adaptation experiments)
- `memory_node.rpc_param.unsignal_batch`

### File-specific notes

- `config/test_rdma.json`: default config for most workloads
- `config/adaption_config.json`: staged workload switching (`ycsb_path`, `second_path`, `third_path`)
- `config/agent.json`: agent channel configuration (its `port` is different from app data-plane usage)
- `config/backend.json` and `config/datastructure.json`: RACE integration related
- `config/smart_config.json`: SMART strategy and QP-level parameters

## Experiment Execution Details

Most experiments are launched from the memory node by Python scripts in `scripts/`.
Representative entry points:

- `run_cas_retry.py`
- `run_adaption.py`
- `run_sequencer.py`
- `run_race.py`
- `run_ycsb.py`

Typical script behavior:

1. update JSON config files
2. start agent server on memory node
3. start agent clients on compute nodes through SSH
4. launch test binary (`build/test/*`)
5. collect outputs under `exp_log/`

## Driver Integration Detail

For Fusa RPC dispatch metadata lookup, this prototype uses:

`thread_id = QPN % __MAX_RECORDER_NUM`

If you integrate other applications with a different QP/thread model, update this mapping rule accordingly.

## Acknowledgments

This submodule includes partial source code derived from or adapted from [`rdma-core`](https://github.com/linux-rdma/rdma-core) and [`SMART-HT`](https://github.com/madsys-dev/smart), and we acknowledge the original projects and their contributors.
