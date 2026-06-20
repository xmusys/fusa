# Fusa — Modified mlx5 Provider

This directory contains the **modified `mlx5` provider** for the [Fusa](https://github.com/Fusa-repo/Fusa) framework.

> The files here replace the upstream `rdma-core/providers/mlx5` at fork point [`0ff4fc60a`](https://github.com/linux-rdma/rdma-core/tree/0ff4fc60a).  
> See the [top-level README](/README.md) for an overview of the full repository.

## Modifications to the mlx5 Provider

| File | Change |
|------|--------|
| `recorder.{c,h}` | **New.** Bloom-filter-based contention recording. Counters track atomic operation collision frequency; when contention exceeds a threshold, the operation is dispatched to the server CPU via RPC instead of executing on the RNIC. |
| `qp.c` | Modified `mlx5_post_send` to hook into the contention recorder and initiate RPC dispatch for high-contention atomics. |
| `verbs.c` | Fusa integration hooks for initialization and teardown. |
| `CMakeLists.txt` | Added `recorder.c` to the shared library sources. |

## FusionCAS Experiment Framework

The `fusioncas/` subdirectory provides the complete evaluation framework:

- **`agent/`** — Fusa-Agent: RPC dispatch control. The agent server on the memory node collects per-slot contention statistics, computes offload strategies, and distributes them to agent clients (compute nodes).
- **`test/`** — Benchmark executables: `test_rdma`, `test_herd`, `test_ycsb`, `test_sequencer`, etc.
- **`smart/`** — Core runtime support for RDMA-CAS/FAA operations.
- **`smart_ht/`** — Hash-table (RACE) workload integration.
- **`scripts/`** — Experiment orchestration (Python) and data extraction.

## Build

### Build the modified mlx5 provider

From the repository root (standard rdma-core build):

```bash
mkdir -p build && cd build
cmake -DIN_PLACE=1 ..
make -j$(nproc)
cd ..
```

### Build the FusionCAS experiments

```bash
cd providers/mlx5/fusioncas
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Quick build (both steps)

```bash
./make.sh        # From repository root
```

## Run a Minimal Experiment

Requirements: two or more machines with ConnectX-5/6/7 NICs connected via RDMA.

### 1. Start the Fusa agent server (memory node)

```bash
./providers/mlx5/fusioncas/build/agent/test_agent server 80000 enable enable 4
```

Arguments: `threshold enable_flag pcie_atomic_flag core_num`

| Arg | Meaning |
|-----|---------|
| `80000` | Contention threshold: atomics exceeding this frequency per slot are offloaded |
| `enable` | Enable the agent dispatch logic |
| `enable` | Enable PCIe Atomic (affects NIC capacity model) |
| `4` | Number of server worker threads |

### 2. Start the Fusa agent client (each compute node)

```bash
./providers/mlx5/fusioncas/build/agent/test_agent client
```

### 3. Run a benchmark

```bash
# On the memory node:
./providers/mlx5/fusioncas/build/test/test_rdma server

# On each compute node (32 threads, depth 2):
./providers/mlx5/fusioncas/build/test/test_rdma client 32 2
```

### 4. Expected behavior

- Low-contention CAS/FAA operations execute directly on the RNIC (normal RDMA path).
- High-contention operations (above threshold) are transparently offloaded to the server CPU via RPC, bypassing RNIC serialization.
- The agent periodically adjusts the offload strategy based on observed contention patterns.

## Runtime Dependencies

The modified `libmlx5.so` must be used with a compatible `libibverbs.so`. Use `LD_LIBRARY_PATH` if the system-installed version is incompatible:

```bash
export LD_LIBRARY_PATH=/path/to/build/lib:$LD_LIBRARY_PATH
```

## Further Reading

- [FusionCAS Details](fusioncas/readme.md) — Agent internals, config files, and experiment workflow.
- [scripts/](fusioncas/scripts/) — Automated experiment launchers and data extraction utilities.

## Acknowledgments

This work builds on [rdma-core](https://github.com/linux-rdma/rdma-core) and incorporates design ideas from [SMART-HT](https://github.com/madsys-dev/smart).
