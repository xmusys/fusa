# Fusa — A Hardware–Software-Collaborated RDMA Atomic Scaling Framework

This repository is a **fork of [rdma-core](https://github.com/linux-rdma/rdma-core)** with modifications to `providers/mlx5` that implement the **Fusa** framework. Fusa is a hardware-software collaborative framework that improves RDMA atomic scalability by keeping low-contention atomics on RNIC hardware and selectively offloading high-contention atomics to server-side CPU software.

The fork is initialized from rdma-core commit [`0ff4fc60a`](https://github.com/linux-rdma/rdma-core/tree/0ff4fc60a) and all modifications are confined to the `providers/mlx5/` directory.

For technical details, please refer to:
> Guangyang Deng, Qiangsheng Su, Zhirong Shen, Qing Wang, Yina LV, Ronglong Wu, Jiwu Shu. Breaking Barriers in Atomic Scaling: A Hardware–Software-Collaborated Framework to Deconstruct RDMA Atomic. In *53rd Annual International Symposium on Computer Architecture (ISCA 2026)*.

---

## Repository Layout

```
├── providers/mlx5/       # Modified mlx5 provider with Fusa hooks
│   ├── qp.c, verbs.c, ...  # Core mlx5 provider sources
│   ├── recorder.{c,h}      # Bloom-filter-based contention recorder
│   ├── fusioncas/          # Experiment framework and evaluation binaries
│   │   ├── agent/          # Fusa-Agent (RPC dispatch control)
│   │   ├── test/           # Experiment executables
│   │   ├── smart/          # Runtime support
│   │   ├── smart_ht/       # Hash-table integration
│   │   ├── util/           # Utility components
│   │   └── scripts/        # Experiment orchestration scripts
│   └── README.md           # Provider-level usage details
├── CMakeLists.txt          # Standard rdma-core CMake (unmodified upstream)
├── build.sh                # Standard rdma-core build script (unmodified upstream)
├── make.sh                 # Shortcut: builds rdma-core then copies libs into fusioncas/
└── README.md               # This file
```

All other files (`libibverbs/`, `librdmacm/`, `libibumad/`, other `providers/*/`) remain at their upstream rdma-core `0ff4fc60a` state.

---

## Building

### 1. Build rdma-core (produces modified `libmlx5.so`)

```bash
# Standard rdma-core build (from repo root)
mkdir -p build
cd build
cmake -DIN_PLACE=1 ..
make -j$(nproc)
cd ..

# The modified libmlx5.so is at: build/lib/libmlx5.so*
```

The `build.sh` script (inherited from upstream) does the same:

```bash
./build.sh
```

### 2. Build the FusionCAS experiment framework

```bash
cd providers/mlx5/fusioncas
mkdir -p build && cd build
cmake ..
make -j$(nproc)
cd ../../..
```

Or use the shortcut `make.sh` which does both steps and copies the built libraries:

```bash
./make.sh
```

---

## Minimal Working Example

After building, the following example demonstrates the basic flow on a cluster with RDMA-capable devices:

### On the memory node (server):

```bash
# Start the Fusa agent in server mode
#   threshold=80000  enable=enable  pcie_atomic=enable  workers=4
./providers/mlx5/fusioncas/build/agent/test_agent server 80000 enable enable 4

# In a separate shell, start the application server
./providers/mlx5/fusioncas/build/test/test_rdma server
```

### On each compute node (client):

```bash
# Start the Fusa agent in client mode
./providers/mlx5/fusioncas/build/agent/test_agent client

# Run the client benchmark (32 threads, depth 2)
./providers/mlx5/fusioncas/build/test/test_rdma client 32 2
```

The `test_rdma` benchmark performs concurrent RDMA CAS/FAA operations against the server. With the Fusa agent running, high-contention atomics are automatically offloaded to the server CPU while low-contention atomics execute directly on the RNIC.

### Running a pre-configured experiment

For YCSB-style evaluation with automated agent orchestration across machines:

```bash
# Requires passwordless SSH to compute nodes. Edit configs and machines in:
#   providers/mlx5/fusioncas/config/test_rdma.json
#   providers/mlx5/fusioncas/scripts/run_ycsb.py

cd providers/mlx5/fusioncas/scripts
python3 run_ycsb.py
```

---

## Verifying the Modifications

```bash
# Check what changed relative to the upstream baseline
git show 8f7b750:providers/mlx5/ > /tmp/baseline-mlx5.tar 2>/dev/null

# The key additions are:
#   providers/mlx5/recorder.{c,h}        — Contention recording (Bloom filter)
#   providers/mlx5/fusioncas/             — Full experiment framework
#   providers/mlx5/CMakeLists.txt          — Added recorder.c to the build
```

Modified upstream files in `providers/mlx5/`:

- `qp.c` — Post-send hook for contention recording and RPC dispatch
- `verbs.c` — Integration hooks
- `CMakeLists.txt` — Added `recorder.c` compilation unit

---

## Dependencies

Same as [upstream rdma-core](https://github.com/linux-rdma/rdma-core):

```bash
# Debian / Ubuntu
apt-get install build-essential cmake gcc libudev-dev libnl-3-dev \
                libnl-route-3-dev ninja-build pkg-config valgrind \
                python3-dev cython3 python3-docutils pandoc

# Fedora / CentOS 8
dnf builddep redhat/rdma-core.spec

# CentOS 7 / Amazon Linux 2
yum install cmake gcc libnl3-devel libudev-devel make pkgconfig valgrind-devel
```

## Acknowledgments

This repository includes source code derived from [rdma-core](https://github.com/linux-rdma/rdma-core) (commit `0ff4fc60a`) and [SMART-HT](https://github.com/madsys-dev/smart). We thank the original projects and their contributors for making their work open source.
