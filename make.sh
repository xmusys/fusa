#!/bin/bash

rm -rf build
mkdir build

./build.sh

cp ./build/lib/libmlx5.so.1.25.56.0 ./providers/mlx5/fusioncas/
cp ./build/lib/libibverbs.so.1.14.56.0 ./providers/mlx5/fusioncas/

mv ./providers/mlx5/fusioncas/libmlx5.so.1.25.56.0 ./providers/mlx5/fusioncas/libmlx5.so
mv ./providers/mlx5/fusioncas/libibverbs.so.1.14.56.0 ./providers/mlx5/fusioncas/libibverbs.so

cp ./providers/mlx5/fusioncas/libmlx5.so ./providers/mlx5/fusioncas/build/
cp ./providers/mlx5/fusioncas/libibverbs.so ./providers/mlx5/fusioncas/build/