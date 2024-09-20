# UnrolledSkipList: Transition from Fine-Grained SkipList to Coarse-Grained Block 

## Introduction

Most of byte-addressable key-value stores designed for NVMM
leverage the in-place structural modification operations (SMOs)
to insert, update, or delete keys through 8-byte pointer updates.
While this in-place SMOs help mitigate the write amplification
problem, excessive in-place updates lead to decreased spatial locality, resulting in degraded read performance. In this study, we propose UnrolledSkipList for NVMM as an intermediate layer between
pointer-based SkipList and an array-based SSTables optimized for
block devices. UnrolledSkipList is a variant of SkipList that manages
multiple keys in a single node structure, combining the advantages
of SkipList and array. UnrolledSkipList aims to absorb SkipList
from DRAM into NVMM and quickly flush them as SSTables to a
block device. We develop a key-value store - UnrolledListDB using
UnrolledSkipList, which effectively reduces the amount of IOs by
using fine-grained pointer-based SMOs, while improving locality
by managing keys in array-based node structures. Through extensive performance study, we show that UnrolledListDB outperforms
other state-of-the-art LSM trees by a large margin.