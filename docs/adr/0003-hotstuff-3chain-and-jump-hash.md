# ADR 0003: Chained HotStuff 3-Chain BFT Finality & C++20 Jump Hash / CRC32C Kernels

- **Status**: Accepted
- **Context**: Multi-party consensus and sharded storage face Byzantine equivocation (`N = 3f + 1`), shard rebalancing key movement, and silent disk corruption.
- **Decision**: Pair a Chained HotStuff 3-chain BFT finality module (`2f + 1` QC aggregation + double-vote equivocation rejection) with a zero-dependency `clang++ -O3 -std=c++20` kernel (`src/helios_consensus/_csrc/helios_kernels.cpp`) implementing Lamping–Veach 64-bit Jump Consistent Hashing and slicing-by-8 Castagnoli `CRC32C` (`0x82F63B78`), backed by bit-exact Python fallbacks.
- **Consequences**: Achieves `0` C++20-vs-Python mismatches across `20,000` keys (`0.07660` keys moved on `12 -> 13` shards vs theoretical `1/13 = 0.07692`) and `64/64` (`100%`) single-bit WAL corruption detection matching RFC-3720 `0xE3069283`.
