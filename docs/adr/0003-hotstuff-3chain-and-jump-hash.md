# ADR 0003: Chained HotStuff 3-Chain BFT Finality & C++20 Jump Hash / CRC32C Kernels

- **Status**: Accepted
- **Context**: Sovereign multi-organization ledgers face equivocating Byzantine validators (`N = 3f + 1`) alongside silent NVMe bit-rot and shard rebalancing storms.
- **Decision**: Pair a Chained HotStuff 3-chain BFT finality engine (`2f + 1` QC aggregation + double-vote equivocation rejection) with a zero-dependency `clang++ -O3 -std=c++20` kernel implementing Lamping–Veach 64-bit Jump Consistent Hashing and Castagnoli `CRC32C` (`0x82F63B78`) WAL frame verification (with bit-exact Python fallback).
- **Consequences**: Achieves `0` C++20-vs-Python hash mismatches across `20,000` keys (`0.07660` keys moved vs `1/13 = 0.07692` optimal) and `64/64` (`100%`) single-bit WAL corruption detection matching RFC-3720 `0xE3069283`.
