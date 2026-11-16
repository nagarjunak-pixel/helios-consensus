// Native C++20 (-O3) Google Jump Consistent Hash (Lamping & Veach 2014) &
// Castagnoli CRC32C (0x82F63B78) Write-Ahead Log Frame Integrity Kernel for HeliosConsensus.

#include <cstddef>
#include <cstdint>

extern "C" {

// Exact Lamping & Veach (2014) Jump Consistent Hash: maps 64-bit key -> bucket in [0, num_buckets).
int32_t helios_jump_consistent_hash(uint64_t key, int32_t num_buckets) {
    int64_t b = -1;
    int64_t j = 0;
    while (j < static_cast<int64_t>(num_buckets)) {
        b = j;
        key = key * 2862933555777941757ULL + 1ULL;
        j = static_cast<int64_t>(
            static_cast<double>(b + 1) *
            (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1ULL))
        );
    }
    return static_cast<int32_t>(b);
}

// Batch Jump Consistent Hash for high-throughput shard placement & minimal-migration proofs.
void helios_jump_hash_batch(
    const uint64_t* __restrict__ keys,
    int32_t count,
    int32_t num_buckets,
    int32_t* __restrict__ buckets_out
) {
    for (int32_t i = 0; i < count; ++i) {
        buckets_out[i] = helios_jump_consistent_hash(keys[i], num_buckets);
    }
}

// Castagnoli CRC32C (reflected polynomial 0x82F63B78) used by RocksDB, Spanner & Raft WAL frames.
uint32_t helios_crc32c(const uint8_t* __restrict__ data, int32_t length) {
    uint32_t crc = 0xFFFFFFFFU;
    for (int32_t i = 0; i < length; ++i) {
        crc ^= static_cast<uint32_t>(data[i]);
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = -(crc & 1U);
            crc = (crc >> 1) ^ (0x82F63B78U & mask);
        }
    }
    return ~crc;
}

}  // extern "C"
