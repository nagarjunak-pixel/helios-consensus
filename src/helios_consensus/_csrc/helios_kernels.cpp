// Native C++20 (-O3) Systems Acceleration Library for HeliosConsensus:
//   1. Slicing-by-8 Table-Driven Castagnoli CRC32C (0x82F63B78)
//   2. Lamping & Veach (2014) Jump Consistent Hash (scalar & batch)
//   3. Binary Segmented WAL Frame Encoder & Crash-Recovery Scanner
//   4. Wing & Gong + Lowe Bitmask Linearizability Solver for Concurrent Registers
//   5. Single & Joint-Consensus Quorum Commit Watermark Calculator
//   6. Self-Contained FIPS 180-4 SHA-256 & RFC-6962 Merkle Tree Root Engine

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <unordered_set>
#include <vector>

namespace {

// ============================================================================
// 1. Compile-Time Slicing-by-8 Castagnoli CRC32C Lookup Tables (0x82F63B78)
// ============================================================================
constexpr std::array<std::array<uint32_t, 256>, 8> make_crc32c_tables() {
    std::array<std::array<uint32_t, 256>, 8> tables{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) ? ((crc >> 1) ^ 0x82F63B78U) : (crc >> 1);
        }
        tables[0][i] = crc;
    }
    for (size_t slice = 1; slice < 8; ++slice) {
        for (size_t i = 0; i < 256; ++i) {
            const uint32_t prev = tables[slice - 1][i];
            tables[slice][i] = (prev >> 8) ^ tables[0][prev & 0xFFU];
        }
    }
    return tables;
}

constexpr auto kCrc32cTables = make_crc32c_tables();

inline uint32_t compute_crc32c(const uint8_t* data, size_t len) noexcept {
    uint32_t crc = 0xFFFFFFFFU;
    size_t i = 0;
    while (i + 8 <= len) {
        uint32_t lo = 0;
        uint32_t hi = 0;
        std::memcpy(&lo, data + i, 4);
        std::memcpy(&hi, data + i + 4, 4);
        lo ^= crc;
        crc = kCrc32cTables[7][lo & 0xFFU] ^
              kCrc32cTables[6][(lo >> 8) & 0xFFU] ^
              kCrc32cTables[5][(lo >> 16) & 0xFFU] ^
              kCrc32cTables[4][(lo >> 24) & 0xFFU] ^
              kCrc32cTables[3][hi & 0xFFU] ^
              kCrc32cTables[2][(hi >> 8) & 0xFFU] ^
              kCrc32cTables[1][(hi >> 16) & 0xFFU] ^
              kCrc32cTables[0][(hi >> 24) & 0xFFU];
        i += 8;
    }
    while (i < len) {
        crc = (crc >> 8) ^ kCrc32cTables[0][(crc ^ data[i]) & 0xFFU];
        ++i;
    }
    return ~crc;
}

// Little-endian serialization helpers (safe against unaligned buffers).
inline void write_u32_le(uint8_t* dst, uint32_t v) noexcept {
    dst[0] = static_cast<uint8_t>(v & 0xFFU);
    dst[1] = static_cast<uint8_t>((v >> 8) & 0xFFU);
    dst[2] = static_cast<uint8_t>((v >> 16) & 0xFFU);
    dst[3] = static_cast<uint8_t>((v >> 24) & 0xFFU);
}

inline void write_u64_le(uint8_t* dst, uint64_t v) noexcept {
    for (int i = 0; i < 8; ++i) {
        dst[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFFU);
    }
}

inline uint32_t read_u32_le(const uint8_t* src) noexcept {
    return static_cast<uint32_t>(src[0]) |
           (static_cast<uint32_t>(src[1]) << 8) |
           (static_cast<uint32_t>(src[2]) << 16) |
           (static_cast<uint32_t>(src[3]) << 24);
}

inline uint64_t read_u64_le(const uint8_t* src) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= (static_cast<uint64_t>(src[i]) << (i * 8));
    }
    return v;
}

// ============================================================================
// 2. Wing & Gong + Lowe Register Linearizability Helpers
// ============================================================================
constexpr int64_t kAbsentState = std::numeric_limits<int64_t>::min();

struct WglOp {
    int32_t slot;
    int32_t op_id;
    int32_t kind;        // 0 = READ, 1 = WRITE, 2 = CAS
    int64_t val;         // kAbsentState if None
    int64_t expected;    // kAbsentState if None
    int32_t ok_tri;      // -1 = None, 0 = False, 1 = True
    int64_t call_t;
    int64_t ret_t;
    bool has_ret;
};

inline bool step_reg(int64_t state, const WglOp& op, int64_t& next_state) noexcept {
    if (op.kind == 1) {  // WRITE
        next_state = op.val;
        return true;
    }
    if (op.kind == 0) {  // READ
        next_state = state;
        return op.val == state;
    }
    // CAS
    if (op.ok_tri < 0) {
        next_state = (state == op.expected) ? op.val : state;
        return true;
    }
    if (op.ok_tri == 1) {
        next_state = op.val;
        return state == op.expected;
    }
    next_state = state;
    return state != op.expected;
}

struct WglEvent {
    int32_t op_idx;
    bool is_call;
    bool is_inf;
    int64_t time;
    int32_t order;
    int32_t op_id;
    int32_t match_idx;
    int32_t prev_idx;
    int32_t next_idx;
};

struct StateKey {
    uint64_t mask;
    int64_t state;
    bool operator==(const StateKey& o) const noexcept {
        return mask == o.mask && state == o.state;
    }
};

struct StateKeyHasher {
    size_t operator()(const StateKey& k) const noexcept {
        uint64_t x = k.mask ^ (static_cast<uint64_t>(k.state) + 0x9E3779B97F4A7C15ULL);
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
        return static_cast<size_t>(x ^ (x >> 31));
    }
};

// ============================================================================
// 3. FIPS 180-4 Self-Contained SHA-256 Implementation
// ============================================================================
constexpr std::array<uint32_t, 64> kSha256K = {
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
    0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
    0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
    0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
    0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
    0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
    0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
    0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U, 0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U
};

inline uint32_t rotr32(uint32_t x, uint32_t n) noexcept {
    return (x >> n) | (x << (32U - n));
}

struct Sha256Ctx {
    uint32_t h[8] = {
        0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
        0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U
    };
    uint8_t block[64]{};
    size_t block_len = 0;
    uint64_t total_bits = 0;

    void compress(const uint8_t b[64]) noexcept {
        uint32_t w[64];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<uint32_t>(b[i * 4]) << 24) |
                   (static_cast<uint32_t>(b[i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(b[i * 4 + 2]) << 8) |
                   static_cast<uint32_t>(b[i * 4 + 3]);
        }
        for (int i = 16; i < 64; ++i) {
            const uint32_t s0 = rotr32(w[i - 15], 7) ^ rotr32(w[i - 15], 18) ^ (w[i - 15] >> 3);
            const uint32_t s1 = rotr32(w[i - 2], 17) ^ rotr32(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }
        uint32_t a = h[0], bv = h[1], c = h[2], d = h[3];
        uint32_t e = h[4], f = h[5], g = h[6], hv = h[7];
        for (int i = 0; i < 64; ++i) {
            const uint32_t s1 = rotr32(e, 6) ^ rotr32(e, 11) ^ rotr32(e, 25);
            const uint32_t ch = (e & f) ^ ((~e) & g);
            const uint32_t t1 = hv + s1 + ch + kSha256K[static_cast<size_t>(i)] + w[i];
            const uint32_t s0 = rotr32(a, 2) ^ rotr32(a, 13) ^ rotr32(a, 22);
            const uint32_t maj = (a & bv) ^ (a & c) ^ (bv & c);
            const uint32_t t2 = s0 + maj;
            hv = g;
            g = f;
            f = e;
            e = d + t1;
            d = c;
            c = bv;
            bv = a;
            a = t1 + t2;
        }
        h[0] += a;
        h[1] += bv;
        h[2] += c;
        h[3] += d;
        h[4] += e;
        h[5] += f;
        h[6] += g;
        h[7] += hv;
    }

    void update(const uint8_t* data, size_t len) noexcept {
        total_bits += static_cast<uint64_t>(len) * 8ULL;
        size_t i = 0;
        if (block_len > 0) {
            while (i < len && block_len < 64) {
                block[block_len++] = data[i++];
            }
            if (block_len == 64) {
                compress(block);
                block_len = 0;
            }
        }
        while (i + 64 <= len) {
            compress(data + i);
            i += 64;
        }
        while (i < len) {
            block[block_len++] = data[i++];
        }
    }

    void finalize(uint8_t out[32]) noexcept {
        block[block_len++] = 0x80U;
        if (block_len > 56) {
            while (block_len < 64) {
                block[block_len++] = 0;
            }
            compress(block);
            block_len = 0;
        }
        while (block_len < 56) {
            block[block_len++] = 0;
        }
        for (int i = 0; i < 8; ++i) {
            block[56 + i] = static_cast<uint8_t>((total_bits >> ((7 - i) * 8)) & 0xFFU);
        }
        compress(block);
        for (int i = 0; i < 8; ++i) {
            out[i * 4] = static_cast<uint8_t>((h[i] >> 24) & 0xFFU);
            out[i * 4 + 1] = static_cast<uint8_t>((h[i] >> 16) & 0xFFU);
            out[i * 4 + 2] = static_cast<uint8_t>((h[i] >> 8) & 0xFFU);
            out[i * 4 + 3] = static_cast<uint8_t>(h[i] & 0xFFU);
        }
    }
};

inline int64_t quorum_watermark(const int64_t* matches, int32_t n) {
    if (n <= 0 || matches == nullptr) {
        return 0;
    }
    std::vector<int64_t> sorted(matches, matches + n);
    std::sort(sorted.begin(), sorted.end(), std::greater<int64_t>());
    const int32_t q = (n / 2) + 1;
    return sorted[static_cast<size_t>(q - 1)];
}

}  // namespace

extern "C" {

// ----------------------------------------------------------------------------
// Jump Consistent Hash (Lamping & Veach, 2014)
// ----------------------------------------------------------------------------
int32_t helios_jump_consistent_hash(uint64_t key, int32_t num_buckets) {
    if (num_buckets <= 1) {
        return 0;
    }
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

// ----------------------------------------------------------------------------
// Slicing-by-8 Castagnoli CRC32C
// ----------------------------------------------------------------------------
uint32_t helios_crc32c(const uint8_t* __restrict__ data, int32_t length) {
    if (length <= 0 || data == nullptr) {
        return compute_crc32c(reinterpret_cast<const uint8_t*>(""), 0);
    }
    return compute_crc32c(data, static_cast<size_t>(length));
}

// ----------------------------------------------------------------------------
// Binary Segmented WAL Encoder & Recovery Scanner
// Header (25 bytes):
//   [0..4)   u32 payload_len
//   [4..8)   u32 crc32c over bytes [8 .. 25 + payload_len)
//   [8..16)  u64 index
//   [16..24) u64 term
//   [24]     u8  record_kind
//   [25..)   payload bytes
// ----------------------------------------------------------------------------
int32_t helios_wal_encode_record(
    uint64_t index,
    uint64_t term,
    uint8_t record_kind,
    const uint8_t* payload,
    int32_t payload_len,
    uint8_t* out_buf,
    int32_t out_cap
) {
    if (payload_len < 0 || out_buf == nullptr) {
        return -1;
    }
    const int64_t total = 25LL + static_cast<int64_t>(payload_len);
    if (total > static_cast<int64_t>(out_cap)) {
        return -1;
    }
    write_u32_le(out_buf, static_cast<uint32_t>(payload_len));
    write_u64_le(out_buf + 8, index);
    write_u64_le(out_buf + 16, term);
    out_buf[24] = record_kind;
    if (payload_len > 0 && payload != nullptr) {
        std::memcpy(out_buf + 25, payload, static_cast<size_t>(payload_len));
    }
    const uint32_t crc = compute_crc32c(out_buf + 8, static_cast<size_t>(17 + payload_len));
    write_u32_le(out_buf + 4, crc);
    return static_cast<int32_t>(total);
}

// stop_reason: 0 = EOF_CLEAN, 1 = TORN_HEADER, 2 = TORN_PAYLOAD, 3 = CRC_MISMATCH, 4 = OVERSIZED
int32_t helios_wal_scan_frames(
    const uint8_t* buf,
    int32_t buf_len,
    int32_t max_payload_len,
    int32_t max_frames,
    int32_t* out_payload_offsets,
    int32_t* out_payload_lengths,
    uint64_t* out_indices,
    uint64_t* out_terms,
    uint8_t* out_kinds,
    int32_t* out_valid_bytes,
    int32_t* out_stop_reason
) {
    if (out_valid_bytes != nullptr) {
        *out_valid_bytes = 0;
    }
    if (out_stop_reason != nullptr) {
        *out_stop_reason = 0;
    }
    if (buf_len <= 0 || buf == nullptr) {
        return 0;
    }

    int32_t pos = 0;
    int32_t count = 0;
    int32_t reason = 0;

    while (pos < buf_len) {
        if (buf_len - pos < 25) {
            reason = 1;  // TORN_HEADER
            break;
        }
        const uint32_t plen = read_u32_le(buf + pos);
        if (plen > static_cast<uint32_t>(max_payload_len)) {
            reason = 4;  // OVERSIZED
            break;
        }
        const int64_t frame_end = static_cast<int64_t>(pos) + 25LL + static_cast<int64_t>(plen);
        if (frame_end > static_cast<int64_t>(buf_len)) {
            reason = 2;  // TORN_PAYLOAD
            break;
        }
        const uint32_t expected_crc = read_u32_le(buf + pos + 4);
        const uint32_t actual_crc = compute_crc32c(buf + pos + 8, static_cast<size_t>(17U + plen));
        if (actual_crc != expected_crc) {
            reason = 3;  // CRC_MISMATCH
            break;
        }
        if (count < max_frames) {
            if (out_payload_offsets != nullptr) out_payload_offsets[count] = pos + 25;
            if (out_payload_lengths != nullptr) out_payload_lengths[count] = static_cast<int32_t>(plen);
            if (out_indices != nullptr) out_indices[count] = read_u64_le(buf + pos + 8);
            if (out_terms != nullptr) out_terms[count] = read_u64_le(buf + pos + 16);
            if (out_kinds != nullptr) out_kinds[count] = buf[pos + 24];
            ++count;
        } else {
            break;
        }
        pos = static_cast<int32_t>(frame_end);
    }

    if (out_valid_bytes != nullptr) {
        *out_valid_bytes = pos;
    }
    if (out_stop_reason != nullptr) {
        *out_stop_reason = reason;
    }
    return count;
}

// ----------------------------------------------------------------------------
// Wing & Gong + Lowe Bitmask Linearizability Checker (single register, N <= 62)
// Returns: 1 = linearizable, 0 = violation, 2 = budget_exhausted
// ----------------------------------------------------------------------------
int32_t helios_wgl_register(
    int32_t n,
    const int32_t* op_ids,
    const int32_t* kinds,
    const int8_t* has_val,
    const int64_t* vals,
    const int8_t* has_exp,
    const int64_t* exps,
    const int32_t* ok_tris,
    const int64_t* calls,
    const int8_t* has_ret,
    const int64_t* rets,
    int32_t budget,
    int32_t* out_explored
) {
    if (out_explored != nullptr) {
        *out_explored = 0;
    }
    if (n <= 0) {
        return 1;
    }
    if (n > 62) {
        return -1;  // caller falls back to arbitrary-width Python solver
    }

    std::vector<WglOp> ops(static_cast<size_t>(n));
    for (int32_t i = 0; i < n; ++i) {
        ops[static_cast<size_t>(i)] = WglOp{
            i,
            op_ids[i],
            kinds[i],
            has_val[i] ? vals[i] : kAbsentState,
            has_exp[i] ? exps[i] : kAbsentState,
            ok_tris[i],
            calls[i],
            rets[i],
            has_ret[i] != 0,
        };
    }

    // Node 0 is the dummy head; nodes 1..2n are sorted events.
    struct RawEv {
        int32_t op_idx;
        bool is_call;
        bool is_inf;
        int64_t time;
        int32_t order;
        int32_t op_id;
    };
    std::vector<RawEv> raw;
    raw.reserve(static_cast<size_t>(2 * n));
    for (int32_t i = 0; i < n; ++i) {
        const auto& op = ops[static_cast<size_t>(i)];
        raw.push_back(RawEv{i, true, false, op.call_t, 0, op.op_id});
        raw.push_back(RawEv{i, false, !op.has_ret, op.ret_t, 1, op.op_id});
    }
    std::sort(raw.begin(), raw.end(), [](const RawEv& a, const RawEv& b) {
        if (a.is_inf != b.is_inf) return !a.is_inf;
        if (!a.is_inf && a.time != b.time) return a.time < b.time;
        if (a.order != b.order) return a.order < b.order;
        return a.op_id < b.op_id;
    });

    std::vector<WglEvent> ev(static_cast<size_t>(2 * n + 1));
    ev[0] = WglEvent{-1, false, false, 0, -1, -1, -1, -1, 1};
    std::vector<int32_t> call_node_of(static_cast<size_t>(n), -1);
    std::vector<int32_t> ret_node_of(static_cast<size_t>(n), -1);

    for (int32_t k = 0; k < 2 * n; ++k) {
        const int32_t node_id = k + 1;
        const auto& r = raw[static_cast<size_t>(k)];
        ev[static_cast<size_t>(node_id)] = WglEvent{
            r.op_idx,
            r.is_call,
            r.is_inf,
            r.time,
            r.order,
            r.op_id,
            -1,
            k,
            (k + 1 < 2 * n) ? (node_id + 1) : -1,
        };
        if (r.is_call) {
            call_node_of[static_cast<size_t>(r.op_idx)] = node_id;
        } else {
            ret_node_of[static_cast<size_t>(r.op_idx)] = node_id;
        }
    }
    for (int32_t i = 0; i < n; ++i) {
        const int32_t c_id = call_node_of[static_cast<size_t>(i)];
        const int32_t r_id = ret_node_of[static_cast<size_t>(i)];
        ev[static_cast<size_t>(c_id)].match_idx = r_id;
        ev[static_cast<size_t>(r_id)].match_idx = c_id;
    }

    auto lift = [&](int32_t c_idx) {
        auto& c = ev[static_cast<size_t>(c_idx)];
        ev[static_cast<size_t>(c.prev_idx)].next_idx = c.next_idx;
        if (c.next_idx != -1) {
            ev[static_cast<size_t>(c.next_idx)].prev_idx = c.prev_idx;
        }
        auto& r = ev[static_cast<size_t>(c.match_idx)];
        ev[static_cast<size_t>(r.prev_idx)].next_idx = r.next_idx;
        if (r.next_idx != -1) {
            ev[static_cast<size_t>(r.next_idx)].prev_idx = r.prev_idx;
        }
    };

    auto unlift = [&](int32_t c_idx) {
        auto& c = ev[static_cast<size_t>(c_idx)];
        auto& r = ev[static_cast<size_t>(c.match_idx)];
        ev[static_cast<size_t>(r.prev_idx)].next_idx = c.match_idx;
        if (r.next_idx != -1) {
            ev[static_cast<size_t>(r.next_idx)].prev_idx = c.match_idx;
        }
        ev[static_cast<size_t>(c.prev_idx)].next_idx = c_idx;
        if (c.next_idx != -1) {
            ev[static_cast<size_t>(c.next_idx)].prev_idx = c_idx;
        }
    };

    int64_t state = kAbsentState;
    uint64_t linearized = 0ULL;
    std::unordered_set<StateKey, StateKeyHasher> cache;
    cache.reserve(1024);

    struct Frame {
        int32_t call_idx;
        int64_t prev_state;
    };
    std::vector<Frame> stack;
    stack.reserve(static_cast<size_t>(n));

    int32_t entry = ev[0].next_idx;
    int32_t explored = 0;

    while (ev[0].next_idx != -1) {
        if (explored >= budget) {
            if (out_explored != nullptr) *out_explored = explored;
            return 2;
        }
        const auto& cur = ev[static_cast<size_t>(entry)];
        if (cur.is_call) {
            int64_t next_state = state;
            if (step_reg(state, ops[static_cast<size_t>(cur.op_idx)], next_state)) {
                const uint64_t bit = 1ULL << static_cast<uint32_t>(cur.op_idx);
                const StateKey key{linearized | bit, next_state};
                if (cache.find(key) == cache.end()) {
                    ++explored;
                    cache.insert(key);
                    stack.push_back(Frame{entry, state});
                    state = next_state;
                    linearized |= bit;
                    lift(entry);
                    entry = ev[0].next_idx;
                    continue;
                }
            }
            entry = cur.next_idx;
        } else {
            if (stack.empty()) {
                if (out_explored != nullptr) *out_explored = explored;
                return 0;
            }
            const Frame top = stack.back();
            stack.pop_back();
            const int32_t op_i = ev[static_cast<size_t>(top.call_idx)].op_idx;
            linearized &= ~(1ULL << static_cast<uint32_t>(op_i));
            state = top.prev_state;
            unlift(top.call_idx);
            entry = ev[static_cast<size_t>(top.call_idx)].next_idx;
        }
    }

    if (out_explored != nullptr) {
        *out_explored = explored;
    }
    return 1;
}

// ----------------------------------------------------------------------------
// Joint-Consensus Quorum Commit Watermark
// ----------------------------------------------------------------------------
int64_t helios_joint_commit_index(
    const int64_t* primary_matches,
    int32_t primary_len,
    const int64_t* secondary_matches,
    int32_t secondary_len
) {
    const int64_t w1 = quorum_watermark(primary_matches, primary_len);
    if (secondary_len <= 0 || secondary_matches == nullptr) {
        return w1;
    }
    const int64_t w2 = quorum_watermark(secondary_matches, secondary_len);
    return std::min<int64_t>(w1, w2);
}

// ----------------------------------------------------------------------------
// SHA-256 & RFC-6962 Binary Merkle Tree Root
// ----------------------------------------------------------------------------
void helios_sha256(const uint8_t* data, int32_t len, uint8_t* out32) {
    Sha256Ctx ctx;
    if (len > 0 && data != nullptr) {
        ctx.update(data, static_cast<size_t>(len));
    }
    ctx.finalize(out32);
}

void helios_merkle_root_rfc6962(
    const uint8_t* concat_leaves,
    const int32_t* leaf_offsets,
    const int32_t* leaf_lengths,
    int32_t leaf_count,
    uint8_t* out32
) {
    if (leaf_count <= 0) {
        Sha256Ctx empty_ctx;
        empty_ctx.finalize(out32);
        return;
    }

    std::vector<std::array<uint8_t, 32>> level(static_cast<size_t>(leaf_count));
    for (int32_t i = 0; i < leaf_count; ++i) {
        Sha256Ctx ctx;
        const uint8_t zero = 0x00U;
        ctx.update(&zero, 1);
        const int32_t off = leaf_offsets[i];
        const int32_t ln = leaf_lengths[i];
        if (ln > 0 && concat_leaves != nullptr) {
            ctx.update(concat_leaves + off, static_cast<size_t>(ln));
        }
        ctx.finalize(level[static_cast<size_t>(i)].data());
    }

    while (level.size() > 1) {
        std::vector<std::array<uint8_t, 32>> next;
        next.reserve((level.size() + 1) / 2);
        for (size_t i = 0; i < level.size(); i += 2) {
            if (i + 1 < level.size()) {
                Sha256Ctx ctx;
                const uint8_t one = 0x01U;
                ctx.update(&one, 1);
                ctx.update(level[i].data(), 32);
                ctx.update(level[i + 1].data(), 32);
                std::array<uint8_t, 32> parent{};
                ctx.finalize(parent.data());
                next.push_back(parent);
            } else {
                next.push_back(level[i]);
            }
        }
        level = std::move(next);
    }

    std::memcpy(out32, level[0].data(), 32);
}

}  // extern "C"
