// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace autil {
class Coding {
public:
    Coding() = default;
    ~Coding() = default;

    // Standard Put... routines append to a string
    static void PutFixed32(std::string *dst, uint32_t value);
    static void PutFixed64(std::string *dst, uint64_t value);
    static void PutVarint32(std::string *dst, uint32_t value);
    static void PutVarint64(std::string *dst, uint64_t value);

    // Pointer-based variants of GetVarint...  These either store a value
    // in *v and return a pointer just past the parsed value, or return
    // nullptr on error.  These routines only look at bytes in the range
    // [p..limit-1]
    static const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *v);
    static const char *GetVarint64Ptr(const char *p, const char *limit, uint64_t *v);

    // Returns the length of the varint32 or varint64 encoding of "v"
    static int VarintLength(uint64_t v);

    // Lower-level versions of Put... that write directly into a character buffer
    // and return a pointer just past the last byte written.
    // REQUIRES: dst has enough space for the value being written
    static char *EncodeVarint32(char *dst, uint32_t value);
    static char *EncodeVarint64(char *dst, uint64_t value);

    // Lower-level versions of Put... that write directly into a character buffer
    // REQUIRES: dst has enough space for the value being written

    static inline void EncodeFixed32(char *dst, uint32_t value) {
        uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

        // Recent clang and gcc optimize this to a single mov / str instruction.
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
    }

    static inline void EncodeFixed64(char *dst, uint64_t value) {
        uint8_t *const buffer = reinterpret_cast<uint8_t *>(dst);

        // Recent clang and gcc optimize this to a single mov / str instruction.
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        buffer[4] = static_cast<uint8_t>(value >> 32);
        buffer[5] = static_cast<uint8_t>(value >> 40);
        buffer[6] = static_cast<uint8_t>(value >> 48);
        buffer[7] = static_cast<uint8_t>(value >> 56);
    }

    // Lower-level versions of Get... that read directly from a character buffer
    // without any bounds checking.

    static inline uint32_t DecodeFixed32(const char *ptr) {
        const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);

        // Recent clang and gcc optimize this to a single mov / ldr instruction.
        return (static_cast<uint32_t>(buffer[0])) | (static_cast<uint32_t>(buffer[1]) << 8) |
               (static_cast<uint32_t>(buffer[2]) << 16) | (static_cast<uint32_t>(buffer[3]) << 24);
    }

    static inline uint64_t DecodeFixed64(const char *ptr) {
        const uint8_t *const buffer = reinterpret_cast<const uint8_t *>(ptr);

        // Recent clang and gcc optimize this to a single mov / ldr instruction.
        return (static_cast<uint64_t>(buffer[0])) | (static_cast<uint64_t>(buffer[1]) << 8) |
               (static_cast<uint64_t>(buffer[2]) << 16) | (static_cast<uint64_t>(buffer[3]) << 24) |
               (static_cast<uint64_t>(buffer[4]) << 32) | (static_cast<uint64_t>(buffer[5]) << 40) |
               (static_cast<uint64_t>(buffer[6]) << 48) | (static_cast<uint64_t>(buffer[7]) << 56);
    }

    // Internal routine for use by fallback path of GetVarint32Ptr
    static const char *GetVarint32PtrFallback(const char *p, const char *limit, uint32_t *value);
};
} // namespace autil
