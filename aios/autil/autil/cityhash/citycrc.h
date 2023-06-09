#pragma once
// Copyright (c) 2011 Google, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// CityHash, by Geoff Pike and Jyrki Alakuijala
//
// This file declares the subset of the CityHash functions that require
// _mm_crc32_u64().  See the CityHash README for details.
//
// Functions in the CityHash family are not suitable for cryptography.

#include <tmmintrin.h>
#include <stddef.h>
#include "autil/cityhash/city.h"

#ifdef __x86_64__
/// Adds the unsigned integer operand to the CRC-32C checksum of the
///    unsigned 64-bit integer operand.
///
/// \headerfile <x86intrin.h>
///
/// This intrinsic corresponds to the <c> CRC32Q </c> instruction.
///
/// \param __C
///    An unsigned integer operand to add to the CRC-32C checksum of operand
///    \a __D.
/// \param __D
///    An unsigned 64-bit integer operand used to compute the CRC-32C checksum.
/// \returns The result of adding operand \a __C to the CRC-32C checksum of
///    operand \a __D.
static __inline__ unsigned long long _mm_crc32_u64(unsigned long long __C, unsigned long long __D)
{
    return __builtin_ia32_crc32di(__C, __D);
}
#endif /* __x86_64__ */

namespace autil {

// Hash function for a byte array.
uint128 CityHashCrc128(const char *s, size_t len);

// Hash function for a byte array.  For convenience, a 128-bit seed is also
// hashed into the result.
uint128 CityHashCrc128WithSeed(const char *s, size_t len, uint128 seed);

// Hash function for a byte array.  Sets result[0] ... result[3].
void CityHashCrc256(const char *s, size_t len, uint64 *result);

}
