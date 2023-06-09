/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "indexlib/index/common/hash_table/CompactSpecialKeyBucket.h"
#include "indexlib/index/common/hash_table/SpecialKeyBucket.h"
#include "indexlib/index/common/hash_table/SpecialValue.h"
#include "indexlib/index/common/hash_table/SpecialValueBucket.h"

namespace indexlibv2::index {

// for normal
template <typename _KT, typename _VT, bool useCompactBucket>
struct ClosedHashTableTraits {
    static const bool HasSpecialKey = true;
    static const bool UseCompactBucket = false;
    typedef SpecialKeyBucket<_KT, _VT> Bucket;
    typedef typename Bucket::SpecialBucket SpecialBucket;
};

template <typename _KT, typename _VT>
struct ClosedHashTableTraits<_KT, _VT, true> {
    static const bool HasSpecialKey = true;
    static const bool UseCompactBucket = true;
    typedef CompactSpecialKeyBucket<_KT, _VT> Bucket;
    typedef typename Bucket::SpecialBucket SpecialBucket;
};

// for timestamp value
template <typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, TimestampValue<_RealVT>, false> {
    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef TimestampValue<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};

// for offset value
template <typename _KT, typename _RealVT, _RealVT EmptyValue, _RealVT DeleteValue>
struct ClosedHashTableTraits<_KT, SpecialValue<_RealVT, EmptyValue, DeleteValue>, false> {
    static_assert(sizeof(_RealVT) == 4 || sizeof(_RealVT) == 8, "to guarantee concurrent read and write");

    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef SpecialValue<_RealVT, EmptyValue, DeleteValue> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};

// for short offset value
template <typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, OffsetValue<_RealVT>, false> {
    static_assert(sizeof(_RealVT) == 4 || sizeof(_RealVT) == 8, "to guarantee concurrent read and write");

    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef OffsetValue<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};
} // namespace indexlibv2::index
