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
#include <memory>

#include "indexlib/base/Types.h"
#include "indexlib/index/common/hash_table/BucketCompressor.h"
#include "indexlib/index/common/hash_table/SpecialKeyBucket.h"
#include "indexlib/index/common/hash_table/SpecialValue.h"
#include "indexlib/index/common/hash_table/SpecialValueBucket.h"

namespace indexlibv2::index {

template <typename Bucket>
class BucketOffsetCompressor : public BucketCompressor
{
public:
    BucketOffsetCompressor() {};
    ~BucketOffsetCompressor() override {};

public:
    size_t Compress(char* in, size_t bucketSize, char* out) override
    {
        // bydefault do nothing
        return bucketSize;
    }
};

template <typename _KT, typename _OFFSET>
class BucketOffsetCompressor<SpecialValueBucket<_KT, TimestampValue<_OFFSET>>> : public BucketCompressor
{
public:
    BucketOffsetCompressor() {};
    ~BucketOffsetCompressor() override {};

public:
    size_t Compress(char* in, size_t bucketSize, char* out) override
    {
        typedef SpecialValueBucket<_KT, TimestampValue<_OFFSET>> LongBucket;
        typedef TimestampValue<short_offset_t> ShortValue;
        typedef SpecialValueBucket<_KT, ShortValue> ShortBucket;
        LongBucket* inBucket = static_cast<LongBucket*>((void*)in);
        ShortBucket outBucket;
        if (inBucket->IsEmpty()) {
            outBucket.SetEmpty();
        } else if (inBucket->IsDeleted()) {
            outBucket.SetDelete(inBucket->Key(), ShortValue(inBucket->Value().Timestamp(),
                                                            static_cast<short_offset_t>(inBucket->Value().Value())));
        } else {
            outBucket.Set(inBucket->Key(), ShortValue(inBucket->Value().Timestamp(),
                                                      static_cast<short_offset_t>(inBucket->Value().Value())));
        }
        *static_cast<ShortBucket*>((void*)out) = outBucket;
        return sizeof(ShortBucket);
    }
};

template <typename _KT, typename _OFFSET>
class BucketOffsetCompressor<SpecialValueBucket<_KT, OffsetValue<_OFFSET>>> : public BucketCompressor
{
public:
    BucketOffsetCompressor() {};
    ~BucketOffsetCompressor() override {};

public:
    size_t Compress(char* in, size_t bucketSize, char* out) override
    {
        typedef SpecialValueBucket<_KT, OffsetValue<_OFFSET>> LongBucket;
        typedef OffsetValue<short_offset_t> ShortValue;
        typedef SpecialValueBucket<_KT, ShortValue> ShortBucket;
        LongBucket* inBucket = static_cast<LongBucket*>((void*)in);
        ShortBucket outBucket;
        if (inBucket->IsEmpty()) {
            outBucket.SetEmpty();
        } else if (inBucket->IsDeleted()) {
            outBucket.SetDelete(inBucket->Key(), ShortValue(static_cast<short_offset_t>(inBucket->Value().Value())));
        } else {
            outBucket.Set(inBucket->Key(), ShortValue(static_cast<short_offset_t>(inBucket->Value().Value())));
        }
        *static_cast<ShortBucket*>((void*)out) = outBucket;
        return sizeof(ShortBucket);
    }
};

} // namespace indexlibv2::index
