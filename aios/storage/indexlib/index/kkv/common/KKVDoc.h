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

#include "autil/StringView.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/ChunkDefine.h"

namespace indexlibv2::index {

// TODO(qisa.cb) 1. KKVDoc should be template for skey type; 2. POD
struct KKVDoc {
    union {
        autil::StringView value; // 128
        ValueOffset valueOffset; // 48
        uint64_t offset;         // 8
    };
    uint64_t skey = 0;      // 8
    uint32_t timestamp = 0; // 4
    uint32_t expireTime;
    bool skeyDeleted = false;
    bool duplicatedKey = false;
    bool inCache = false;

    KKVDoc() : KKVDoc(0, 0, 0) {}

    KKVDoc(uint64_t skey, uint32_t ts) : KKVDoc(skey, ts, 0) {}

    KKVDoc(uint64_t skey, uint32_t ts, uint64_t offset)
        : offset(offset)
        , skey(skey)
        , timestamp(ts)
        , expireTime(indexlib::UNINITIALIZED_EXPIRE_TIME)
        , skeyDeleted(false)
        , duplicatedKey(false)
        , inCache(false)
    {
    }

    KKVDoc(uint64_t skey, uint32_t ts, ValueOffset valueOffset)
        : KKVDoc(skey, ts, indexlib::UNINITIALIZED_EXPIRE_TIME, false, valueOffset)
    {
    }

    KKVDoc(uint64_t skey, uint32_t ts, const autil::StringView& value)
        : KKVDoc(skey, ts, indexlib::UNINITIALIZED_EXPIRE_TIME, false, value)
    {
    }

    KKVDoc(uint64_t skey, uint32_t ts, uint32_t expireTime, bool skeyDeleted, ValueOffset valueOffset)
        : valueOffset(valueOffset)
        , skey(skey)
        , timestamp(ts)
        , expireTime(expireTime)
        , skeyDeleted(skeyDeleted)
        , duplicatedKey(false)
        , inCache(false)
    {
    }

    KKVDoc(uint64_t skey, uint32_t ts, uint32_t expireTime, bool skeyDeleted, const autil::StringView& value)
        : value(value)
        , skey(skey)
        , timestamp(ts)
        , expireTime(expireTime)
        , skeyDeleted(skeyDeleted)
        , duplicatedKey(false)
        , inCache(false)
    {
    }

    KKVDoc(const KKVDoc& rhs)
        : value(rhs.value)
        , skey(rhs.skey)
        , timestamp(rhs.timestamp)
        , expireTime(rhs.expireTime)
        , skeyDeleted(rhs.skeyDeleted)
        , duplicatedKey(rhs.duplicatedKey)
        , inCache(rhs.inCache)
    {
    }

    ~KKVDoc() {}

    KKVDoc& operator=(const KKVDoc& rhs)
    {
        if (this != &rhs) {
            value = rhs.value;
            skey = rhs.skey;
            timestamp = rhs.timestamp;
            expireTime = rhs.expireTime;
            skeyDeleted = rhs.skeyDeleted;
            duplicatedKey = rhs.duplicatedKey;
            inCache = rhs.inCache;
        }
        return *this;
    }
    void SetValueOffset(ValueOffset offset) { valueOffset = offset; }
    void SetValue(autil::StringView v) { value = v; }
};

} // namespace indexlibv2::index
