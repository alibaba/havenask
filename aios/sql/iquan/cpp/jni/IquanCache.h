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
#include <vector>


#include "autil/Log.h"
#include "autil/LruCache.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/common/catalog/LayerTablePlanMetaDef.h"

namespace iquan {

template<typename T>
struct CacheValueInfo {
public:
    using ValueType = T;
    int64_t updateTimeUs;
    int64_t sizeInBytes;
    ValueType value;

    CacheValueInfo() : updateTimeUs(0), sizeInBytes(1) {}

    CacheValueInfo(int64_t updateTimeUs, int64_t sizeInBytes, const ValueType &value)
        : updateTimeUs(updateTimeUs), sizeInBytes(sizeInBytes), value(value) {}

    CacheValueInfo(const CacheValueInfo &other) {
        updateTimeUs = other.updateTimeUs;
        sizeInBytes = other.sizeInBytes;
        value = other.value;
    }

    CacheValueInfo &operator=(const CacheValueInfo &other) {
        if (this == &other) {
            return *this;
        }
        updateTimeUs = other.updateTimeUs;
        sizeInBytes = other.sizeInBytes;
        value = other.value;
        return *this;
    }

    inline int64_t size() const { return sizeInBytes; }
};

template <typename T>
class IquanCache {
public:
    using CacheValueInfoTyped = CacheValueInfo<T>;
    class CacheValueGetSizeCallBack {
    public:
        int64_t operator()(const CacheValueInfoTyped &value) { return value.size(); }
    };
    using LruCacheType = autil::LruCache<uint64_t, CacheValueInfoTyped, CacheValueGetSizeCallBack>;

public:
    IquanCache(const CacheConfig &cacheConfig);
    ~IquanCache();

    IquanCache(const IquanCache<T> &) = delete;
    IquanCache &operator=(const IquanCache<T> &) = delete;

public:
    Status reset();
    uint64_t keyCount() const;
    int64_t capcaity() const;
    int64_t size() const;
    Status put(
        const std::string &key,
        int64_t size,
        const typename CacheValueInfoTyped::ValueType &value,
        uint64_t *pHashKey = nullptr);
    Status get(
        const std::string &key,
        typename CacheValueInfoTyped::ValueType &value,
        uint64_t *pHashKey = nullptr);
    Status calcHashKey(const std::string &key, uint64_t &keyHashValue);

private:
    std::unique_ptr<LruCacheType> cachePtr;
    int64_t maxSizeInBytes;
    int64_t expireTimeUs;
};

} // namespace iquan
