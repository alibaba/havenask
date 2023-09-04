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
#include "iquan/jni/IquanCache.h"

#include "autil/HashAlgorithm.h"
#include "autil/TimeUtility.h"
#include "iquan/common/catalog/PlanMeta.h"

namespace iquan {

template <typename T>
IquanCache<T>::IquanCache(const CacheConfig &cacheConfig)
    : maxSizeInBytes(cacheConfig.maxSize) {}

template <typename T>
IquanCache<T>::~IquanCache() {
    if (likely(cachePtr != nullptr)) {
        cachePtr.reset();
    }
}

template <typename T>
Status IquanCache<T>::reset() {
    cachePtr.reset(new LruCacheType(maxSizeInBytes));
    return Status::OK();
}

template <typename T>
uint64_t IquanCache<T>::keyCount() const {
    return cachePtr->getKeyCount();
}

template <typename T>
int64_t IquanCache<T>::capcaity() const {
    return cachePtr->getCacheSize();
}

template <typename T>
int64_t IquanCache<T>::size() const {
    return cachePtr->getCacheSizeUsed();
}

template <typename T>
Status IquanCache<T>::calcHashKey(const std::string &key, uint64_t &keyHashValue) {
    keyHashValue = autil::HashAlgorithm::hashString64(key.c_str(), key.size());
    return Status::OK();
}

template <typename T>
Status IquanCache<T>::put(const std::string &key,
                          int64_t size,
                          const typename CacheValueInfoTyped::ValueType &value,
                          uint64_t *pHashKey) {
    if (unlikely(cachePtr == nullptr)) {
        return Status(IQUAN_FAIL, "cache is not reset");
    }
    uint64_t hashKey = 0;
    IQUAN_ENSURE_FUNC(calcHashKey(key, hashKey));
    int64_t currentTimeUs = autil::TimeUtility::currentTime();
    CacheValueInfoTyped cacheInfo(currentTimeUs, size, value);
    bool ret = cachePtr->put(hashKey, cacheInfo);
    if (unlikely(!ret)) {
        return Status(IQUAN_FAIL, "put key to cache fail");
    }
    if (pHashKey) {
        *pHashKey = hashKey;
    }
    return Status::OK();
}

template <typename T>
Status IquanCache<T>::get(const std::string &key,
                          typename CacheValueInfoTyped::ValueType &value,
                          uint64_t *pHashKey) {
    if (unlikely(cachePtr == nullptr)) {
        return Status(IQUAN_FAIL, "cache is not reset");
    }
    uint64_t hashKey = 0;
    IQUAN_ENSURE_FUNC(calcHashKey(key, hashKey));
    CacheValueInfoTyped cacheInfo;
    bool ret = cachePtr->get(hashKey, cacheInfo);
    if (unlikely(!ret)) {
        return Status(IQUAN_CACHE_KEY_NOT_FOUND, "get key from cache fail");
    }
    value = cacheInfo.value;
    if (pHashKey) {
        *pHashKey = hashKey;
    }
    return Status::OK();
}

template class IquanCache<std::shared_ptr<autil::legacy::RapidDocument>>;
template class IquanCache<std::shared_ptr<PlanMeta>>;

} // namespace iquan
