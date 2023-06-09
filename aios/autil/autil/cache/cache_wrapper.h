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

#include "autil/cache/cache.h"
#include "autil/Span.h"

namespace autil {

class CacheItemRet {
public:
    CacheItemRet(CacheBase::Handle* handle, CacheBase *cache)
        : _handle(handle)
        , _cache(cache)
    {}

    ~CacheItemRet() {
        if (_handle != nullptr) {
            _cache->Release(_handle);
            _handle = nullptr;
        }
    }

public:
    operator bool() const {
        return _handle != nullptr;
    }

    template <typename CacheItem>
    CacheItem* GetCacheItem() const {
        return _handle != nullptr ? reinterpret_cast<CacheItem*>(_cache->Value(_handle)) : nullptr;
    }

private:
    CacheBase::Handle *_handle;
    CacheBase *_cache;
};

class CacheWrapper {
public:
    explicit CacheWrapper(const std::shared_ptr<CacheBase> &cache);

public:
    template <typename CacheItem>
    bool put(const autil::StringView &key, CacheItem *cacheItem) {
        return _internal->Insert(key, cacheItem, cacheItem->Size(), CacheItem::Deleter);
    }

    CacheItemRet get(const autil::StringView &key) {
        auto handle = _internal->Lookup(key);
        return CacheItemRet{handle, _internal.get()};
    }

    void invalidate(const autil::StringView &key) {
        return _internal->Erase(key);
    }

    CacheBase *getImpl() const {
        return _internal.get();
    }

private:
    std::shared_ptr<CacheBase> _internal;
};

}
