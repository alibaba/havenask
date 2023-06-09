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
#include "indexlib/framework/Locator.h"
#include "indexlib/index/kv/KVCommonDefine.h"

namespace autil {
class CacheAllocator;
using CacheAllocatorPtr = std::shared_ptr<CacheAllocator>;
} // namespace autil

namespace indexlibv2::table {

class KVCacheItem
{
public:
    size_t Size() const { return sizeof(KVCacheItem) + _value.size(); }
    bool HasValue() const { return !_value.empty(); }
    void SetHasValue(bool hasValue)
    {
        if (!hasValue) {
            _value = autil::StringView::empty_instance();
        }
    }
    const std::shared_ptr<framework::Locator>& GetLocator() const { return _locator; }
    uint32_t GetTimestamp() const { return _timestamp; }
    const autil::StringView& GetValue() const { return _value; }

    static KVCacheItem* Create(const std::shared_ptr<framework::Locator>& locator, uint32_t timestamp,
                               const autil::StringView& value)
    {
        KVCacheItem* ret = new KVCacheItem();
        ret->_locator = locator;
        ret->_timestamp = timestamp;
        if (value.size() > 0) {
            char* newValue = new char[value.size()];
            memcpy(newValue, value.data(), value.size());
            ret->_value = autil::StringView(newValue, value.size());
        }
        return ret;
    }
    static void Deleter(const autil::StringView& key, void* value, const autil::CacheAllocatorPtr& allocator)
    {
        if (value) {
            KVCacheItem* cacheItem = (KVCacheItem*)value;
            if (cacheItem->_value.size() > 0) {
                delete[](char*) cacheItem->_value.data();
                cacheItem->_value = autil::StringView::empty_instance();
            }
            delete cacheItem;
        }
    }

private:
    std::shared_ptr<framework::Locator> _locator;
    uint32_t _timestamp = 0;
    autil::StringView _value;
};

} // namespace indexlibv2::table
