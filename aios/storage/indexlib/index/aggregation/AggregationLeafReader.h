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

#include "autil/Log.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/ISegmentReader.h"
#include "indexlib/index/aggregation/IndexMeta.h"
#include "indexlib/index/aggregation/KeyMeta.h"

namespace indexlib::file_system {
class IDirectory;
class Directory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class AggregationLeafReader final : public ISegmentReader
{
public:
    AggregationLeafReader();
    ~AggregationLeafReader();

public:
    Status Open(const std::shared_ptr<indexlibv2::config::AggregationIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory);

public:
    std::unique_ptr<IKeyIterator> CreateKeyIterator() const override;
    std::unique_ptr<IValueIterator> Lookup(uint64_t key, autil::mem_pool::PoolBase* pool) const override;
    bool GetValueMeta(uint64_t key, ValueMeta& valueMeta) const override;

private:
    Status OpenMeta(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);
    Status OpenKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);
    Status OpenValue(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);

    const KeyMeta* KeyBegin() const { return _keyAddr; }
    const KeyMeta* KeyEnd() const { return _keyAddr + _indexMeta->keyCount; }

    const KeyMeta* FindKey(uint64_t key) const;

private:
    std::unique_ptr<IndexMeta> _indexMeta;
    const KeyMeta* _keyAddr = nullptr;
    const char* _valueAddr = nullptr;
    std::shared_ptr<indexlib::file_system::FileReader> _keyFileReader;
    std::shared_ptr<indexlib::file_system::FileReader> _valueFileReader;
    int32_t _valueSize = -1;
    bool _ownKeyMemory = false;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
