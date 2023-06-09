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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/KeyVector.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::file_system {
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class IValueList;

class PostingMap
{
public:
    PostingMap(autil::mem_pool::PoolBase* objPool, autil::mem_pool::Pool* bufferPool,
               const std::shared_ptr<config::AggregationIndexConfig>& indexConfig, size_t initialKeyCount);
    ~PostingMap();

public:
    Status Add(uint64_t key, const autil::StringView& data);
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDir);

    size_t GetKeyCount() const { return _keyVector->GetKeyCount(); }
    uint64_t GetMaxValueCountOfAllKeys() const { return _maxValueCount; }
    uint64_t GetTotalBytes() const { return _totalBytes; }

public:
    std::unique_ptr<IKeyIterator> CreateKeyIterator() const { return _keyVector->CreateIterator(); }
    const IValueList* Lookup(uint64_t key) const;

private:
    std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                     const std::string& fileName) const;

private:
    using PostingTable = indexlib::util::HashMap<uint64_t, IValueList*>;

private:
    autil::mem_pool::PoolBase* _objPool = nullptr;
    autil::mem_pool::Pool* _bufferPool = nullptr;
    std::unique_ptr<KeyVector> _keyVector;
    PostingTable* _postingMap = nullptr;
    std::shared_ptr<config::AggregationIndexConfig> _indexConfig;
    uint64_t _totalBytes = 0;
    uint64_t _maxValueCount = 0;
    uint64_t _maxValueBytes = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
