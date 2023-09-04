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

#include "indexlib/base/Constant.h"
#include "indexlib/index/IShardRecordIterator.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/RecordFilter.h"

namespace indexlibv2::index {
class KVDiskIndexer;
class PackAttributeFormatter;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {

class KVShardRecordIterator final : public IShardRecordIterator
{
public:
    KVShardRecordIterator() {}
    KVShardRecordIterator(bool ignoreValue) : _ignoreValue(ignoreValue) {}
    ~KVShardRecordIterator() = default;

public:
    Status Init(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments,
                const std::map<std::string, std::string>& params,
                const std::shared_ptr<indexlibv2::config::ITabletSchema>& readSchema,
                const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator, int64_t currentTs) override;
    Status Next(IShardRecordIterator::ShardRecord* shardRecord, std::string* checkpoint) override;
    bool HasNext() override;
    Status Seek(const std::string& checkpoint) override;

private:
    struct KVIteratorWrapper {
        IKVIterator* iterator = nullptr;
        std::unique_ptr<IKVIterator> kvIterator;
        std::shared_ptr<indexlib::file_system::FileReader> pkFileReader;
        std::unique_ptr<HashTableBase> pkHashTable;
        std::shared_ptr<IKVSegmentReader> pkValueReader;
    };

private:
    void GetKVIndexConfig(const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
                          std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                          std::shared_ptr<indexlibv2::config::KVIndexConfig>& pkValueIndexConfig) const;
    std::unique_ptr<RecordFilter> CreateRecordFilter(uint64_t currentTsInSecond) const;
    Status CreateKVRecordIterators(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments);
    std::pair<Status, std::unique_ptr<KVIteratorWrapper>>
    CreateKVIteratorWrapper(const std::shared_ptr<indexlibv2::framework::Segment>& segment,
                            const std::shared_ptr<KVDiskIndexer>& kvIndexer,
                            const std::shared_ptr<KVDiskIndexer>& pkValueIndexer, bool needPkHashTable) const;
    std::pair<Status, std::shared_ptr<KVDiskIndexer>>
    GetIndexer(const std::shared_ptr<indexlibv2::framework::Segment>& segment,
               std::shared_ptr<indexlibv2::config::KVIndexConfig> indexConfig) const;
    void SetCheckpoint(offset_t iterIdx, offset_t offset, std::string* checkpoint);
    Status AddRawKey(index::IShardRecordIterator::ShardRecord* shardRecord,
                     const std::shared_ptr<IKVSegmentReader>& pkValueReader);

private:
    static const size_t MAX_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;
    bool _ignoreValue = false;
    offset_t _currIteratorIndex = 0;
    schemaid_t _readerSchemaId = DEFAULT_SCHEMAID;
    autil::mem_pool::Pool _pool;
    std::unique_ptr<RecordFilter> _recordFilter;
    std::shared_ptr<AdapterIgnoreFieldCalculator> _ignoreFieldCalculator;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _kvIndexConfig;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _pkValueIndexConfig;
    std::vector<std::unique_ptr<KVIteratorWrapper>> _kvIterators;
    std::shared_ptr<PackAttributeFormatter> _pkFormatter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
