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

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/IShardRecordIterator.h"
#include "indexlib/index/kkv/common/KKVRecordFilter.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/merge/OnDiskKKVIterator.h"
#include "indexlib/index/kkv/merge/OnDiskSinglePKeyIterator.h"
#include "indexlib/index/kv/IKVSegmentReader.h"

namespace indexlibv2::index {
class KKVDiskIndexer;
class PackAttributeFormatter;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class KKVIndexConfig;
}

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {

template <typename SKeyType>
class KKVShardRecordIterator final : public IShardRecordIterator
{
private:
    struct KKVRecord {
        bool isAvailable = false;
        index::IShardRecordIterator::ShardRecord shardRecord;
        std::string checkpoint;
        void Clear()
        {
            isAvailable = false;
            shardRecord.otherFields.clear();
            checkpoint.clear();
        }
    };

public:
    KKVShardRecordIterator() = default;
    ~KKVShardRecordIterator() = default;

public:
    Status Init(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments,
                const std::map<std::string, std::string>& params,
                const std::shared_ptr<indexlibv2::config::TabletSchema>& tabletSchema,
                const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator, int64_t currentTs) override;
    Status Next(index::IShardRecordIterator::ShardRecord* shardRecord, std::string* checkpoint) override;
    bool HasNext() override;
    Status Seek(const std::string& checkpoint) override;

    const std::shared_ptr<config::KKVIndexConfig>& TEST_GetKKVIndexConfig() const { return _kkvIndexConfig; }
    const std::shared_ptr<config::KVIndexConfig>& TEST_GetPKValueIndexConfig() const { return _pkValueIndexConfig; }

private:
    void GetKKVIndexConfig(const std::shared_ptr<indexlibv2::config::TabletSchema>& tabletSchema);
    std::unique_ptr<KKVRecordFilter> CreateRecordFilter(uint64_t currentTsInSecond) const;
    std::shared_ptr<OnDiskKKVIterator<SKeyType>>
    CreateKKVIterator(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments) const;
    Status CreatePKValueReaders(const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments);
    void SetCheckpoint(offset_t pkeyOffset, offset_t skeyOffset, std::string* checkpoint);
    Status AddRawKey(index::IShardRecordIterator::ShardRecord* shardRecord,
                     const std::shared_ptr<IKVSegmentReader>& pkValueReader);
    Status AddOtherFields(index::IShardRecordIterator::ShardRecord& shardRecord);

    Status DoNext();
    Status ReadSinglePrefixKey();
    void MoveToFirstValidSKeyPosition();
    void MoveToNextValidSKeyPosition();
    void MoveToNextPkeyIter()
    {
        _kkvIterator->MoveToNext();
        _pkeyOffset++;
        _skeyOffset = 0;
    }
    void MoveToNextSkeyIter()
    {
        _currentPKeyIter->MoveToNext();
        _skeyOffset++;
    }
    void GetKKVRecord(index::IShardRecordIterator::ShardRecord* shardRecord, std::string* checkpoint)
    {
        assert(_currentRecord.isAvailable);
        shardRecord->key = _currentRecord.shardRecord.key;
        shardRecord->value = _currentRecord.shardRecord.value;
        shardRecord->timestamp = _currentRecord.shardRecord.timestamp;
        shardRecord->otherFields.swap(_currentRecord.shardRecord.otherFields);
        *checkpoint = _currentRecord.checkpoint;
        _currentRecord.isAvailable = false;
    }

private:
    offset_t _pkeyOffset = 0;
    offset_t _skeyOffset = 0;
    uint64_t _currentTsInSecond = 0;
    KKVRecord _currentRecord;
    std::string _ttlFieldName;
    static const size_t MAX_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;
    autil::mem_pool::Pool _pool;
    std::shared_ptr<config::TabletSchema> _schema;
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    std::shared_ptr<config::KVIndexConfig> _pkValueIndexConfig;
    std::unique_ptr<KKVRecordFilter> _recordFilter;
    std::shared_ptr<OnDiskKKVIterator<SKeyType>> _kkvIterator;
    OnDiskSinglePKeyIterator<SKeyType>* _currentPKeyIter = nullptr;
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> _segments;
    std::map<size_t, std::shared_ptr<IKVSegmentReader>> _pkValueReaders;
    std::shared_ptr<PackAttributeFormatter> _pkFormatter;

private:
    AUTIL_LOG_DECLARE();
};

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVShardRecordIterator);

} // namespace indexlibv2::index
