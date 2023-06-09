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
#include "indexlib/index/kv/KVShardRecordIterator.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/AdapterKVSegmentReader.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/FixedLenHashTableCreator.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVSegmentReaderCreator.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/NoneFilter.h"
#include "indexlib/index/kv/TTLFilter.h"
#include "indexlib/index/kv/VarLenHashTableCreator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"

using namespace std;
using namespace indexlibv2::config;
using namespace indexlibv2::framework;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KVShardRecordIterator);

Status KVShardRecordIterator::Init(const vector<shared_ptr<Segment>>& segments, const map<string, string>& params,
                                   const shared_ptr<TabletSchema>& readSchema,
                                   const shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                                   int64_t currentTs)
{
    AUTIL_LOG(INFO, "init KVShardRecordIterator with segment size [%lu]", segments.size());
    assert(!segments.empty());
    _readerSchemaId = readSchema->GetSchemaId();
    GetKVIndexConfig(readSchema, _kvIndexConfig, _pkValueIndexConfig);
    assert(_kvIndexConfig);
    if (_pkValueIndexConfig) {
        auto pkValueConfig = _pkValueIndexConfig->GetValueConfig();
        if (pkValueConfig) {
            _pkFormatter = make_shared<PackAttributeFormatter>();
            auto [status2, pkPackAttrConfig] = pkValueConfig->CreatePackAttributeConfig();
            RETURN_IF_STATUS_ERROR(status2, "create pkey pack attribute config failed");
            if (!_pkFormatter->Init(pkPackAttrConfig)) {
                return Status::InternalError("init pk value formatter failed");
            }
        }
    }
    _ignoreFieldCalculator = ignoreFieldCalculator;
    auto currentTsInSecond = (uint64_t)autil::TimeUtility::us2sec(currentTs);
    _recordFilter = CreateRecordFilter(currentTsInSecond);
    return CreateKVRecordIterators(segments);
}

Status KVShardRecordIterator::Next(index::IShardRecordIterator::ShardRecord* shardRecord, string* checkpoint)
{
    if (_pool.getUsedBytes() >= MAX_POOL_MEMORY_THRESHOLD) {
        _pool.reset();
    }

    while (HasNext()) {
        auto offset = _kvIterators[_currIteratorIndex]->kvIterator->GetOffset();
        Record record;
        auto status = _kvIterators[_currIteratorIndex]->kvIterator->Next(&_pool, record);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "iterator next failed, status[%s], iterator index[%lu]", status.ToString().c_str(),
                      _currIteratorIndex);
            return status;
        }
        if (!record.deleted) {
            bool isOverlapped = false;
            for (offset_t id = 0; id < _currIteratorIndex; ++id) {
                autil::StringView findValue;
                indexlib::util::Status status = _kvIterators[id]->pkHashTable->Find(record.key, findValue);
                if (indexlib::util::NOT_FOUND != status) {
                    isOverlapped = true;
                    break;
                }
            }
            if (!isOverlapped) {
                if (_recordFilter->IsPassed(record)) {
                    SetCheckpoint(_currIteratorIndex, offset, checkpoint);
                    shardRecord->key = record.key;
                    shardRecord->value = record.value;
                    shardRecord->timestamp = record.timestamp;
                    return AddRawKey(shardRecord, _kvIterators[_currIteratorIndex]->pkValueReader);
                }
            }
        }
    }
    return Status::Eof();
}

bool KVShardRecordIterator::HasNext()
{
    while (_currIteratorIndex < _kvIterators.size()) {
        if (_kvIterators[_currIteratorIndex]->kvIterator->HasNext()) {
            return true;
        }
        _currIteratorIndex++;
    }
    return false;
}

Status KVShardRecordIterator::Seek(const string& checkpoint)
{
    ShardCheckpoint* shardCheckpoint = (ShardCheckpoint*)checkpoint.data();
    if (shardCheckpoint->first >= _kvIterators.size()) {
        AUTIL_LOG(ERROR, "shard checkpoint iterator id[%lu] >= iterators size[%lu]", shardCheckpoint->first,
                  _kvIterators.size());
        return Status::OutOfRange();
    }
    _currIteratorIndex = shardCheckpoint->first;
    if (shardCheckpoint->second == 0) {
        _kvIterators[_currIteratorIndex]->kvIterator->Reset();
    } else {
        auto status = _kvIterators[_currIteratorIndex]->kvIterator->Seek(shardCheckpoint->second);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "iterator seek failed, status[%s], iterator index[%lu] offset[%lu]",
                      status.ToString().c_str(), _currIteratorIndex, shardCheckpoint->second);
            return status;
        }
    }
    for (auto i = _currIteratorIndex + 1; i < _kvIterators.size(); ++i) {
        _kvIterators[i]->kvIterator->Reset();
    }
    return Status::OK();
}

void KVShardRecordIterator::GetKVIndexConfig(const shared_ptr<TabletSchema>& readSchema,
                                             shared_ptr<KVIndexConfig>& kvIndexConfig,
                                             shared_ptr<KVIndexConfig>& pkValueIndexConfig) const
{
    auto indexConfigs = readSchema->GetIndexConfigs();
    assert(indexConfigs.size() <= 2);
    for (const auto& indexConfig : indexConfigs) {
        auto kvConfig = dynamic_pointer_cast<KVIndexConfig>(indexConfig);
        if (KV_RAW_KEY_INDEX_NAME != kvConfig->GetIndexName()) {
            kvIndexConfig = kvConfig;
        } else {
            pkValueIndexConfig = kvConfig;
        }
    }
}

unique_ptr<RecordFilter> KVShardRecordIterator::CreateRecordFilter(uint64_t currentTsInSecond) const
{
    if (!_kvIndexConfig->TTLEnabled()) {
        return std::make_unique<NoneFilter>();
    }

    return std::make_unique<TTLFilter>(_kvIndexConfig->GetTTL(), currentTsInSecond);
}

Status KVShardRecordIterator::CreateKVRecordIterators(const vector<shared_ptr<Segment>>& segments)
{
    for (auto iter = segments.rbegin(); iter != segments.rend(); iter++) {
        auto [status, kvIndexer] = GetIndexer(*iter, _kvIndexConfig);
        RETURN_IF_STATUS_ERROR(status, "get kv indexer failed");
        shared_ptr<KVDiskIndexer> pkValueIndexer;
        if (_pkValueIndexConfig) {
            auto [status, indexer] = GetIndexer(*iter, _pkValueIndexConfig);
            RETURN_IF_STATUS_ERROR(status, "get pk value indexer failed");
            pkValueIndexer = indexer;
        }
        auto [retStatus, iteratorWrapper] =
            CreateKVIteratorWrapper(*iter, kvIndexer, pkValueIndexer, iter != segments.rend() - 1);
        if (!retStatus.IsOK()) {
            return retStatus;
        }
        _kvIterators.emplace_back(move(iteratorWrapper));
    }
    return Status::OK();
}

pair<Status, unique_ptr<KVShardRecordIterator::KVIteratorWrapper>> KVShardRecordIterator::CreateKVIteratorWrapper(
    const shared_ptr<Segment>& segment, const shared_ptr<KVDiskIndexer>& kvIndexer,
    const shared_ptr<KVDiskIndexer>& pkValueIndexer, bool needPkHashTable) const
{
    assert(segment->GetSegmentSchema());
    schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();
    vector<string> ignoreFields = _ignoreFieldCalculator->GetIgnoreFields(std::min(segSchemaId, _readerSchemaId),
                                                                          std::max(segSchemaId, _readerSchemaId));
    auto [status, segmentReader] = KVSegmentReaderCreator::CreateSegmentReader(
        kvIndexer->GetReader(), kvIndexer->GetIndexConfig(), _kvIndexConfig, ignoreFields, false);
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }

    auto iteratorWrapper = std::make_unique<KVIteratorWrapper>();

    iteratorWrapper->kvIterator = segmentReader->CreateIterator();
    if (pkValueIndexer) {
        iteratorWrapper->pkValueReader = pkValueIndexer->GetReader();
    }
    if (!needPkHashTable) {
        return std::make_pair(status, std::move(iteratorWrapper));
    }

    const auto& indexDir = kvIndexer->GetIndexDirectory();
    auto kvDir = indexDir->GetDirectory(_kvIndexConfig->GetIndexName(), false);
    if (!kvDir) {
        status = Status::Corruption("index dir [%s] does not exist in [%s]", _kvIndexConfig->GetIndexName().c_str(),
                                    indexDir->DebugString().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return std::make_pair(status, nullptr);
    }

    auto result = kvDir->GetIDirectory()->CreateFileReader(KV_KEY_FILE_NAME, indexlib::file_system::FSOT_MEM);
    if (!result.OK()) {
        return std::make_pair(result.Status(), nullptr);
    }
    iteratorWrapper->pkFileReader = result.Value();

    KVFormatOptions formatOpts;
    status = formatOpts.Load(kvDir);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "load format options failed, path[%s]", kvDir->DebugString().c_str());
        return std::make_pair(status, nullptr);
    }
    auto typeId = MakeKVTypeId(*_kvIndexConfig, &formatOpts);
    unique_ptr<HashTableInfo> hashTableInfo;
    if (typeId.isVarLen) {
        hashTableInfo = VarLenHashTableCreator::CreateHashTableForReader(typeId, false);
    } else {
        hashTableInfo = FixedLenHashTableCreator::CreateHashTableForReader(typeId, false);
    }
    iteratorWrapper->pkHashTable = hashTableInfo->StealHashTable<HashTableBase>();
    if (!iteratorWrapper->pkHashTable) {
        return std::make_pair(Status::InternalError("expect type: HashTableBase"), nullptr);
    }

    auto pkFileReader = iteratorWrapper->pkFileReader;
    assert(pkFileReader->GetBaseAddress() != nullptr);
    if (!iteratorWrapper->pkHashTable->MountForRead(pkFileReader->GetBaseAddress(), pkFileReader->GetLength())) {
        status = Status::Corruption("mount hash table failed, file: %s, length: %lu",
                                    pkFileReader->DebugString().c_str(), pkFileReader->GetLength());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return std::make_pair(status, nullptr);
    }

    return std::make_pair(Status::OK(), std::move(iteratorWrapper));
}

pair<Status, shared_ptr<KVDiskIndexer>> KVShardRecordIterator::GetIndexer(const shared_ptr<Segment>& segment,
                                                                          shared_ptr<KVIndexConfig> indexConfig) const
{
    auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
    if (!status.IsOK()) {
        return std::make_pair(Status::InternalError("no indexer for [%s] in segment[%d] ",
                                                    indexConfig->GetIndexName().c_str(), segment->GetSegmentId()),
                              nullptr);
    }
    auto diskIndexer = dynamic_pointer_cast<KVDiskIndexer>(indexer);
    if (!diskIndexer) {
        return std::make_pair(Status::InternalError("indexer for [%s] in segment[%d] with no OnDiskIndexer",
                                                    indexConfig->GetIndexName().c_str(), segment->GetSegmentId()),
                              nullptr);
    }
    return std::make_pair(status, diskIndexer);
}

Status KVShardRecordIterator::AddRawKey(index::IShardRecordIterator::ShardRecord* shardRecord,
                                        const std::shared_ptr<IKVSegmentReader>& pkValueReader)
{
    if (!_pkValueIndexConfig || !_pkFormatter) {
        shardRecord->otherFields[_kvIndexConfig->GetKeyFieldName()] = autil::StringUtil::toString(shardRecord->key);
        return Status::OK();
    }

    uint64_t ts = 0;
    autil::StringView pkValue;
    auto status =
        future_lite::interface::syncAwait(pkValueReader->Get(shardRecord->key, pkValue, ts, &_pool, nullptr, nullptr));
    switch (status) {
    case indexlib::util::Status::DELETED:
        return Status::NotFound("no record found, [DELETED]");
    case indexlib::util::Status::FAIL:
        return Status::NotFound("no record found, [FAIL]");
    case indexlib::util::Status::NOT_FOUND:
        return Status::NotFound("no record found, [NOT FOUND]");
    default:
        break;
    }
    string rawPkey;
    FieldValueExtractor extractor(_pkFormatter.get(), pkValue, &_pool);
    assert(extractor.GetFieldCount() == 1);
    string attrName;
    if (!extractor.GetStringValue(0, attrName, rawPkey)) {
        AUTIL_LOG(ERROR, "attr pkey read str value failed");
        return Status::Corruption();
    }
    shardRecord->otherFields[_kvIndexConfig->GetKeyFieldName()] = rawPkey;
    return Status::OK();
}

void KVShardRecordIterator::SetCheckpoint(offset_t iterIdx, offset_t offset, string* checkpoint)
{
    checkpoint->assign((char*)&iterIdx, sizeof(iterIdx));
    checkpoint->append((char*)&(offset), sizeof(offset));
}

} // namespace indexlibv2::index
