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
#include "indexlib/index/kkv/KKVShardRecordIterator.h"

#include "indexlib/config/MutableJson.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/KVDiskIndexer.h"

using namespace std;
using namespace indexlibv2::config;
using namespace indexlibv2::framework;

namespace indexlibv2::index {
AUTIL_LOG_SETUP_TEMPLATE(indexlib.table, KKVShardRecordIterator, SKeyType);

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::Init(const vector<shared_ptr<Segment>>& segments,
                                              const map<string, string>& params,
                                              const shared_ptr<ITabletSchema>& readSchema,
                                              const shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                                              int64_t currentTs)
{
    AUTIL_LOG(INFO, "init KKVShardRecordIterator with segment size [%lu]", segments.size());
    assert(!segments.empty());
    _schema = readSchema;
    GetKKVIndexConfig(readSchema);
    assert(_kkvIndexConfig);
    _segments = segments;
    auto result = CreateKKVIterator(segments);
    RETURN_IF_STATUS_ERROR(result.first, "create kkv iterator failed[%s] for table[%s]",
                           result.first.ToString().c_str(), readSchema->GetTableName().c_str());
    _kkvIterator = result.second;
    if (_pkValueIndexConfig) {
        auto status = CreatePKValueReaders(segments);
        RETURN_IF_STATUS_ERROR(status, "create PKValue reader failed[%s] for table[%s]", status.ToString().c_str(),
                               readSchema->GetTableName().c_str());
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

    auto [status, ttlEnable] = readSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
    if (status.IsNotFound()) {
        ttlEnable = false;
    } else if (!status.IsOK()) {
        RETURN_IF_STATUS_ERROR(status, "get 'enable_ttl' failed[%s] for table[%s]", status.ToString().c_str(),
                               readSchema->GetTableName().c_str());
    }
    auto [retStatus, ttlFromDoc] = readSchema->GetRuntimeSettings().GetValue<bool>("ttl_from_doc");
    if (retStatus.IsNotFound()) {
        ttlFromDoc = false;
    } else if (!retStatus.IsOK()) {
        RETURN_IF_STATUS_ERROR(retStatus, "get 'ttl_from_doc' failed[%s] for table[%s]", retStatus.ToString().c_str(),
                               readSchema->GetTableName().c_str());
    }
    if (ttlEnable && ttlFromDoc) {
        auto [status, ttlFieldName] = readSchema->GetRuntimeSettings().GetValue<string>("ttl_field_name");
        if (status.IsNotFound()) {
            ttlFieldName = DOC_TIME_TO_LIVE_IN_SECONDS;
        } else if (!status.IsOK()) {
            RETURN_IF_STATUS_ERROR(status, "get 'ttl_field_name' failed, [%s] for table[%s]", status.ToString().c_str(),
                                   readSchema->GetTableName().c_str());
        }
        _ttlFieldName = ttlFieldName;
    }

    _currentTsInSecond = autil::TimeUtility::us2sec(currentTs);
    _recordFilter = CreateRecordFilter(_currentTsInSecond);
    AUTIL_LOG(INFO, "init KKVShardRecordIterator success for table[%s]", readSchema->GetTableName().c_str());
    return Status::OK();
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::Next(index::IShardRecordIterator::ShardRecord* shardRecord, string* checkpoint)
{
    if (_currentRecord.isAvailable) {
        GetKKVRecord(shardRecord, checkpoint);
        return Status::OK();
    }
    auto status = DoNext();
    if (status.IsOK()) {
        GetKKVRecord(shardRecord, checkpoint);
    }
    return status;
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::DoNext()
{
    while (_kkvIterator->IsValid()) {
        _currentPKeyIter = _kkvIterator->GetCurrentIterator();
        auto status = ReadSinglePrefixKey();
        if (status.IsOK() || !status.IsEof()) {
            return status;
        }
        MoveToNextPkeyIter();
    }
    return Status::Eof();
}

template <typename SKeyType>
inline void KKVShardRecordIterator<SKeyType>::MoveToFirstValidSKeyPosition()
{
    while (_currentPKeyIter->IsValid()) {
        if (!_recordFilter->IsPassed(_currentPKeyIter->GetCurrentTs())) {
            MoveToNextSkeyIter();
            continue;
        }

        bool isDeleted = _currentPKeyIter->CurrentSkeyDeleted();
        if (isDeleted || _currentPKeyIter->CurrentSKeyExpired(_currentTsInSecond)) {
            MoveToNextSkeyIter();
            continue;
        }
        break;
    }
}

template <typename SKeyType>
inline void KKVShardRecordIterator<SKeyType>::MoveToNextValidSKeyPosition()
{
    MoveToNextSkeyIter();
    MoveToFirstValidSKeyPosition();
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::ReadSinglePrefixKey()
{
    _currentRecord.Clear();
    MoveToFirstValidSKeyPosition();
    while (_currentPKeyIter->IsValid()) {
        bool hasPkeyDeleted = _currentPKeyIter->HasPKeyDeleted();
        uint32_t pkeyDeletedTs = _currentPKeyIter->GetPKeyDeletedTs();
        bool isSkeyDeleted = _currentPKeyIter->CurrentSkeyDeleted();
        _currentRecord.shardRecord.timestamp = _currentPKeyIter->GetCurrentTs();
        uint32_t expireTime = _currentPKeyIter->GetCurrentExpireTime();
        auto pkeyHash = _currentPKeyIter->GetPKeyHash();
        if (!isSkeyDeleted && hasPkeyDeleted) {
            if (_currentRecord.shardRecord.timestamp < pkeyDeletedTs) {
                isSkeyDeleted = true;
            }
        }

        if (!isSkeyDeleted && !(!_ttlFieldName.empty() && _currentTsInSecond > expireTime)) {
            _currentPKeyIter->GetCurrentValue(_currentRecord.shardRecord.value);
            if (!_currentPKeyIter->CurrentSKeyExpired(_currentTsInSecond)) {
                SetCheckpoint(_pkeyOffset, _skeyOffset, &_currentRecord.checkpoint);
                _currentRecord.shardRecord.key = pkeyHash;
                _currentRecord.isAvailable = true;
                auto status = AddOtherFields(_currentRecord.shardRecord);
                MoveToNextValidSKeyPosition();
                return status;
            }
        }
        MoveToNextValidSKeyPosition();
    }
    return Status::Eof();
}

template <typename SKeyType>
bool KKVShardRecordIterator<SKeyType>::HasNext()
{
    if (_currentRecord.isAvailable) {
        return true;
    }
    return DoNext().IsOK();
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::Seek(const string& checkpoint)
{
    ShardCheckpoint* shardCheckpoint = (ShardCheckpoint*)checkpoint.data();
    _pkeyOffset = 0;
    _skeyOffset = 0;
    _currentRecord.Clear();
    auto result = CreateKKVIterator(_segments);
    RETURN_IF_STATUS_ERROR(result.first, "create kkv iterator failed[%s] for table[%s]",
                           result.first.ToString().c_str(), _kkvIndexConfig->GetIndexName().c_str());
    _kkvIterator = result.second;
    while (_pkeyOffset < shardCheckpoint->first && _kkvIterator->IsValid()) {
        _currentPKeyIter = _kkvIterator->GetCurrentIterator();
        MoveToNextPkeyIter();
    }
    if (!_kkvIterator->IsValid()) {
        if (shardCheckpoint->first != 0 || shardCheckpoint->second != 0) {
            auto status = Status::Corruption("seek to pkey position[%lu] failed for index[%s]", shardCheckpoint->first,
                                             _kkvIndexConfig->GetIndexName().c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        return Status::OK();
    }
    _currentPKeyIter = _kkvIterator->GetCurrentIterator();
    while (_skeyOffset < shardCheckpoint->second) {
        MoveToNextSkeyIter();
    }
    if (!_currentPKeyIter->IsValid()) {
        auto status = Status::Corruption("seek to pkey position[%lu] skey position[%lu] failed for index[%s]",
                                         shardCheckpoint->first, shardCheckpoint->second,
                                         _kkvIndexConfig->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    return Status::OK();
}

template <typename SKeyType>
void KKVShardRecordIterator<SKeyType>::GetKKVIndexConfig(const shared_ptr<ITabletSchema>& readSchema)
{
    auto indexConfigs = readSchema->GetIndexConfigs();
    assert(indexConfigs.size() <= 2);
    for (const auto& indexConfig : indexConfigs) {
        if (index::KKV_RAW_KEY_INDEX_NAME != indexConfig->GetIndexName()) {
            _kkvIndexConfig = dynamic_pointer_cast<KKVIndexConfig>(indexConfig);
        } else {
            _pkValueIndexConfig = dynamic_pointer_cast<KVIndexConfig>(indexConfig);
        }
    }
}

template <typename SKeyType>
std::pair<Status, std::shared_ptr<OnDiskKKVIterator<SKeyType>>> KKVShardRecordIterator<SKeyType>::CreateKKVIterator(
    const vector<shared_ptr<indexlibv2::framework::Segment>>& segments) const
{
    vector<IIndexMerger::SourceSegment> sourceSegments;
    for (auto segment : segments) {
        IIndexMerger::SourceSegment sourceSegment;
        sourceSegment.segment = segment;
        sourceSegments.emplace_back(sourceSegment);
    }
    auto kkvIterator = make_shared<OnDiskKKVIterator<SKeyType>>(_kkvIndexConfig);
    auto status = kkvIterator->Init(sourceSegments);
    if (!status.IsOK()) {
        return {status, nullptr};
    }
    return {Status::OK(), kkvIterator};
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::CreatePKValueReaders(const vector<shared_ptr<framework::Segment>>& segments)
{
    assert(_pkValueIndexConfig);
    for (size_t i = 0; i < segments.size(); i++) {
        auto [status, indexer] =
            segments[i]->GetIndexer(_pkValueIndexConfig->GetIndexType(), _pkValueIndexConfig->GetIndexName());
        if (!status.IsOK()) {
            return Status::InternalError("no indexer[%s] in segment[%d], table = [%s]",
                                         _pkValueIndexConfig->GetIndexName().c_str(), segments[i]->GetSegmentId(),
                                         _schema->GetTableName().c_str());
        }
        auto diskIndexer = dynamic_pointer_cast<KVDiskIndexer>(indexer);
        if (!diskIndexer) {
            return Status::InternalError("indexer for [%s] in segment[%d] with no OnDiskIndexer, table = [%s]",
                                         _pkValueIndexConfig->GetIndexName().c_str(), segments[i]->GetSegmentId(),
                                         _schema->GetTableName().c_str());
        }
        _pkValueReaders[i] = diskIndexer->GetReader();
    }
    return Status::OK();
}

template <typename SKeyType>
unique_ptr<KKVRecordFilter> KKVShardRecordIterator<SKeyType>::CreateRecordFilter(uint64_t currentTsInSecond) const
{
    if (!_kkvIndexConfig->TTLEnabled() || !_ttlFieldName.empty()) {
        return std::make_unique<KKVRecordFilter>();
    }
    return std::make_unique<KKVRecordFilter>(_kkvIndexConfig->GetTTL(), currentTsInSecond);
}

template <typename SKeyType>
void KKVShardRecordIterator<SKeyType>::SetCheckpoint(offset_t pkeyOffset, offset_t skeyOffset, string* checkpoint)
{
    checkpoint->assign((char*)&pkeyOffset, sizeof(pkeyOffset));
    checkpoint->append((char*)&(skeyOffset), sizeof(skeyOffset));
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::AddRawKey(index::IShardRecordIterator::ShardRecord* shardRecord,
                                                   const std::shared_ptr<IKVSegmentReader>& pkValueReader)
{
    if (!_pkValueIndexConfig || !_pkFormatter) {
        shardRecord->otherFields[_kkvIndexConfig->GetPrefixFieldName()] = autil::StringUtil::toString(shardRecord->key);
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
    shardRecord->otherFields[_kkvIndexConfig->GetPrefixFieldName()] = rawPkey;
    return Status::OK();
}

template <typename SKeyType>
Status KKVShardRecordIterator<SKeyType>::AddOtherFields(index::IShardRecordIterator::ShardRecord& shardRecord)
{
    if (_kkvIndexConfig->OptimizedStoreSKey()) {
        SKeyType skeyHash = _currentPKeyIter->GetCurrentSkey();
        shardRecord.otherFields[_kkvIndexConfig->GetSuffixFieldName()] = autil::StringUtil::toString(skeyHash);
    }
    uint32_t expireTime = _currentPKeyIter->GetCurrentExpireTime();
    if (expireTime != indexlib::UNINITIALIZED_EXPIRE_TIME && !_ttlFieldName.empty()) {
        shardRecord.otherFields[_ttlFieldName] = autil::StringUtil::toString(expireTime - shardRecord.timestamp);
    }
    return AddRawKey(&shardRecord, _pkValueReaders[_currentPKeyIter->GetCurrentSegmentIdx()]);
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVShardRecordIterator);

} // namespace indexlibv2::index
