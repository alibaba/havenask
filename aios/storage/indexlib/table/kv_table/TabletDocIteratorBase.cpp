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
#include "indexlib/table/kv_table/TabletDocIteratorBase.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/util/KeyValueMap.h"

using namespace std;
using namespace autil;
using namespace indexlibv2::config;
using namespace indexlibv2::document;
using namespace indexlibv2::framework;
using namespace indexlibv2::index;
using namespace indexlibv2::plain;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.index, TabletDocIteratorBase);

const std::string TabletDocIteratorBase::READER_TIMESTAMP = "reader_timestamp";

Status TabletDocIteratorBase::Init(const shared_ptr<framework::TabletData>& tabletData,
                                   pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                                   const std::shared_ptr<indexlibv2::framework::MetricsManager>&,
                                   const std::vector<std::string>& requiredFields, const map<string, string>& params)
{
    if (!tabletData) {
        AUTIL_LOG(ERROR, "tablet data is nullptr");
        return Status::Corruption("tablet data is nullptr");
    }
    auto slice = tabletData->CreateSlice();
    if (slice.empty()) {
        AUTIL_LOG(INFO, "init doc iterator with empty slice");
        assert(!HasNext());
        return Status::OK();
    }
    _fieldNames = requiredFields;
    Status status = DoInit(tabletData, params);
    RETURN_IF_STATUS_ERROR(status, "init tablet doc iterator failed, table version[%s]",
                           tabletData->GetOnDiskVersion().ToString().c_str());
    size_t shardCount = tabletData->GetOnDiskVersion().GetSegmentDescriptions()->GetLevelInfo()->GetShardCount();
    status = SelectTargetShards(shardCount, rangeInRatio);
    RETURN_IF_STATUS_ERROR(status, "select shards for tablet doc iterator failed, table version[%s]",
                           tabletData->GetOnDiskVersion().ToString().c_str());
    return InitIterators(tabletData, params);
}

Status TabletDocIteratorBase::Next(RawDocument* rawDocument, string* checkpoint, IDocument::DocInfo* docInfo)
{
    assert(HasNext());
    if (_pool.getUsedBytes() >= MAX_RELEASE_POOL_MEMORY_THRESHOLD) {
        _pool.release();
    } else if (_pool.getUsedBytes() >= MAX_RESET_POOL_MEMORY_THRESHOLD) {
        _pool.reset();
    }

    Status status = ReadValue(&_nextRecord, _shardIterId, rawDocument);
    RETURN_IF_STATUS_ERROR(status, "read record value failed, status [%s], shard iterator index[%lu]",
                           status.ToString().c_str(), _shardIterId);
    rawDocument->setDocOperateType(ADD_DOC);
    docInfo->timestamp = _nextRecord.timestamp;
    status = MoveToNext();
    SetCheckPoint(_nextShardCheckpoint, checkpoint);
    RETURN_IF_STATUS_ERROR(status, "move to next record failed, shard iterator index[%lu]", _shardIterId);
    return status;
}

bool TabletDocIteratorBase::HasNext() const { return _shardIterId < _shardDocIterators.size(); }

Status TabletDocIteratorBase::MoveToNext()
{
    while (_shardIterId < _shardDocIterators.size()) {
        if (_shardDocIterators[_shardIterId]->HasNext()) {
            _nextRecord.otherFields.clear();
            Status status = _shardDocIterators[_shardIterId]->Next(&_nextRecord, &_nextShardCheckpoint);
            if (status.IsEof()) {
                continue;
            }
            RETURN_IF_STATUS_ERROR(status, "get next raw document failed, status [%s], shard iterator index[%lu]",
                                   status.ToString().c_str(), _shardIterId);
            return status;
        }
        _shardIterId++;
    }
    return Status::OK();
}

Status TabletDocIteratorBase::Seek(const string& checkpoint)
{
    if (checkpoint.size() < sizeof(_shardIterId)) {
        return Status::InvalidArgs("checkpoint size [%lu] not match", checkpoint.size());
    }

    TabletCheckpoint* tabletCheckpoint = (TabletCheckpoint*)checkpoint.data();
    if (tabletCheckpoint->shardIterIdx == _shardDocIterators.size()) {
        _shardIterId = tabletCheckpoint->shardIterIdx;
        return Status::OK();
    }
    if (tabletCheckpoint->shardIterIdx < 0 || tabletCheckpoint->shardIterIdx > _shardDocIterators.size()) {
        return Status::OutOfRange("shard iterator id [%lu] out of range [0, %lu]", tabletCheckpoint->shardIterIdx,
                                  _shardDocIterators.size());
    }
    string shardCheckpoint = checkpoint.substr(sizeof(tabletCheckpoint->shardIterIdx));
    Status status = _shardDocIterators[tabletCheckpoint->shardIterIdx]->Seek(shardCheckpoint);
    RETURN_IF_STATUS_ERROR(
        status, "shard iterator seek failed, status [%s], iterator index [%zu], shard checkpoint [%lu , %lu]",
        status.ToString().c_str(), tabletCheckpoint->shardIterIdx, tabletCheckpoint->segmentIterIdx,
        tabletCheckpoint->segmentOffset);

    IShardRecordIterator::ShardCheckpoint defaultShardCheckpoint;
    string defaultCheckpoint((char*)&defaultShardCheckpoint, sizeof(defaultShardCheckpoint));
    for (auto i = tabletCheckpoint->shardIterIdx + 1; i < _shardDocIterators.size(); ++i) {
        status = _shardDocIterators[i]->Seek(defaultCheckpoint);
        RETURN_IF_STATUS_ERROR(
            status, "shard iterator seek failed, status [%s], iterator index [%lu], shard checkpoint [%lu , %lu]",
            status.ToString().c_str(), i, defaultShardCheckpoint.first, defaultShardCheckpoint.second);
    }

    _shardIterId = tabletCheckpoint->shardIterIdx;
    return MoveToNext();
}

Status TabletDocIteratorBase::DoInit(const shared_ptr<TabletData>& tabletData, const map<string, string>& params)
{
    _timeStamp = TimeUtility::currentTime();
    string timeStampStr = indexlib::util::GetValueFromKeyValueMap(params, READER_TIMESTAMP);
    if (!timeStampStr.empty() && !StringUtil::fromString(timeStampStr, _timeStamp)) {
        AUTIL_LOG(WARN, "kv reader's timestamp [%s] is invalid, use current timestamp", timeStampStr.c_str());
    }

    return InitFromTabletData(tabletData);
}

Status TabletDocIteratorBase::InitFormatter(const shared_ptr<ValueConfig>& valueConfig,
                                            const shared_ptr<ValueConfig>& pkValueConfig)
{
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    RETURN_IF_STATUS_ERROR(status, "create pack attribute config failed");
    _formatter = make_shared<PackAttributeFormatter>();
    if (!_formatter->Init(packAttrConfig)) {
        return Status::InternalError("init value formatter failed");
    }
    _plainFormatEncoder.reset(_formatter->CreatePlainFormatEncoder());
    if (pkValueConfig) {
        _pkFormatter = make_shared<PackAttributeFormatter>();
        auto [status2, pkPackAttrConfig] = pkValueConfig->CreatePackAttributeConfig();
        RETURN_IF_STATUS_ERROR(status2, "create pkey pack attribute config failed");
        if (!_pkFormatter->Init(pkPackAttrConfig)) {
            return Status::InternalError("init pk value formatter failed");
        }
    }
    return Status::OK();
}

Status TabletDocIteratorBase::InitIterators(const shared_ptr<TabletData>& tabletData, const map<string, string>& params)
{
    TabletData::Slice slice = tabletData->CreateSlice();
    if (slice.empty()) {
        AUTIL_LOG(ERROR, "init iterators failed with empty slice");
        return Status::Corruption("empty tablet data");
    }

    vector<vector<shared_ptr<Segment>>> shardSegments;
    shardSegments.resize(_targetShardRange.second - _targetShardRange.first + 1);

    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segment = *iter;
        auto multiShardSegment = std::dynamic_pointer_cast<MultiShardDiskSegment>(segment);
        uint32_t shardCount = segment->GetSegmentInfo()->GetShardCount();
        if (segment->GetSegmentInfo()->HasMultiSharding()) {
            for (uint32_t i = _targetShardRange.first; i <= _targetShardRange.second; i++) {
                if (multiShardSegment->GetShardSegment(i)->GetSegmentInfo()->GetDocCount() == 0) {
                    continue;
                }
                shardSegments[i - _targetShardRange.first].push_back(multiShardSegment->GetShardSegment(i));
            }
        } else {
            uint32_t shardId = shardCount == 1 ? 0 : segment->GetSegmentInfo()->GetShardId();
            if (_targetShardRange.first <= shardId && shardId <= _targetShardRange.second) {
                if (segment->GetSegmentInfo()->GetDocCount() == 0) {
                    continue;
                }
                shardSegments[shardId - _targetShardRange.first].push_back(multiShardSegment->GetShardSegment(shardId));
            }
        }
    }

    _shardDocIterators.clear();
    for (auto& segments : shardSegments) {
        if (!segments.empty()) {
            auto shardDocIterator = CreateShardDocIterator();
            Status status = shardDocIterator->Init(segments, params, _tabletSchema, _ignoreFieldCalculator, _timeStamp);
            RETURN_IF_STATUS_ERROR(status, "init shardDocIterator failed");
            _shardDocIterators.push_back(std::move(shardDocIterator));
        }
    }

    _shardIterId = 0;
    return MoveToNext();
}

Status TabletDocIteratorBase::SelectTargetShards(size_t& shardCount, pair<uint32_t, uint32_t> rangeInRatio)
{
    if (rangeInRatio.first < 0 || rangeInRatio.first > rangeInRatio.second ||
        rangeInRatio.second > RANGE_UPPER_BOUND - 1) {
        return Status::InvalidArgs("invalid range [%u - %u]");
    }

    _targetShardRange.first = (rangeInRatio.first * shardCount + RANGE_UPPER_BOUND - 1) / RANGE_UPPER_BOUND;
    _targetShardRange.second = rangeInRatio.second * shardCount / RANGE_UPPER_BOUND;
    AUTIL_LOG(INFO, "select range [%u - %u], target shard range [%u - %u] with shard count [%lu]", rangeInRatio.first,
              rangeInRatio.second, _targetShardRange.first, _targetShardRange.second, shardCount);
    return Status::OK();
}

Status TabletDocIteratorBase::ReadValue(const index::IShardRecordIterator::ShardRecord* shardRecord,
                                        size_t recordShardId, RawDocument* doc)
{
    if (!_fieldNames.empty()) {
        for (auto kv : shardRecord->otherFields) {
            doc->setField(kv.first, kv.second);
        }
        StringView packValue = PreprocessPackValue(shardRecord->value);
        FieldValueExtractor extractor(_formatter.get(), packValue, &_pool);
        for (size_t idx = 0; idx < extractor.GetFieldCount(); idx++) {
            string attrName;
            string attrValue;
            if (!extractor.GetStringValue(idx, attrName, attrValue)) {
                AUTIL_LOG(ERROR, "attr idx [%zu] read str value failed, pkeyHash[%lu]", idx, shardRecord->key);
                return Status::Corruption();
            }
            if (NotInFields(attrName)) {
                continue;
            }
            doc->setField(attrName, attrValue);
        }
    }

    return Status::OK();
}

bool TabletDocIteratorBase::NotInFields(const string& fieldName) const
{
    return find(_fieldNames.begin(), _fieldNames.end(), fieldName) == _fieldNames.end();
}

void TabletDocIteratorBase::SetCheckPoint(const string& shardCheckpoint, std::string* checkpoint)
{
    checkpoint->assign((char*)&_shardIterId, sizeof(_shardIterId));
    checkpoint->append(shardCheckpoint);
}

} // namespace indexlibv2::table
