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
#include "indexlib/index/kv/KVMerger.h"

#include <algorithm>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/AdapterKVSegmentReader.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/FixedLenHashTableCreator.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVSegmentReaderCreator.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/KeyMergeWriter.h"
#include "indexlib/index/kv/KeyWriter.h"
#include "indexlib/index/kv/NoneFilter.h"
#include "indexlib/index/kv/RecordFilter.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/SimpleMultiSegmentKVIterator.h"
#include "indexlib/index/kv/SortedMultiSegmentKVIterator.h"
#include "indexlib/index/kv/TTLFilter.h"
#include "indexlib/index/kv/VarLenHashTableCreator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVMerger);

KVMerger::KVMerger() {}

KVMerger::~KVMerger() {}

Status KVMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                      const std::map<std::string, std::any>& params)
{
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InvalidArgs("not an kv index config");
    }

    auto it = params.find(DROP_DELETE_KEY);
    if (it == params.end()) {
        return Status::InvalidArgs("no drop_delete_key in params");
    }
    std::string dropDeleteKey = std::any_cast<std::string>(it->second);
    if (!autil::StringUtil::fromString(dropDeleteKey, _dropDeleteKey)) {
        return Status::InvalidArgs("drop_delete_key in params not bool");
    }

    std::string currentTimeStr;
    it = params.find(CURRENT_TIME_IN_SECOND);
    if (it != params.end()) {
        currentTimeStr = std::any_cast<std::string>(it->second);
    }
    return CreateRecordFilter(currentTimeStr);
}

Status KVMerger::CreateRecordFilter(const std::string& currentTimeStr)
{
    if (!_indexConfig->TTLEnabled()) {
        _recordFilter = std::make_unique<NoneFilter>();
    } else {
        int64_t currentTsInSec;
        if (!autil::StringUtil::fromString(currentTimeStr, currentTsInSec) || currentTsInSec <= 0) {
            return Status::InvalidArgs("invalid timestamp: %s", currentTimeStr.c_str());
        }
        _recordFilter = std::make_unique<TTLFilter>(_indexConfig->GetTTL(), currentTsInSec);
    }
    return Status::OK();
}

Status KVMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                       const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    if (segMergeInfos.targetSegments.empty()) {
        return Status::OK();
    }
    if (segMergeInfos.targetSegments.size() > 1) {
        return Status::Unimplement("do not support merge source segments to multiple segments");
    }
    auto s = PrepareMerge(segMergeInfos);
    if (!s.IsOK()) {
        return s;
    }
    return DoMerge(segMergeInfos);
}

Status KVMerger::PrepareMerge(const SegmentMergeInfos& segMergeInfos)
{
    assert(segMergeInfos.targetSegments.size() == 1);
    const auto& targetSegment = segMergeInfos.targetSegments[0];
    _targetDir = PrepareTargetSegmentDirectory(targetSegment->segmentDir);
    if (!_targetDir) {
        return Status::IOError("prepare index directory for %s failed, segment id: %d",
                               _indexConfig->GetIndexName().c_str(), targetSegment->segmentId);
    }

    assert(targetSegment->schema != nullptr);
    _schemaId = targetSegment->schema->GetSchemaId();
    auto [s, sortDesc] = targetSegment->schema->GetSetting<config::SortDescriptions>("sort_descriptions");
    if (s.IsOK()) {
        _sortDescriptions = std::make_unique<config::SortDescriptions>(sortDesc);
    } else if (!s.IsNotFound()) {
        return s;
    }
    std::vector<SegmentStatistics> statVec;
    s = LoadSegmentStatistics(segMergeInfos, statVec);
    RETURN_IF_STATUS_ERROR(s, "load segment statistics failed");

    auto [keyMemoryUsage, _] = EstimateMemoryUsage(statVec);

    s = CreateKeyWriter(&_pool, keyMemoryUsage, statVec);
    if (!s.IsOK()) {
        return s;
    }

    return Status::OK();
}

Status KVMerger::DoMerge(const SegmentMergeInfos& segMergeInfos)
{
    auto s = MergeMultiSegments(segMergeInfos);
    if (!s.IsOK()) {
        return s;
    }

    s = Dump();
    if (!s.IsOK()) {
        return s;
    }

    FillSegmentMetrics(segMergeInfos.targetSegments[0]->segmentMetrics.get());

    return Status::OK();
}

std::shared_ptr<indexlib::file_system::Directory>
KVMerger::PrepareTargetSegmentDirectory(const std::shared_ptr<indexlib::file_system::Directory>& root) const
{
    auto impl = root->GetIDirectory();

    indexlib::file_system::DirectoryOption dirOption;
    auto result = impl->MakeDirectory(KV_INDEX_PATH, dirOption);
    if (!result.OK()) {
        AUTIL_LOG(ERROR, "make %s failed, error: %s", KV_INDEX_PATH.c_str(), result.Status().ToString().c_str());
        return nullptr;
    }
    auto indexDir = result.Value();
    indexlib::file_system::RemoveOption removeOption;
    removeOption.mayNonExist = true;
    auto s = indexDir->RemoveDirectory(_indexConfig->GetIndexName(), removeOption).Status();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "remove %s failed, error: %s", _indexConfig->GetIndexName().c_str(), s.ToString().c_str());
        return nullptr;
    }

    result = indexDir->MakeDirectory(_indexConfig->GetIndexName(), dirOption);
    if (!result.OK()) {
        AUTIL_LOG(ERROR, "make %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  result.Status().ToString().c_str());
        return nullptr;
    }

    return std::make_shared<indexlib::file_system::Directory>(result.Value());
}

KVTypeId KVMerger::MakeTypeId(const std::vector<SegmentStatistics>& statVec) const
{
    auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
    typeId.shortOffset = false; // disable short offset
    return typeId;
}

Status KVMerger::CreateKeyWriter(autil::mem_pool::PoolBase* pool, int64_t maxKeyMemoryUse,
                                 std::vector<SegmentStatistics>& statVec)
{
    _typeId = MakeTypeId(statVec);
    auto writer = std::make_unique<KeyMergeWriter>();
    auto s = writer->Init(_typeId);
    if (!s.IsOK()) {
        return s;
    }
    auto occupancyPct = _indexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    s = writer->AllocateMemory(pool, maxKeyMemoryUse, occupancyPct);
    if (!s.IsOK()) {
        return s;
    }
    _keyWriter = std::move(writer);
    return Status::OK();
}

Status KVMerger::AddRecord(const Record& record) { return _keyWriter->Add(record.key, record.value, record.timestamp); }

Status KVMerger::DeleteRecord(const Record& record) { return _keyWriter->Delete(record.key, record.timestamp); }

Status KVMerger::MergeMultiSegments(const SegmentMergeInfos& segMergeInfos)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    for (auto it = segMergeInfos.srcSegments.rbegin(); it != segMergeInfos.srcSegments.rend(); ++it) {
        segments.push_back(it->segment);
    }
    std::unique_ptr<MultiSegmentKVIterator> iterator;
    if (!_sortDescriptions || !_typeId.isVarLen) {
        iterator = std::make_unique<SimpleMultiSegmentKVIterator>(_schemaId, _indexConfig, _ignoreFieldCalculator,
                                                                  std::move(segments));
    } else {
        AUTIL_LOG(INFO, "sort merge by %s", ToJsonString(*_sortDescriptions, true).c_str());
        iterator = std::make_unique<SortedMultiSegmentKVIterator>(_schemaId, _indexConfig, _ignoreFieldCalculator,
                                                                  std::move(segments), *_sortDescriptions);
    }
    auto s = iterator->Init();
    if (!s.IsOK()) {
        return s;
    }
    autil::mem_pool::UnsafePool pool;
    while (iterator->HasNext()) {
        Record record;
        pool.reset();
        auto status = iterator->Next(&pool, record);
        if (!status.IsOK() && !status.IsEof()) {
            return status;
        }
        if (status.IsEof()) {
            break;
        }
        if (!_recordFilter->IsPassed(record)) {
            continue;
        }
        if (_dropDeleteKey) {
            if (_removedKeySet.count(record.key) > 0) {
                continue;
            }
            if (record.deleted) {
                _removedKeySet.insert(record.key);
                continue;
            }
        }
        if (_keyWriter->KeyExist(record.key)) {
            continue;
        }
        if (!record.deleted) {
            status = AddRecord(record);
        } else {
            status = DeleteRecord(record);
        }
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

Status KVMerger::LoadSegmentStatistics(const SegmentMergeInfos& segMergeInfos,
                                       std::vector<SegmentStatistics>& statVec) const
{
    const auto& groupName = _indexConfig->GetIndexName();
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        auto segmentMetrics = srcSegment.segment->GetSegmentMetrics();
        SegmentStatistics stat;
        if (!stat.Load(segmentMetrics.get(), groupName)) {
            return Status::InvalidArgs("load statistics for segment[%d] failed, groupName[%s]",
                                       srcSegment.segment->GetSegmentId(), groupName.c_str());
        }
        statVec.emplace_back(stat);
    }
    return Status::OK();
}

std::pair<int64_t, int64_t> KVMerger::EstimateMemoryUsage(const std::vector<SegmentStatistics>& statVec) const
{
    size_t totalKeyCount = 0;
    size_t maxKeyCountInSegment = 0;
    for (const auto& stat : statVec) {
        totalKeyCount += stat.keyCount;
        maxKeyCountInSegment = std::max(maxKeyCountInSegment, stat.keyCount);
    }

    int64_t memoryUsage = 0;

    // for writer
    auto typeId = MakeTypeId(statVec);

    std::unique_ptr<HashTableInfo> hashTableInfo;
    if (typeId.isVarLen) {
        hashTableInfo = VarLenHashTableCreator::CreateHashTableForWriter(typeId);
    } else {
        hashTableInfo = FixedLenHashTableCreator::CreateHashTableForWriter(typeId);
    }
    HashTableOptions options(_indexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct());
    options.mayStretch = true;

    auto hashTable = hashTableInfo->StealHashTable<HashTableBase>();
    int64_t keyMemoryUsage = hashTable->CapacityToTableMemory(totalKeyCount, options);
    memoryUsage += keyMemoryUsage;

    // estimate iterator memory usage
    options.occupancyPct = 100;
    memoryUsage += hashTable->CapacityToTableMemory(maxKeyCountInSegment, options);

    // buffers for reader/writer
    memoryUsage += indexlib::file_system::ReaderOption::DEFAULT_BUFFER_SIZE * 2;
    // key is compress ?
    memoryUsage += indexlib::file_system::WriterOption::DEFAULT_BUFFER_SIZE; // key

    return {keyMemoryUsage, memoryUsage};
}

Status KVMerger::InitIgnoreFieldCalculater(framework::TabletData* tabletData)
{
    _ignoreFieldCalculator = std::make_shared<index::AdapterIgnoreFieldCalculator>();
    if (!_ignoreFieldCalculator->Init(_indexConfig, tabletData)) {
        return Status::InternalError("init AdapterIgnoreFieldCalculator fail for index [%s]",
                                     _indexConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

} // namespace indexlibv2::index
