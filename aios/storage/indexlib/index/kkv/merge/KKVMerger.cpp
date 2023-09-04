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
#include "indexlib/index/kkv/merge/KKVMerger.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/dump/KKVDataDumperFactory.h"
#include "indexlib/index/kkv/dump/KKVFileWriterOptionHelper.h"
namespace indexlibv2::index {
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, KKVMergerTyped, SKeyType);

void KKVMerger::FillSegmentMetrics(indexlib::framework::SegmentMetrics* segmentMetrics, const std::string& groupName,
                                   size_t pkeyCount, size_t skeyCount, size_t maxValueLen, size_t maxSkeyCount)
{
    KKVSegmentStatistics stat;
    stat.pkeyCount = pkeyCount;
    stat.skeyCount = skeyCount;
    stat.maxValueLen = maxValueLen;
    stat.maxSKeyCount = maxSkeyCount;
    if (segmentMetrics) {
        stat.Store(segmentMetrics, groupName);
    }
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      const std::map<std::string, std::any>& params)
{
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InvalidArgs("not an kkv index config");
    }

    auto it = params.find(KKV_DROP_DELETE_KEY);
    if (it == params.end()) {
        return Status::InvalidArgs("no drop_delete_key in params");
    }
    const std::string dropDeleteKey = std::any_cast<std::string>(it->second);
    if (!autil::StringUtil::fromString(dropDeleteKey, _dropDeleteKey)) {
        return Status::InvalidArgs("drop_delete_key in params not bool");
    }

    std::string currentTimeStr;
    it = params.find(KKV_CURRENT_TIME_IN_SECOND);
    if (it != params.end()) {
        currentTimeStr = std::any_cast<std::string>(it->second);
    }
    return CreateRecordFilter(currentTimeStr);
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::CreateRecordFilter(const std::string& currentTimeStr)
{
    if (!_indexConfig->TTLEnabled()) {
        _recordFilter = std::make_unique<KKVRecordFilter>();
    } else {
        int64_t currentTsInSec = 0;
        if (!autil::StringUtil::fromString(currentTimeStr, currentTsInSec) || currentTsInSec <= 0) {
            return Status::InvalidArgs("invalid timestamp: %s", currentTimeStr.c_str());
        }
        _currentTsInSec = currentTsInSec;
        _recordFilter = std::make_unique<KKVRecordFilter>(_indexConfig->GetTTL(), currentTsInSec);
        AUTIL_LOG(INFO, "ttl:[%lu] enabled currentTsInSec:[%lu]", _indexConfig->GetTTL(), currentTsInSec);
    }
    return Status::OK();
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::Merge(const SegmentMergeInfos& segMergeInfos,
                                       const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    if (segMergeInfos.targetSegments.empty()) {
        return Status::OK();
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(PrepareMerge(segMergeInfos));
    return DoMerge(segMergeInfos);
}

template <typename SKeyType>
StatusOr<std::shared_ptr<indexlib::file_system::IDirectory>>
KKVMergerTyped<SKeyType>::PrepareTargetSegmentDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& root)
{
    indexlib::file_system::DirectoryOption dirOption;
    auto result = root->MakeDirectory(KKV_INDEX_PATH, dirOption);
    auto status = result.Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "make %s failed, error: %s", KKV_INDEX_PATH, status.ToString().c_str());
        return {status.steal_error()};
    }
    auto indexDir = result.Value();

    indexlib::file_system::RemoveOption removeOption;
    removeOption.mayNonExist = true;
    status = indexDir->RemoveDirectory(_indexConfig->GetIndexName(), removeOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "remove %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  status.ToString().c_str());
        return {status.steal_error()};
    }

    result = indexDir->MakeDirectory(_indexConfig->GetIndexName(), dirOption);
    status = result.Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "make %s failed, error: %s", _indexConfig->GetIndexName().c_str(), status.ToString().c_str());
        return {status.steal_error()};
    }

    _indexDir = indexDir;
    return {result.Value()};
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::PrepareMerge(const SegmentMergeInfos& segMergeInfos)
{
    if (segMergeInfos.targetSegments.size() > 1) {
        return Status::Unimplement("do not support merge source segments to multiple segments");
    }
    const auto& targetSegment = segMergeInfos.targetSegments[0];
    auto r = PrepareTargetSegmentDirectory(targetSegment->segmentDir->GetIDirectory());
    if (!r.IsOK()) {
        AUTIL_LOG(ERROR, "prepare index directory for %s failed, segment id: %d", _indexConfig->GetIndexName().c_str(),
                  targetSegment->segmentId);
        return r.steal_error();
    }
    _targetDir = r.steal_value();

    assert(targetSegment->schema != nullptr);
    _schemaId = targetSegment->schema->GetSchemaId();
    std::vector<KKVSegmentStatistics> statVec;
    RETURN_STATUS_DIRECTLY_IF_ERROR(LoadSegmentStatistics(segMergeInfos, statVec));

    return Status::OK();
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::LoadSegmentStatistics(const SegmentMergeInfos& segMergeInfos,
                                                       std::vector<KKVSegmentStatistics>& statVec) const
{
    const auto& groupName = _indexConfig->GetIndexName();
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        auto segmentMetrics = srcSegment.segment->GetSegmentMetrics();
        KKVSegmentStatistics stat;
        if (!stat.Load(segmentMetrics.get(), groupName)) {
            return Status::InvalidArgs("load statistics for segment[%d] failed, groupName[%s]",
                                       srcSegment.segment->GetSegmentId(), groupName.c_str());
        }
        statVec.emplace_back(stat);
    }
    return Status::OK();
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::DoMerge(const SegmentMergeInfos& segMergeInfos)
{
    AUTIL_LOG(INFO, "merge kkv begin, target dir[%s], source segment size[%lu], target segment size[%lu]",
              _indexDir->DebugString().c_str(), segMergeInfos.srcSegments.size(), segMergeInfos.targetSegments.size());

    int64_t beginTime = autil::TimeUtility::currentTime();

    _iterator.reset(new OnDiskKKVIteratorTyped(_indexConfig, this->_iOConfig));
    RETURN_STATUS_DIRECTLY_IF_ERROR(_iterator->Init(segMergeInfos.srcSegments));

    auto phrase = _dropDeleteKey ? KKVDumpPhrase::MERGE_BOTTOMLEVEL : KKVDumpPhrase::MERGE;
    _dataDumper = KKVDataDumperFactory::Create<SKeyType>(_indexConfig, _storeTs, phrase);
    assert(_dataDumper != nullptr);

    uint64_t maxKeyCount = _iterator->GetEstimatePkeyCount();
    AUTIL_LOG(INFO, "estimate max pkey count [%lu]", maxKeyCount);

    auto skeyOption = KKVFileWriterOptionHelper::Create(_indexConfig->GetIndexPreference().GetSkeyParam(),
                                                        _iOConfig.writeBufferSize, _iOConfig.enableAsyncWrite);
    auto valueOption = KKVFileWriterOptionHelper::Create(_indexConfig->GetIndexPreference().GetValueParam(),
                                                         _iOConfig.writeBufferSize, _iOConfig.enableAsyncWrite);
    RETURN_STATUS_DIRECTLY_IF_ERROR(_dataDumper->Init(_targetDir, skeyOption, valueOption, maxKeyCount));

    uint64_t count = 0;
    while (_iterator->IsValid()) {
        OnDiskSinglePKeyIteratorTyped* dataIter = _iterator->GetCurrentIterator();
        RETURN_STATUS_DIRECTLY_IF_ERROR(CollectSinglePrefixKey(dataIter, _dropDeleteKey));
        _iterator->MoveToNext();
        ++count;
        if (count % 100000 == 0) {
            AUTIL_LOG(INFO, "already merge [%lu] pkey, memory peak for simple pool is [%lu]", count,
                      _simplePool.getPeakOfUsedBytes());
        }
        if (_mergeItemMetrics && count % 1000 == 0) {
            _mergeItemMetrics->UpdateCurrentRatio(_iterator->GetMergeProgressRatio());
        }
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(_dataDumper->Close());

    FillSegmentMetrics(segMergeInfos.targetSegments[0]->segmentMetrics.get(), _indexConfig->GetIndexName(),
                       _dataDumper->GetPKeyCount(), _dataDumper->GetTotalSKeyCount(), _dataDumper->GetMaxValueLen(),
                       _dataDumper->GetMaxSKeyCount());

    int64_t endTime = autil::TimeUtility::currentTime();
    AUTIL_LOG(INFO, "merge kkv end, target dir[%s], time interval [%ld]ms", _indexDir->DebugString().c_str(),
              (endTime - beginTime) / 1000);
    return Status::OK();
}

template <typename SKeyType>
inline void KKVMergerTyped<SKeyType>::MoveToFirstValidSKeyPosition(OnDiskSinglePKeyIteratorTyped* dataIter,
                                                                   bool isBottomLevel)
{
    assert(dataIter);
    while (dataIter->IsValid()) {
        uint32_t timestamp = dataIter->GetCurrentTs();
        if (!_recordFilter->IsPassed(timestamp) && !_indexConfig->StoreExpireTime()) {
            // over ttl
            dataIter->MoveToNext();
            continue;
        }

        if (isBottomLevel) {
            bool skeyDeleted = dataIter->CurrentSkeyDeleted();
            if (skeyDeleted || dataIter->CurrentSKeyExpired(_currentTsInSec)) {
                dataIter->MoveToNext();
                continue;
            }
        }
        break;
    }
}

template <typename SKeyType>
inline void KKVMergerTyped<SKeyType>::MoveToNextValidSKeyPosition(OnDiskSinglePKeyIteratorTyped* dataIter,
                                                                  bool isBottomLevel)
{
    dataIter->MoveToNext();
    MoveToFirstValidSKeyPosition(dataIter, isBottomLevel);
}

template <typename SKeyType>
Status KKVMergerTyped<SKeyType>::CollectSinglePrefixKey(OnDiskSinglePKeyIteratorTyped* dataIter, bool isBottomLevel)
{
    assert(dataIter);
    assert(_dataDumper);

    MoveToFirstValidSKeyPosition(dataIter, isBottomLevel);
    if (!isBottomLevel && dataIter->HasPKeyDeleted()) {
        uint32_t deletePKeyTs = dataIter->GetPKeyDeletedTs();
        if (_recordFilter->IsPassed(deletePKeyTs)) {
            auto pkeyHash = dataIter->GetPKeyHash();
            KKVDoc doc;
            doc.timestamp = deletePKeyTs;
            auto status = _dataDumper->Dump(pkeyHash, /*isDeletedPkey*/ true,
                                            /*isLastNode*/ !dataIter->IsValid(), doc);
            RETURN_IF_STATUS_ERROR(status, "failed to dump deleted pkey:[%lu]", pkeyHash);
        }
    }

    while (dataIter->IsValid()) {
        KKVDoc doc;
        doc.skey = dataIter->GetCurrentSkey();
        doc.skeyDeleted = dataIter->CurrentSkeyDeleted();
        doc.timestamp = dataIter->GetCurrentTs();
        doc.expireTime = dataIter->GetCurrentExpireTime();
        doc.skeyDeleted = doc.skeyDeleted || dataIter->CurrentSKeyExpired(_currentTsInSec);
        if (!doc.skeyDeleted) {
            dataIter->GetCurrentValue(doc.value);
        }
        auto pkeyHash = dataIter->GetPKeyHash();
        MoveToNextValidSKeyPosition(dataIter, isBottomLevel);
        auto status = _dataDumper->Dump(pkeyHash, /*isDeletedPkey*/ false,
                                        /*isLastNode*/ !dataIter->IsValid(), doc);
        RETURN_IF_STATUS_ERROR(status, "failed to dump pkey:[%lu]", pkeyHash);
    }
    return Status::OK();
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVMergerTyped);

} // namespace indexlibv2::index
