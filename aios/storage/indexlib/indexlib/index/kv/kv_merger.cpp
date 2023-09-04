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
#include "indexlib/index/kv/kv_merger.h"

#include "autil/EnvUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/memory.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/doc_reader_base.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_merge_writer.h"
#include "indexlib/index/kv/kv_segment_iterator.h"
#include "indexlib/index/kv/kv_ttl_decider.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index_define.h"
// #include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/util/metrics/ProgressMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVMerger);

KVMerger::KVMerger() : mCurrentTsInSecond(0), mTotalProgress(0), mCurrentProgress(0) {}

KVMerger::~KVMerger() {}

void KVMerger::Init(const index_base::PartitionDataPtr& partData, const IndexPartitionSchemaPtr& schema,
                    int64_t currentTs)
{
    mPartData = partData;
    mSchema = schema;
    mTTLDecider.reset(new KVTTLDecider);
    mTTLDecider->Init(schema);
    mKVIndexConfig = CreateDataKVIndexConfig(schema);
    mCurrentTsInSecond = MicrosecondToSecond(currentTs);

    mKVIndexConfig = CreateDataKVIndexConfig(mSchema);
    mPkeyAttrConfig = mSchema->GetRegionSchema(0)->CreateAttributeConfig(mKVIndexConfig->GetKeyFieldName());
    mKeyHasherType = mKVIndexConfig->GetKeyHashFunctionType();

    bool needMerge = false;
    if (autil::EnvUtil::getEnvWithoutDefault("NEED_MERGE", needMerge)) {
        mNeedMerge = needMerge;
    }

    bool storePKey = false;
    if (autil::EnvUtil::getEnvWithoutDefault(NEED_STORE_PKEY_VALUE, storePKey)) {
        mNeedStorePKeyValue = storePKey;
    }
}

void KVMerger::GenerateSegmentIds()
{
    reverse(mSegmentDataVec.begin(), mSegmentDataVec.end());
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        mSegmentIds.push_back(mSegmentDataVec[i].GetSegmentId());
    }
}

void KVMerger::MergeOneSegment(const SegmentData& segmentData, const KVMergeWriterPtr& writer, bool isBottomLevel)
{
    const std::shared_ptr<const SegmentInfo>& segInfo = segmentData.GetSegmentInfo();
    if (segInfo->docCount == 0) {
        return;
    }

    mCurrentDocId = 0;

    file_system::DirectoryPtr directory = segmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(mKVIndexConfig->GetIndexName(), true);

    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    if (kvDir->IsExist(KV_FORMAT_OPTION_FILE_NAME)) {
        std::string content;
        kvDir->Load(KV_FORMAT_OPTION_FILE_NAME, content);
        kvOptions->FromString(content);
    }

    KVSegmentIterator segmentIterator;
    // TODO: use schema
    if (!segmentIterator.Open(mSchema, kvOptions, segmentData)) {
        return;
    }
    while (segmentIterator.IsValid()) {
        ++mCurrentProgress;
        if (mMergeItemMetrics && (mCurrentProgress % 1000 == 0)) {
            double ratio = (mCurrentProgress < mTotalProgress) ? (double)mCurrentProgress / mTotalProgress : 1.0f;
            mMergeItemMetrics->UpdateCurrentRatio(ratio);
        }

        keytype_t pkeyHash;
        autil::StringView value;
        uint32_t timestamp;
        bool isDeleted;
        regionid_t regionId;
        segmentIterator.Get(pkeyHash, value, timestamp, isDeleted, regionId);
        if (mTTLDecider->IsExpiredItem(regionId, timestamp, mCurrentTsInSecond)) {
            mCurrentDocId++;
            segmentIterator.MoveToNext();
            continue;
        }
        if (isBottomLevel) {
            if (mRemovedKeySet.count(pkeyHash) > 0) {
                mCurrentDocId++;
                segmentIterator.MoveToNext();
                continue;
            }
            if (isDeleted) {
                mRemovedKeySet.insert(pkeyHash);
                mCurrentDocId++;
                segmentIterator.MoveToNext();
                continue;
            }
        }

        string pkeyStrValue;
        if (mNeedStorePKeyValue) {
            if (!mDocReader.ReadPkeyValue(mCurrentSegmentIdx, mCurrentDocId, pkeyHash, pkeyStrValue)) {
                IE_LOG(WARN, "read pkey value failed, pkeyHash[%lu], current segment idx[%ld]", pkeyHash,
                       mCurrentSegmentIdx);
            }
        }
        StringView pkeyValueData(pkeyStrValue.data(), pkeyStrValue.size());
        if (!writer->AddIfNotExist(pkeyHash, pkeyValueData, value, timestamp, isDeleted, regionId)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Insert key[%lu:%u:%d] failed, file[%s]", pkeyHash, timestamp,
                                 isDeleted, segmentData.GetDirectory()->DebugString().c_str());
        }
        mCurrentDocId++;
        segmentIterator.MoveToNext();
    }
}

void KVMerger::LoadSegmentMetrics(const SegmentDataVector& segmentDataVec,
                                  framework::SegmentMetricsVec& segmentMetricsVec)
{
    for (const auto& segmentData : segmentDataVec) {
        std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics());
        metrics->Load(segmentData.GetDirectory());
        segmentMetricsVec.push_back(metrics);
    }
}

bool KVMerger::GetSegmentDocs()
{
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        auto directory = mSegmentDataVec[i].GetDirectory();
        auto indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
        auto kvDir = indexDir->GetDirectory(mKVIndexConfig->GetIndexName(), true);
        KVFormatOptionsPtr kvOptions(new KVFormatOptions());
        if (kvDir->IsExist(KV_FORMAT_OPTION_FILE_NAME)) {
            std::string content;
            kvDir->Load(KV_FORMAT_OPTION_FILE_NAME, content);
            kvOptions->FromString(content);
        }
        KVSegmentIteratorPtr segmentIterator(new KVSegmentIterator());
        if (!segmentIterator->Open(mSchema, kvOptions, mSegmentDataVec[i], false)) {
            IE_LOG(WARN, "open segment iterator failed, segment [%s]", mSegmentDataVec[i].GetSegmentDirName().c_str());
            return false;
        }
        mSegmentDocCountVec.push_back(segmentIterator->Size());
        IE_LOG(INFO, "segment doc size = [%ld]", mSegmentDocCountVec[i]);
    }
    return true;
}

void KVMerger::Merge(const file_system::DirectoryPtr& segmentDir, const file_system::DirectoryPtr& indexDir,
                     const SegmentDataVector& segmentDataVec, const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    IE_LOG(INFO, "merge kv begin, dir[%s], isBottomLevel[%d], column[%lu]", indexDir->DebugString().c_str(),
           targetTopoInfo.isBottomLevel, targetTopoInfo.columnIdx);
    int64_t beginTime = TimeUtility::currentTime();

    if (mNeedMerge && mNeedStorePKeyValue) {
        mSegmentDataVec = segmentDataVec;
        GenerateSegmentIds();
        GetSegmentDocs();
        mDocReader.SetSchema(mSchema);
        mDocReader.SetSegmentDataVec(mSegmentDataVec);
        mDocReader.SetSegmentIds(mSegmentIds);
        mDocReader.SetSegmentDocCountVec(mSegmentDocCountVec);
        mDocReader.CreateSegmentAttributeIteratorVec(mPartData);
    }

    Pool pool;
    framework::SegmentMetricsVec segmentMetricsVec;
    LoadSegmentMetrics(segmentDataVec, segmentMetricsVec);

    // mergerWriter always disable shortOffset
    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    kvOptions->SetShortOffset(false);
    bool useCompactBucket = NeedCompactBucket(mKVIndexConfig, segmentMetricsVec, targetTopoInfo);
    kvOptions->SetUseCompactBucket(useCompactBucket);

    auto writer = KVFactory::CreateMergeWriter(mKVIndexConfig);
    assert(writer.operator bool());
    writer->Init(segmentDir, indexDir, mSchema, mKVIndexConfig, kvOptions, mNeedStorePKeyValue, &pool,
                 segmentMetricsVec, targetTopoInfo);

    IE_LOG(INFO, "merge pool allocated[%lu], used[%lu], total[%lu]", pool.getAllocatedSize(), pool.getUsedBytes(),
           pool.getTotalBytes());

    // merge
    const string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
    mTotalProgress = CalculateTotalProgress(groupName, segmentMetricsVec);
    mCurrentProgress = 0;

    if (mNeedMerge) {
        framework::SegmentMetricsVec::const_reverse_iterator metricsIt = segmentMetricsVec.rbegin();
        SegmentDataVector::const_reverse_iterator it = segmentDataVec.rbegin();

        for (; it != segmentDataVec.rend(); ++it, ++metricsIt) {
            int64_t begin = TimeUtility::currentTime();
            MergeOneSegment(*it, writer, targetTopoInfo.isBottomLevel);
            mCurrentSegmentIdx++;

            uint64_t columnDocCount = 0;
            bool ret = (*metricsIt)->Get<size_t>(groupName, KV_KEY_COUNT, columnDocCount);
            assert(ret);
            (void)(ret);
            IE_LOG(INFO,
                   "merge kv segment[%d] end, doc count[%lu/%lu], "
                   "dir[%s], time interval [%ldus]",
                   it->GetSegmentId(), columnDocCount, it->GetSegmentInfo()->docCount, indexDir->DebugString().c_str(),
                   TimeUtility::currentTime() - begin);
        }
    }

    Dump(writer, indexDir, groupName);

    IE_LOG(INFO, "merge kv end, dir[%s], time interval [%ldus]", indexDir->DebugString().c_str(),
           TimeUtility::currentTime() - beginTime);
}

bool KVMerger::NeedCompactBucket(const KVIndexConfigPtr& kvConfig, const framework::SegmentMetricsVec& segmentMetrics,
                                 const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    bool isBottom = targetTopoInfo.isBottomLevel;
    if (kvConfig->TTLEnabled()) {
        return false;
    }
    if (!kvConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline()) {
        return false;
    }
    int32_t valueSize = kvConfig->GetValueConfig()->GetFixedLength();
    if (valueSize > 8 || valueSize < 0) {
        return false;
    }
    int32_t keySize = kvConfig->IsCompactHashKey() ? sizeof(compact_keytype_t) : sizeof(keytype_t);
    if (keySize <= valueSize) {
        return false;
    }
    if (isBottom) {
        return true;
    }
    const string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
    for (auto& metric : segmentMetrics) {
        uint64_t deleteCount = 0;
        if (!metric->Get(groupName, KV_KEY_DELETE_COUNT, deleteCount) || deleteCount != 0) {
            return false;
        }
    }
    return true;
}

void KVMerger::Dump(const KVMergeWriterPtr& writer, const DirectoryPtr& indexDir, const string& groupName)
{
    IE_LOG(INFO, "merge kv, dump begin, dir[%s]", indexDir->DebugString().c_str());
    writer->OptimizeDump();
    IE_LOG(INFO, "merge kv, dump end, dir[%s]", indexDir->DebugString().c_str());

    // write segment metrics
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics());
    writer->FillSegmentMetrics(metrics, groupName);
    metrics->Store(indexDir);
}

int64_t KVMerger::EstimateMemoryUse(const index_base::SegmentDataVector& segmentDataVec,
                                    const index_base::SegmentTopologyInfo& targetTopoInfo) const
{
    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    kvOptions->SetShortOffset(false);
    KVMergeWriterPtr writer = KVFactory::CreateMergeWriter(mKVIndexConfig);
    KVSegmentIterator iterator;
    assert(writer);
    framework::SegmentMetricsVec segmentMetricsVec;
    LoadSegmentMetrics(segmentDataVec, segmentMetricsVec);
    int64_t writerUse = writer->EstimateMemoryUse(mKVIndexConfig, kvOptions, segmentMetricsVec, targetTopoInfo);
    int64_t iteratorUse = iterator.EstimateMemoryUse(mKVIndexConfig, kvOptions, segmentMetricsVec, targetTopoInfo);
    IE_LOG(INFO, "writerUse[%ld], iteratorUse[%ld]", writerUse, iteratorUse);
    return writerUse + iteratorUse;
}

size_t KVMerger::CalculateTotalProgress(const std::string& groupName,
                                        const framework::SegmentMetricsVec& segmentMetricsVec)
{
    size_t totalKeyCount = 0;
    for (size_t i = 0; i < segmentMetricsVec.size(); i++) {
        uint64_t keyCount = 0;
        bool ret = segmentMetricsVec[i]->Get<size_t>(groupName, KV_KEY_COUNT, keyCount);
        assert(ret);
        (void)(ret);

        totalKeyCount += keyCount;
    }
    return totalKeyCount;
}
}} // namespace indexlib::index
