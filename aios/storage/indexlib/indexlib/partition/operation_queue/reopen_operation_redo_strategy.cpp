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
#include "indexlib/partition/operation_queue/reopen_operation_redo_strategy.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader_interface.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/on_disk_partition_data.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReopenOperationRedoStrategy);

ReopenOperationRedoStrategy::ReopenOperationRedoStrategy()
    : mIsIncConsistentWithRt(false)
    , mMaxTsInNewVersion(INVALID_TIMESTAMP)
    , mHasSubSchema(false)
    , mPkIndexType(it_primarykey64)
    , mEnableRedoSpeedup(true)
{
}

ReopenOperationRedoStrategy::~ReopenOperationRedoStrategy() {}

void ReopenOperationRedoStrategy::Init(const index_base::PartitionDataPtr& newPartitionData,
                                       const index_base::Version& lastVersion, const OnlineConfig& onlineConfig,
                                       const IndexPartitionSchemaPtr& schema)
{
    mEnableRedoSpeedup = onlineConfig.IsRedoSpeedupEnabled();
    Version newVersion = newPartitionData->GetOnDiskVersion();

    if (!mEnableRedoSpeedup) {
        IE_LOG(INFO, "ReopenOperationRedoStrategy is DISABLED in schema[%s] in version[%d]",
               schema->GetSchemaName().c_str(), newVersion.GetVersionId());
        return;
    }

    int64_t versionTsAlignment = onlineConfig.GetVersionTsAlignment();
    bool versionTsIsValid = true;
    if (newVersion.GetFormatVersion() >= 2 && (newVersion.GetTimestamp() % versionTsAlignment != 0)) {
        IE_LOG(WARN, "version[%d] with ts = [%ld] in schema[%s] is not aligned by [%ld] microseconds",
               newVersion.GetVersionId(), newVersion.GetTimestamp(), schema->GetSchemaName().c_str(),
               versionTsAlignment);
        versionTsIsValid = false;
    }
    if (lastVersion.GetFormatVersion() >= 2 && (lastVersion.GetTimestamp() % versionTsAlignment != 0)) {
        IE_LOG(WARN, "version[%d] with ts = [%ld] in schema[%s] is not aligned by [%ld] microseconds",
               lastVersion.GetVersionId(), lastVersion.GetTimestamp(), schema->GetSchemaName().c_str(),
               versionTsAlignment);
        versionTsIsValid = false;
    }

    if (!versionTsIsValid) {
        IE_LOG(WARN,
               "ReopenOperationRedoStrategy is DISABLED in schema[%s] in version[%d], "
               "due to unaligned version ts",
               schema->GetSchemaName().c_str(), newVersion.GetVersionId());
        mEnableRedoSpeedup = false;
        return;
    }

    mIsIncConsistentWithRt = onlineConfig.isIncConsistentWithRealtime && newVersion.GetFormatVersion() >= 2 &&
                             lastVersion.GetFormatVersion() >= 2;

    mMaxTsInNewVersion = GetMaxTimestamp(newPartitionData);
    InitSegmentMap(newVersion, lastVersion);
    mHasSubSchema = false;
    if (schema->GetSubIndexPartitionSchema()) {
        mHasSubSchema = true;
    }
    Version diffVersion = newVersion - lastVersion;
    const file_system::IFileSystemPtr& fileSystem = newPartitionData->GetRootDirectory()->GetFileSystem();
    if (!mIsIncConsistentWithRt) {
        OnDiskPartitionDataPtr diffPartitionData =
            OnDiskPartitionData::CreateOnDiskPartitionData(fileSystem, schema, diffVersion, "", true);
        mDiffPkReader = LoadPrimaryKeyIndexReader(diffPartitionData, schema);
        mDiffPartitionInfo = diffPartitionData->GetPartitionInfo();
    }
    IE_LOG(INFO,
           "ReopenOperationRedoStrategy Inited with lastVersion[%d], newVersion[%d], "
           "maxTsInNewVersion[%ld], isIncConsistentWithRt[%d]",
           lastVersion.GetVersionId(), newVersion.GetVersionId(), mMaxTsInNewVersion, mIsIncConsistentWithRt);
    mRmOpCreator.reset(new RemoveOperationCreator(schema));
}

PrimaryKeyIndexReaderPtr ReopenOperationRedoStrategy::LoadPrimaryKeyIndexReader(const PartitionDataPtr& partitionData,
                                                                                const IndexPartitionSchemaPtr& schema)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    IndexConfigPtr pkIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig) {
        return PrimaryKeyIndexReaderPtr();
    }
    PrimaryKeyIndexReaderPtr pkReader(
        IndexReaderFactory::CreatePrimaryKeyIndexReader(pkIndexConfig->GetInvertedIndexType()));
    assert(pkReader);
    const auto& legacyPkReader = std::dynamic_pointer_cast<index::LegacyPrimaryKeyReaderInterface>(pkReader);
    assert(legacyPkReader);
    legacyPkReader->OpenWithoutPKAttribute(pkIndexConfig, partitionData);
    mPkIndexType = indexSchema->GetPrimaryKeyIndexType();
    return pkReader;
}

void ReopenOperationRedoStrategy::InitSegmentMap(const index_base::Version& newVersion,
                                                 const index_base::Version& lastVersion)
{
    Version diffVersion = newVersion - lastVersion;
    Version inteVersion = newVersion - diffVersion;
    segmentid_t maxSegId = inteVersion.GetLastSegment();
    mBitmap.Alloc(maxSegId + 1);

    Version::Iterator iter = inteVersion.CreateIterator();
    while (iter.HasNext()) {
        segmentid_t segId = iter.Next();
        mBitmap.Set(segId);
    }
}

int64_t ReopenOperationRedoStrategy::GetMaxTimestamp(const index_base::PartitionDataPtr& partitionData)
{
    auto newVersion = partitionData->GetOnDiskVersion();
    size_t segCount = newVersion.GetSegmentCount();
    int64_t maxTsInVersion = INVALID_TIMESTAMP;
    for (size_t i = 0; i < segCount; ++i) {
        segmentid_t segId = newVersion[i];
        int64_t segTimestamp = partitionData->GetSegmentData(segId).GetSegmentInfo()->timestamp;
        maxTsInVersion = max(maxTsInVersion, segTimestamp);
    }
    return maxTsInVersion;
}

const set<segmentid_t>& ReopenOperationRedoStrategy::GetSkipDeleteSegments() const { return mSkipDelOpSegments; }

bool ReopenOperationRedoStrategy::NeedRedo(segmentid_t operationSegment, OperationBase* operation,
                                           OperationRedoHint& redoHint)
{
    if (!mEnableRedoSpeedup) {
        return true;
    }
    bool ret;
    auto docOpType = operation->GetDocOperateType();
    int64_t opTs = operation->GetTimestamp();
    if (docOpType == UPDATE_FIELD) {
        mRedoCounter.updateOpCount++;
        if (!mIsIncConsistentWithRt || opTs <= mMaxTsInNewVersion) {
            return true;
        }
        segmentid_t maxSegmentIdInVersion = (segmentid_t)(mBitmap.GetItemCount() - 1);
        segmentid_t segmentId = operation->GetSegmentId();
        if (segmentId > maxSegmentIdInVersion || segmentId == INVALID_SEGMENTID) {
            // operation effect in rt segments
            return true;
        }
        ret = !mBitmap.Test(segmentId);
        if (!ret) {
            mRedoCounter.skipRedoUpdateOpCount++;
        }
        return ret;
    }

    if (docOpType == DELETE_DOC) {
        mRedoCounter.deleteOpCount++;
        if (mHasSubSchema) {
            return true;
        }
        segmentid_t maxSegmentIdInVersion = (segmentid_t)(mBitmap.GetItemCount() - 1);
        segmentid_t segmentId = operation->GetSegmentId();
        if (segmentId > maxSegmentIdInVersion || segmentId == INVALID_SEGMENTID) {
            return true;
        }
        if (!mBitmap.Test(segmentId)) {
            return true;
        }
        // delete doc belongs to loaded segments
        mSkipDelOpSegments.insert(segmentId);
        if (mIsIncConsistentWithRt) {
            ret = (opTs <= mMaxTsInNewVersion);
            if (!ret) {
                mRedoCounter.skipRedoDeleteOpCount++;
            }
            return ret;
        }

        // isIncConsistentWithRt == false
        if (mDiffPkReader && mDiffPartitionInfo) {
            uint128_t pkHash = mRmOpCreator->GetPkHashFromOperation(operation, mPkIndexType);
            docid_t docIdInDiffVersion = mDiffPkReader->LookupWithPKHash(pkHash);
            if (docIdInDiffVersion == INVALID_DOCID) {
                // pk not found in diff segments, no need redo
                mRedoCounter.skipRedoDeleteOpCount++;
                return false;
            }
            mRedoCounter.hintOpCount++;
            pair<segmentid_t, docid_t> docInfo = mDiffPartitionInfo->GetLocalDocInfo(docIdInDiffVersion);
            redoHint.SetSegmentId(docInfo.first);
            redoHint.SetLocalDocId(docInfo.second);
        }
        return true;
    }
    mRedoCounter.otherOpCount++;
    return true;
}
}} // namespace indexlib::partition
