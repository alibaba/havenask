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
#include "indexlib/merger/document_reclaimer/document_reclaimer.h"

#include "autil/StringUtil.h"
#include "beeper/beeper.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_creator.h"
#include "indexlib/merger/document_reclaimer/obsolete_doc_reclaimer.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, DocumentReclaimer);

DocumentReclaimer::DocumentReclaimer(const SegmentDirectoryPtr& segmentDirectory, const IndexPartitionSchemaPtr& schema,
                                     const IndexPartitionOptions& options, int64_t currentTs,
                                     MetricProviderPtr metricProvider)
    : mSegmentDirectory(segmentDirectory)
    , mSchema(schema)
    , mOptions(options)
    , mCurrentTs(currentTs)
    , mMetricProvider(metricProvider)
{
    mDocReclaimConfigPath = options.GetMergeConfig().docReclaimConfigPath;
}

DocumentReclaimer::~DocumentReclaimer() {}

void DocumentReclaimer::Reclaim()
{
    bool needReclaim = false;
    if (mSchema->TTLEnabled()) {
        needReclaim = true;
    }

    if (!mDocReclaimConfigPath.empty()) {
        LoadIndexReclaimerParams();
        if (mIndexReclaimerParams.size() != 0) {
            needReclaim = true;
        }
    }

    if (!needReclaim) {
        return;
    }

    MultiPartSegmentDirectoryPtr segDir = DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory);
    if (segDir) {
        ReclaimMultiPartition(segDir);
    } else {
        ReclaimSinglePartition(mSegmentDirectory);
    }
    IE_LOG(INFO, "document reclaim done: config path[%s], config content[%s]", mDocReclaimConfigPath.c_str(),
           mReclaimParamJsonStr.c_str())
}

void DocumentReclaimer::ReclaimSinglePartition(const SegmentDirectoryPtr& segDir)
{
    auto tableType = mSchema->GetTableType();
    if (tableType == tt_index) {
        ReclaimIndexTable(segDir);
        return;
    }

    if (tableType == tt_kv || tableType == tt_kkv) {
        RemoveExpiredSegments(segDir);
        return;
    }
}

void DocumentReclaimer::RemoveExpiredSegments(const SegmentDirectoryPtr& segDir)
{
    // TODO(TTL): remove segment by max expire time
    if (!mSchema->TTLEnabled() || !mOptions.GetMergeConfig().enableReclaimExpiredDoc) {
        return;
    }
    int64_t ttl = SecondToMicrosecond(mOptions.GetBuildConfig().ttl);
    if (ttl < 0) {
        ttl = SecondToMicrosecond(mSchema->GetDefaultTTL());
    }

    PartitionDataPtr partitionData = segDir->GetPartitionData();
    for (auto iter = partitionData->Begin(); iter != partitionData->End(); iter++) {
        const std::shared_ptr<const SegmentInfo>& segmentInfo = iter->GetSegmentInfo();
        if (mCurrentTs - segmentInfo->timestamp > ttl) {
            IE_LOG(INFO, "reached ttl[%ld], remove segment[%d] of ts[%ld], current ts[%ld]", ttl, iter->GetSegmentId(),
                   segmentInfo->timestamp, mCurrentTs);
            segDir->RemoveSegment(iter->GetSegmentId());
        }
    }
    segDir->CommitVersion();
    IE_LOG(INFO, "remove expired segments done");
}

void DocumentReclaimer::ReclaimIndexTable(const SegmentDirectoryPtr& segDir)
{
    auto rootDirectory = segDir->GetRootDir();
    SegmentDirectory::RemoveUselessSegment(rootDirectory);

    ObsoleteDocReclaimerPtr obsoleteDocReclaimer;
    if (mSchema->TTLEnabled() && mOptions.GetMergeConfig().enableReclaimExpiredDoc) {
        obsoleteDocReclaimer.reset(new ObsoleteDocReclaimer(mSchema, mMetricProvider));
        obsoleteDocReclaimer->RemoveObsoleteSegments(segDir);
    }
    DocumentDeleterPtr docDeleter = CreateDocumentDeleter(segDir);
    if (obsoleteDocReclaimer) {
        obsoleteDocReclaimer->Reclaim(docDeleter, segDir);
    }

    DeleteMatchedDocument(segDir, docDeleter);
    if (docDeleter->IsDirty()) {
        DumpSegmentAndCommitVersion(segDir, docDeleter);
    }
}

void DocumentReclaimer::ReclaimMultiPartition(const MultiPartSegmentDirectoryPtr& segDir)
{
    const vector<Version>& versions = segDir->GetVersions();
    const vector<file_system::DirectoryPtr>& rootDirs = segDir->GetRootDirs();
    assert(versions.size() == rootDirs.size());
    vector<Version> newVersions;
    bool hasSub = mSchema->GetSubIndexPartitionSchema().operator bool();
    for (size_t i = 0; i < versions.size(); ++i) {
        SegmentDirectoryPtr segDir(new SegmentDirectory(rootDirs[i], versions[i]));
        segDir->Init(hasSub);
        ReclaimSinglePartition(segDir);
        newVersions.push_back(segDir->GetVersion());
    }
    segDir->Reload(newVersions);
}

void DocumentReclaimer::LoadIndexReclaimerParams()
{
    mIndexReclaimerParams.clear();
    mReclaimParamJsonStr = "";
    auto ec = FslibWrapper::AtomicLoad(mDocReclaimConfigPath, mReclaimParamJsonStr).Code();
    if (ec == FSEC_NOENT) {
        IE_LOG(INFO, "document reclaim config file[%s] does not exist", mDocReclaimConfigPath.c_str());
        return;
    }
    THROW_IF_FS_ERROR(ec, "load document reclaim config file[%s] failed", mDocReclaimConfigPath.c_str());

    try {
        FromJsonString(mIndexReclaimerParams, mReclaimParamJsonStr);
    } catch (const util::ExceptionBase& e) {
        INDEXLIB_FATAL_ERROR(BadParameter, "document reclaim config[%s] FromJsonString failed",
                             mReclaimParamJsonStr.c_str());
    }
}

DocumentDeleterPtr DocumentReclaimer::CreateDocumentDeleter(const SegmentDirectoryPtr& segDir) const
{
    const PartitionDataPtr& partitionData = segDir->GetPartitionData();
    DocumentDeleterPtr deleter(new DocumentDeleter(mSchema));
    deleter->Init(partitionData);
    return deleter;
}

void DocumentReclaimer::DeleteMatchedDocument(const SegmentDirectoryPtr& segDir,
                                              const DocumentDeleterPtr& docDeleter) const
{
    if (mIndexReclaimerParams.size() == 0) {
        return;
    }

    const PartitionDataPtr& partitionData = segDir->GetPartitionData();
    IndexReclaimerCreator creator(mSchema, partitionData, mMetricProvider);
    IE_LOG(INFO, "begin DeleteMatchDocument with reclaim params");
    for (size_t i = 0; i < mIndexReclaimerParams.size(); ++i) {
        IndexReclaimerPtr indexReclaimer(creator.Create(mIndexReclaimerParams[i]));
        if (indexReclaimer) {
            indexReclaimer->Reclaim(docDeleter);
        } else {
            string paramStr = ToJsonString(mIndexReclaimerParams[i]);
            INDEXLIB_FATAL_ERROR(BadParameter, "create reclaimer for param[%s] failed", paramStr.c_str());
        }
    }
    IE_LOG(INFO, "end DeleteMatchDocument with reclaim params");
}

void DocumentReclaimer::DumpSegmentAndCommitVersion(const SegmentDirectoryPtr& segDir,
                                                    const DocumentDeleterPtr& docDeleter) const
{
    SegmentInfo segInfo;
    segInfo.schemaId = mSchema->GetSchemaVersionId();
    const string& latestCounterMap = segDir->GetLatestCounterMapContent();
    DirectoryPtr directory = segDir->CreateNewSegment(segInfo);
    if (!directory) {
        INDEXLIB_FATAL_ERROR(FileIO, "create new segment failed");
    }
    docDeleter->Dump(directory);
    if (mSchema->TTLEnabled()) {
        segInfo.maxTTL = docDeleter->GetMaxTTL();
    }
    WriterOption couterWriterOption = WriterOption::AtomicDump();
    couterWriterOption.notInPackage = true;
    if (mSchema->GetSubIndexPartitionSchema()) {
        // store sub segment info first
        DirectoryPtr subDirectory = directory->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
        subDirectory->Store(COUNTER_FILE_NAME, latestCounterMap, couterWriterOption);
        segInfo.Store(subDirectory);

        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "DocumentReclaimer create sub segment %d, segment info : %s",
                                          *segDir->GetNewSegmentIds().rbegin(), segInfo.ToString(true).c_str());
    }
    directory->Store(COUNTER_FILE_NAME, latestCounterMap, couterWriterOption);
    SegmentInfoPtr segInfoPtr(new SegmentInfo(segInfo));
    if (mSchema->EnableTemperatureLayer()) {
        SegmentTemperatureMeta meta = SegmentTemperatureMeta::GenerateDefaultSegmentMeta();
        meta.segmentId = *segDir->GetNewSegmentIds().rbegin();
        segDir->AddSegmentTemperatureMeta(meta);
        segInfo.AddDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, ToJsonString(meta.GenerateCustomizeMetric()));
    }

    if (mSchema->GetFileCompressSchema()) {
        uint64_t fingerPrint = mSchema->GetFileCompressSchema()->GetFingerPrint();
        segInfo.AddDescription(SEGMENT_COMPRESS_FINGER_PRINT, autil::StringUtil::toString(fingerPrint));
    }
    DeployIndexWrapper::DumpSegmentDeployIndex(directory, segInfoPtr);
    segInfo.Store(directory);
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "DocumentReclaimer create segment %d, segment info : %s",
                                      *segDir->GetNewSegmentIds().rbegin(), segInfo.ToString(true).c_str());
    directory->Close();
    segDir->CommitVersion();
}
}} // namespace indexlib::merger
