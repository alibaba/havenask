#include <autil/StringUtil.h>
#include <beeper/beeper.h>
#include "indexlib/index_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/merger/document_reclaimer/document_reclaimer.h"
#include "indexlib/merger/document_reclaimer/obsolete_doc_reclaimer.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, DocumentReclaimer);

DocumentReclaimer::DocumentReclaimer(
        const SegmentDirectoryPtr& segmentDirectory,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        int64_t currentTs,
        MetricProviderPtr metricProvider)
    : mSegmentDirectory(segmentDirectory)
    , mSchema(schema)
    , mOptions(options)
    , mCurrentTs(currentTs)
    , mMetricProvider(metricProvider)
{
    mDocReclaimConfigPath = options.GetMergeConfig().docReclaimConfigPath;
}

DocumentReclaimer::~DocumentReclaimer() 
{
}

void DocumentReclaimer::Reclaim()
{
    bool needReclaim = false;
    if (mSchema->TTLEnabled())
    {
        needReclaim = true;
    }
    
    if (!mDocReclaimConfigPath.empty())
    {
        LoadIndexReclaimerParams();
        if (mIndexReclaimerParams.size() != 0)
        {
            needReclaim = true;
        }
    }
    
    if (!needReclaim)
    {
        return;
    }
    
    MultiPartSegmentDirectoryPtr segDir =
        DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory,
                             mSegmentDirectory);
    if (segDir)
    {
        ReclaimMultiPartition(segDir);
    }
    else
    {
        ReclaimSinglePartition(mSegmentDirectory);
    }
    IE_LOG(INFO, "document reclaim done: config path[%s], config content[%s]", 
           mDocReclaimConfigPath.c_str(), mReclaimParamJsonStr.c_str())
}

void DocumentReclaimer::ReclaimSinglePartition(const SegmentDirectoryPtr& segDir)
{
    auto tableType = mSchema->GetTableType();
    if (tableType == tt_index)
    {
        ReclaimIndexTable(segDir);
        return;
    }

    if (tableType == tt_kv || tableType == tt_kkv)
    {
        RemoveExpiredSegments(segDir);
        return;
    }
}

void DocumentReclaimer::RemoveExpiredSegments(const SegmentDirectoryPtr& segDir)
{
    if (!mSchema->TTLEnabled() || !mOptions.GetMergeConfig().enableReclaimExpiredDoc)
    {
        return;
    }
    int64_t ttl = SecondToMicrosecond(mOptions.GetBuildConfig().ttl);
    if (ttl < 0)
    {
        ttl = SecondToMicrosecond(mSchema->GetDefaultTTL());
    }
    
    PartitionDataPtr partitionData = segDir->GetPartitionData();
    for (auto iter = partitionData->Begin(); iter != partitionData->End(); iter++)
    {
        auto segmentInfo = iter->GetSegmentInfo();
        if (mCurrentTs - segmentInfo.timestamp > ttl)
        {
            IE_LOG(INFO, "reached ttl[%ld], remove segment[%d] of ts[%ld], current ts[%ld]",
                   ttl, iter->GetSegmentId(), segmentInfo.timestamp, mCurrentTs);
            segDir->RemoveSegment(iter->GetSegmentId());
        }
    }
    segDir->CommitVersion();
    IE_LOG(INFO, "remove expired segments done");
}

void DocumentReclaimer::ReclaimIndexTable(const SegmentDirectoryPtr& segDir)
{
    string rootPath = segDir->GetRootDir();
    SegmentDirectory::RemoveUselessSegment(rootPath);
    
    ObsoleteDocReclaimerPtr obsoleteDocReclaimer;
    if (mSchema->TTLEnabled() && mOptions.GetMergeConfig().enableReclaimExpiredDoc)
    {
        obsoleteDocReclaimer.reset(new ObsoleteDocReclaimer(mSchema, mMetricProvider));
        obsoleteDocReclaimer->RemoveObsoleteSegments(segDir);
    }
    DocumentDeleterPtr docDeleter = CreateDocumentDeleter(segDir);
    if (obsoleteDocReclaimer)
    {
        obsoleteDocReclaimer->Reclaim(docDeleter, segDir);
    }
    
    DeleteMatchedDocument(segDir, docDeleter);
    if (docDeleter->IsDirty())
    {
        DumpSegmentAndCommitVersion(segDir, docDeleter);
    }
}

void DocumentReclaimer::ReclaimMultiPartition(
        const MultiPartSegmentDirectoryPtr& segDir)
{
    const vector<Version> &versions = segDir->GetVersions();
    const vector<string> &rootDirs = segDir->GetRootDirs();
    assert(versions.size() == rootDirs.size());
    vector<Version> newVersions;
    bool hasSub = mSchema->GetSubIndexPartitionSchema();
    for (size_t i = 0; i < versions.size(); ++i)
    {
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
    if (!storage::FileSystemWrapper::AtomicLoad(mDocReclaimConfigPath, mReclaimParamJsonStr, true))
    {
        IE_LOG(INFO, "document reclaim config file[%s] does not exist", mDocReclaimConfigPath.c_str());
        return;
    }
    
    try
    {
        FromJsonString(mIndexReclaimerParams, mReclaimParamJsonStr);
    }
    catch (const misc::ExceptionBase& e)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, 
                             "document reclaim config[%s] FromJsonString failed", 
                             mReclaimParamJsonStr.c_str());
    }
}

DocumentDeleterPtr DocumentReclaimer::CreateDocumentDeleter(
        const SegmentDirectoryPtr& segDir) const
{
    const PartitionDataPtr& partitionData = segDir->GetPartitionData();
    DocumentDeleterPtr deleter(new DocumentDeleter(mSchema));
    deleter->Init(partitionData);
    return deleter;
}

void DocumentReclaimer::DeleteMatchedDocument(const SegmentDirectoryPtr& segDir,
        const DocumentDeleterPtr& docDeleter) const
{
    if (mIndexReclaimerParams.size() == 0)
    {
        return;
    }
    
    const PartitionDataPtr& partitionData = segDir->GetPartitionData();
    IndexReclaimerCreator creator(mSchema, partitionData, mMetricProvider);
    IE_LOG(INFO, "begin DeleteMatchDocument with reclaim params");
    for (size_t i = 0; i < mIndexReclaimerParams.size(); ++i)
    {
        IndexReclaimerPtr indexReclaimer(creator.Create(mIndexReclaimerParams[i]));
        if (indexReclaimer)
        {
            indexReclaimer->Reclaim(docDeleter);
        }
        else 
        {
            string paramStr = ToJsonString(mIndexReclaimerParams[i]);
            INDEXLIB_FATAL_ERROR(BadParameter, "create reclaimer for param[%s] failed",
                                 paramStr.c_str());
        }
    }
    IE_LOG(INFO, "end DeleteMatchDocument with reclaim params");
}

void DocumentReclaimer::DumpSegmentAndCommitVersion(
        const SegmentDirectoryPtr& segDir, 
        const DocumentDeleterPtr& docDeleter) const
{
    SegmentInfo segInfo;
    segInfo.schemaId = mSchema->GetSchemaVersionId();
    const string& latestCounterMap = segDir->GetLatestCounterMapContent();
    DirectoryPtr directory = segDir->CreateNewSegment(segInfo);
    if (!directory)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "create new segment failed");
    }
    docDeleter->Dump(directory);
    if (mSchema->TTLEnabled())
    {
        segInfo.maxTTL = docDeleter->GetMaxTTL();
    }
    if (mSchema->GetSubIndexPartitionSchema())
    {
        // store sub segment info first
        DirectoryPtr subDirectory = directory->GetDirectory(
                SUB_SEGMENT_DIR_NAME, true);
        subDirectory->Store(COUNTER_FILE_NAME, latestCounterMap, true);
        segInfo.Store(subDirectory);

        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "DocumentReclaimer create sub segment %d, segment info : %s",
                *segDir->GetNewSegmentIds().rbegin(),
                segInfo.ToString(true).c_str());
    }
    directory->Store(COUNTER_FILE_NAME, latestCounterMap, true);
    SegmentInfoPtr segInfoPtr(new SegmentInfo(segInfo));
    DeployIndexWrapper::DumpSegmentDeployIndex(directory, segInfoPtr);
    segInfo.Store(directory);
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "DocumentReclaimer create segment %d, segment info : %s",
            *segDir->GetNewSegmentIds().rbegin(),
            segInfo.ToString(true).c_str());
    segDir->CommitVersion();
}

IE_NAMESPACE_END(merger);

