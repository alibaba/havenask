#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/common/dump_thread_pool.h"
#include "indexlib/index_base/segment/segment_container.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/document/document.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
#include <beeper/beeper.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, InMemorySegment);

InMemorySegment::InMemorySegment(const BuildConfig& buildConfig,
                                 const BlockMemoryQuotaControllerPtr& memController,
                                 const CounterMapPtr& counterMap)
    : mBuildConfig(buildConfig)
    , mDumpMemoryUse(0)
    , mIsSubSegment(false)
    , mCounterMap(counterMap)
    , mStatus(BUILDING)
{
    mMemoryController.reset(new UnsafeSimpleMemoryQuotaController(memController));
}

InMemorySegment::InMemorySegment(const InMemorySegment& other,
                                 BuildingSegmentData& segmentData)
    : mOperationWriter(other.mOperationWriter)
    , mBuildConfig(other.mBuildConfig)
    , mMemoryController(other.mMemoryController)
    , mDumpMemoryUse(other.mDumpMemoryUse)
    , mIsSubSegment(other.mIsSubSegment)
    , mCounterMap(other.mCounterMap)
    , mStatus(other.mStatus)
{
    mSegmentWriter.reset(other.mSegmentWriter->CloneWithNewSegmentData(segmentData));
    mSegmentData = segmentData;
    mSegmentReader = mSegmentWriter->CreateInMemSegmentReader();
    mSegmentInfo = mSegmentWriter->GetSegmentInfo();
}

InMemorySegment::~InMemorySegment() 
{
    if (!mIsSubSegment)
    {
        if (mSegmentWriter.use_count() > 1)
        {
            IE_LOG(ERROR, "mSegmentWriter.use_count: [%ld]", mSegmentWriter.use_count());
            IE_LOG(ERROR, "mSegmentWriter will not be released");
            assert(false);
        }

        // TODO keep consistent while two InMemorySegment hold same MemoryController
        if (mMemoryController.use_count() == 1)
        {
            IE_LOG(INFO, "free quota [%ld]", mMemoryController->GetUsedQuota());
        }
    }
}

void InMemorySegment::Init(const BuildingSegmentData& segmentData,
                           const SegmentWriterPtr& segmentWriter,
                           bool isSubSegment)
{
    assert(segmentWriter);
    mSegmentWriter = segmentWriter;
    mSegmentData = segmentData;
    mSegmentReader = segmentWriter->CreateInMemSegmentReader();
    mSegmentInfo = segmentWriter->GetSegmentInfo();
    mIsSubSegment = isSubSegment;
    if (!mIsSubSegment)
    {
        UpdateMemUse();
        IE_LOG(INFO, "use quota [%ld]", mMemoryController->GetUsedQuota());
    }
}

void InMemorySegment::Init(const BuildingSegmentData& segmentData, bool isSubSegment)
{
    mSegmentData = segmentData;
    mIsSubSegment = isSubSegment;
}

const DirectoryPtr& InMemorySegment::GetDirectory()
{
    if (!mSegmentData.GetDirectory())
    {
        mSegmentData.PrepareDirectory();
    }
    return mSegmentData.GetDirectory();
}

bool InMemorySegment::IsDirectoryDumped() const
{
    DirectoryPtr directory = mSegmentData.GetDirectory();
    if (directory && directory->IsExist(SEGMENT_INFO_FILE_NAME))
    {
        return true;
    }
    return false;
}

void InMemorySegment::BeginDump()
{
    EndSegment();
    if (mSubInMemSegment)
    {
        mSubInMemSegment->EndSegment();
    }
    if (!mIsSubSegment)
    {
        UpdateMemUse();
        IE_LOG(INFO, "used quota [%ld]", mMemoryController->GetUsedQuota());
        MultiThreadDump();
    }
}

void InMemorySegment::MultiThreadDump()
{
    IE_LOG(INFO, "MultiThreadDump begin");
    int64_t begin = TimeUtility::currentTime();
    DumpThreadPool dumpThreadPool(mBuildConfig.dumpThreadCount, 1024);

    if (!dumpThreadPool.start("indexDump")) {
        INDEXLIB_THROW(misc::RuntimeException, "create dump thread pool failed");
    }

    vector<common::DumpItem*> dumpItems;
    if (mSubInMemSegment)
    {
        mSubInMemSegment->CreateDumpItems(dumpItems);
    }

    CreateDumpItems(dumpItems);
    for (size_t i = 0; i < dumpItems.size(); ++i)
    {
        dumpThreadPool.pushWorkItem(dumpItems[i]);
    }
    dumpThreadPool.stop();
    mDumpMemoryUse = dumpThreadPool.getPeakOfMemoryUse();

    int64_t end = TimeUtility::currentTime();
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(
            INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "buildDump segment [%d], totalMemory [%lu], "
            "dumpMemoryUse [%lu], totol cost [%ld] ms",
            GetSegmentId(), GetTotalMemoryUse(),
            mDumpMemoryUse, (end - begin) / 1000);
    
    IE_LOG(INFO, "MultiThreadDump end");
}

void InMemorySegment::EndDump()
{
    if (mSubInMemSegment)
    {
        mSubInMemSegment->EndDump();
    }

    DirectoryPtr directory = GetDirectory();
    PackDirectoryPtr packDirectory = DYNAMIC_POINTER_CAST(
            PackDirectory, directory);
    if (packDirectory)
    {
        packDirectory->ClosePackageFileWriter();
    }

    DirectoryPtr unpackSegmentDir = mSegmentData.GetUnpackDirectory();
    if (mCounterMap)
    {
        string counterStr = mCounterMap->ToJsonString();
        unpackSegmentDir->Store(COUNTER_FILE_NAME, counterStr, false);
    }
    
    //first store deploy index and segment info file name will be written into it
    if (!mIsSubSegment)
    {
        DeployIndexWrapper::DumpSegmentDeployIndex(unpackSegmentDir, mSegmentInfo);
    }
    // segment info need to be flush out at last, because it represent a complete segment data 
    if (mSegmentInfo)
    {
        mSegmentInfo->Store(unpackSegmentDir);
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "EndDump %s %d, segment info : %s",
                mIsSubSegment ? "sub segment" : "segment", GetSegmentId(),
                mSegmentInfo->ToString(true).c_str());
    }
    else
    {
        //TODO: get last segment info
        SegmentInfo segmentInfo = mSegmentData.GetSegmentInfo();
        segmentInfo.Store(unpackSegmentDir);
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "EndDump %s %d, segment info : %s",
                mIsSubSegment ? "sub segment" : "segment", GetSegmentId(),
                segmentInfo.ToString(true).c_str());
    }

    UpdateMemUse();
}

timestamp_t InMemorySegment::GetTimestamp() const
{
    if (mSegmentInfo)
    {
        return mSegmentInfo->timestamp;
    }
    return INVALID_TIMESTAMP;
}

void InMemorySegment::UpdateMemUse()
{
    if (mIsSubSegment)
    {
        return;
    }
    
    int64_t currentUse = (int64_t)GetTotalMemoryUse();
    int64_t allocateMemUse = currentUse - mMemoryController->GetUsedQuota();
    if (allocateMemUse > 0)
    {
        mMemoryController->Allocate(allocateMemUse);
    }
}

void InMemorySegment::UpdateSegmentInfo(const DocumentPtr& doc)
{
    //TODO: update segment data?
        
    mSegmentInfo->Update(doc);

    if (mSubInMemSegment)
    {
        mSubInMemSegment->UpdateSegmentInfo(doc);
    }
}

void InMemorySegment::SetSubInMemorySegment(InMemorySegmentPtr inMemorySegment)
{
    assert(inMemorySegment->GetSegmentId() == GetSegmentId());
    mSubInMemSegment = inMemorySegment; 
}

void InMemorySegment::EndSegment()
{
    if (mSegmentWriter)
    {
        mSegmentWriter->EndSegment();
    }
}

void InMemorySegment::CreateDumpItems(vector<common::DumpItem*>& dumpItems)
{
    DirectoryPtr dumpDirectory = GetDirectory();
    if (mOperationWriter)
    {
        mOperationWriter->CreateDumpItems(dumpDirectory, dumpItems);
    }
    if (mSegmentWriter)
    {
        mSegmentWriter->CreateDumpItems(dumpDirectory, dumpItems);
    }
}

size_t InMemorySegment::GetTotalMemoryUse() const 
{
    size_t memUse = 0;
    if (mSegmentWriter)
    {
        memUse += mSegmentWriter->GetTotalMemoryUse();
    }
    if (mOperationWriter)
    {
        memUse += mOperationWriter->GetTotalMemoryUse();
    }
    return memUse;
}

void InMemorySegment::SetBaseDocId(exdocid_t docid)
{
    mSegmentData.SetBaseDocId(docid);
}

segmentid_t InMemorySegment::GetSegmentId() const
{
    return mSegmentData.GetSegmentId();
}

exdocid_t InMemorySegment::GetBaseDocId() const
{
    return mSegmentData.GetBaseDocId();
}

MemoryReserverPtr InMemorySegment::CreateMemoryReserver(const string& name)
{
    return MemoryReserverPtr(
        new MemoryReserver(name, mMemoryController->GetBlockMemoryController()));
}

int64_t InMemorySegment::GetUsedQuota() const
{
    if (mMemoryController)
    {
        return mMemoryController->GetUsedQuota();
    }
    return 0;
}

IE_NAMESPACE_END(index_base);

