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
#include "indexlib/index_base/segment/in_memory_segment.h"

#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/common/dump_thread_pool.h"
#include "indexlib/document/document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_container.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, InMemorySegment);

InMemorySegment::InMemorySegment(const BuildConfig& buildConfig, const BlockMemoryQuotaControllerPtr& memController,
                                 const CounterMapPtr& counterMap)
    : mBuildConfig(buildConfig)
    , mDumpMemoryUse(0)
    , mIsSubSegment(false)
    , mCounterMap(counterMap)
    , mStatus(BUILDING)
{
    mMemoryController.reset(new UnsafeSimpleMemoryQuotaController(memController));
}

InMemorySegment::InMemorySegment(const InMemorySegment& other, BuildingSegmentData& segmentData,
                                 InMemorySegment::CloneType type)
    : mOperationWriter(other.mOperationWriter)
    , mBuildConfig(other.mBuildConfig)
    , mMemoryController(other.mMemoryController)
    , mDumpMemoryUse(other.mDumpMemoryUse)
    , mIsSubSegment(other.mIsSubSegment)
    , mCounterMap(other.mCounterMap)
    , mStatus(other.mStatus)
{
    mSegmentWriter.reset(
        other.mSegmentWriter->CloneWithNewSegmentData(segmentData, type == InMemorySegment::CT_SHARED));

    mSegmentData = segmentData;
    mSegmentReader = mSegmentWriter->CreateInMemSegmentReader();
    mSegmentInfo = mSegmentWriter->GetSegmentInfo();
}

InMemorySegment::~InMemorySegment()
{
    if (!mIsSubSegment) {
        if (mSegmentWriter.use_count() > 1) {
            IE_LOG(WARN, "mSegmentWriter.use_count: [%ld]", mSegmentWriter.use_count());
            IE_LOG(WARN, "mSegmentWriter will not be released");
        }

        // TODO keep consistent while two InMemorySegment hold same MemoryController
        if (mMemoryController.use_count() == 1) {
            IE_LOG(INFO, "free quota [%ld]", mMemoryController->GetUsedQuota());
        }
    }
}

void InMemorySegment::Init(const BuildingSegmentData& segmentData, const SegmentWriterPtr& segmentWriter,
                           bool isSubSegment)
{
    assert(segmentWriter);
    mSegmentWriter = segmentWriter;
    mSegmentData = segmentData;
    mSegmentReader = segmentWriter->CreateInMemSegmentReader();
    mSegmentInfo = segmentWriter->GetSegmentInfo();
    mIsSubSegment = isSubSegment;
    if (!mIsSubSegment) {
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
    if (!mSegmentData.GetDirectory()) {
        mSegmentData.PrepareDirectory();
    }
    return mSegmentData.GetDirectory();
}

bool InMemorySegment::IsDirectoryDumped() const
{
    DirectoryPtr directory = mSegmentData.GetDirectory();
    if (directory && directory->IsExist(SEGMENT_INFO_FILE_NAME)) {
        return true;
    }
    return false;
}

void InMemorySegment::BeginDump()
{
    if (!mIsSubSegment) {
        UpdateMemUse();
        IE_LOG(INFO, "used quota [%ld]", mMemoryController->GetUsedQuota());
        MultiThreadDump();
    }
}

void InMemorySegment::MultiThreadDump()
{
    autil::ScopedTime2 timer;
    IE_LOG(INFO, "MultiThreadDump begin");
    int64_t begin = TimeUtility::currentTime();
    DumpThreadPool dumpThreadPool(mBuildConfig.dumpThreadCount, 1024);

    if (!dumpThreadPool.start("indexDump")) {
        INDEXLIB_THROW(util::RuntimeException, "create dump thread pool failed");
    }

    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    if (mSubInMemSegment) {
        mSubInMemSegment->CreateDumpItems(dumpItems);
    }

    CreateDumpItems(dumpItems);
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        dumpThreadPool.pushWorkItem(dumpItems[i].release());
    }
    dumpThreadPool.stop();
    mDumpMemoryUse = dumpThreadPool.getPeakOfMemoryUse();

    int64_t end = TimeUtility::currentTime();
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "buildDump segment [%d], totalMemory [%lu], "
                                      "dumpMemoryUse [%lu], totol cost [%ld] ms",
                                      GetSegmentId(), GetTotalMemoryUse(), mDumpMemoryUse, (end - begin) / 1000);

    IE_LOG(INFO, "MultiThreadDump end, used[%.3f]s", timer.done_sec());
}

void InMemorySegment::EndDump()
{
    if (mSubInMemSegment) {
        mSubInMemSegment->EndDump();
    }

    DirectoryPtr directory = GetDirectory();
    // flush before DumpSegmentDeployIndex to make package data & meta info DumpSegmentDeployIndex
    directory->FlushPackage();

    DirectoryPtr unpackSegmentDir = mSegmentData.GetDirectory();
    if (mCounterMap) {
        string counterStr = mCounterMap->ToJsonString();
        file_system::WriterOption writerOption;
        writerOption.notInPackage = true;
        unpackSegmentDir->Store(COUNTER_FILE_NAME, counterStr, writerOption);
    }

    SegmentInfoPtr segmentInfo = mSegmentInfo;
    if (!segmentInfo) {
        // TODO: get last segment info
        segmentInfo.reset(new SegmentInfo(*mSegmentData.GetSegmentInfo()));
    }
    SetTemperatureMeta(segmentInfo.get());

    // first store deploy index and segment info file name will be written into it
    if (!mIsSubSegment) {
        DeployIndexWrapper::DumpSegmentDeployIndex(unpackSegmentDir, segmentInfo);
    }

    // segment info need to be flush out at last, because it represent a complete segment data
    segmentInfo->Store(unpackSegmentDir);
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "EndDump %s %d, segment info : %s",
                                      mIsSubSegment ? "sub segment" : "segment", GetSegmentId(),
                                      segmentInfo->ToString(true).c_str());
    UpdateMemUse();
}

void InMemorySegment::SetTemperatureMeta(SegmentInfo* segmentInfo)
{
    std::shared_ptr<indexlib::framework::SegmentGroupMetrics> customizeMetrics;
    if (mSegmentWriter && mSegmentWriter->HasCustomizeMetrics(customizeMetrics)) {
        segmentInfo->AddDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, customizeMetrics->ToJsonString());
    }

    if (mSegmentWriter && mSegmentWriter->GetFileCompressSchema() != nullptr) {
        uint64_t fingerPrint = mSegmentWriter->GetFileCompressSchema()->GetFingerPrint();
        segmentInfo->AddDescription(SEGMENT_COMPRESS_FINGER_PRINT, autil::StringUtil::toString(fingerPrint));
    }
}

timestamp_t InMemorySegment::GetTimestamp() const
{
    if (mSegmentInfo) {
        return mSegmentInfo->timestamp;
    }
    return INVALID_TIMESTAMP;
}

void InMemorySegment::UpdateMemUse()
{
    if (mIsSubSegment) {
        return;
    }

    int64_t currentUse = (int64_t)GetTotalMemoryUse();
    int64_t allocateMemUse = currentUse - mMemoryController->GetUsedQuota();
    if (allocateMemUse > 0) {
        mMemoryController->Allocate(allocateMemUse);
    }
}

void InMemorySegment::UpdateSegmentInfo(const DocumentPtr& doc)
{
    // TODO: update segment data?
    mSegmentInfo->Update(doc);

    if (mSubInMemSegment) {
        mSubInMemSegment->UpdateSegmentInfo(doc);
    }
}

void InMemorySegment::SetSubInMemorySegment(InMemorySegmentPtr inMemorySegment)
{
    assert(inMemorySegment->GetSegmentId() == GetSegmentId());
    mSubInMemSegment = inMemorySegment;
}

InMemorySegmentPtr InMemorySegment::Snapshot()
{
    InMemorySegmentPtr snapshotSegment(new InMemorySegment(*this));
    if (mSubInMemSegment) {
        snapshotSegment->mSubInMemSegment = mSubInMemSegment->Snapshot();
    }
    return snapshotSegment;
}

void InMemorySegment::EndSegment()
{
    if (mSegmentWriter) {
        mSegmentWriter->EndSegment();
    }
    if (mSubInMemSegment) {
        mSubInMemSegment->EndSegment();
    }
}

void InMemorySegment::CreateDumpItems(vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    DirectoryPtr dumpDirectory = GetDirectory();
    if (mOperationWriter) {
        mOperationWriter->CreateDumpItems(dumpDirectory, dumpItems);
    }
    if (mSegmentWriter) {
        mSegmentWriter->CreateDumpItems(dumpDirectory, dumpItems);
    }
}

size_t InMemorySegment::GetTotalMemoryUse() const
{
    size_t memUse = 0;
    if (mSegmentWriter) {
        memUse += mSegmentWriter->GetTotalMemoryUse();
    }
    if (mOperationWriter) {
        memUse += mOperationWriter->GetTotalMemoryUse();
    }
    return memUse;
}

void InMemorySegment::SetBaseDocId(exdocid_t docid) { mSegmentData.SetBaseDocId(docid); }

segmentid_t InMemorySegment::GetSegmentId() const { return mSegmentData.GetSegmentId(); }

exdocid_t InMemorySegment::GetBaseDocId() const { return mSegmentData.GetBaseDocId(); }

MemoryReserverPtr InMemorySegment::CreateMemoryReserver(const string& name)
{
    return MemoryReserverPtr(new MemoryReserver(name, mMemoryController->GetBlockMemoryController()));
}

int64_t InMemorySegment::GetUsedQuota() const
{
    if (mMemoryController) {
        return mMemoryController->GetUsedQuota();
    }
    return 0;
}
}} // namespace indexlib::index_base
