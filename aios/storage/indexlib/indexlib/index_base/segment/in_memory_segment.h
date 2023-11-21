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
#pragma once

#include <memory>

#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index_base, SegmentContainer);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegmentReader);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(document, Document);

namespace indexlib { namespace util {

template <typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<int64_t> UnsafeSimpleMemoryQuotaController;
DEFINE_SHARED_PTR(UnsafeSimpleMemoryQuotaController);
}} // namespace indexlib::util

namespace indexlib { namespace index_base {

class InMemorySegment;
DEFINE_SHARED_PTR(InMemorySegment);
class SegmentWriter;
DEFINE_SHARED_PTR(SegmentWriter);

class InMemorySegment
{
public:
    enum CloneType {
        CT_SHARED = 0,
        CT_PRIVATE = 1,
    };
    InMemorySegment(const config::BuildConfig& buildConfig, const util::BlockMemoryQuotaControllerPtr& memController,
                    const util::CounterMapPtr& counterMap);

    /*CT_SHARED: modify on building deletionmap is visiable to other segment,
      CT_PRIVATE: modify on building deletionmap is not visiable to other segment*/
    InMemorySegment(const InMemorySegment& other, BuildingSegmentData& segmentData, CloneType type);
    virtual ~InMemorySegment();

public:
    enum Status { BUILDING, WAITING_TO_DUMP, DUMPING, DUMPED };

public:
    void Init(const BuildingSegmentData& segmentData, const SegmentWriterPtr& segmentWriter, bool isSubSegment);

    void Init(const BuildingSegmentData& segmentData, bool isSubSegment);
    Status GetStatus() const { return mStatus; }
    void SetStatus(Status status)
    {
        mStatus = status;
        if (mSubInMemSegment) {
            mSubInMemSegment->SetStatus(status);
        }
    }

    // TODO: in sub doc, it's multi segment writer
    const SegmentWriterPtr& GetSegmentWriter() const { return mSegmentWriter; }

    // support operate sub
    bool IsDirectoryDumped() const;
    void BeginDump();
    void EndDump();
    void UpdateSegmentInfo(const document::DocumentPtr& doc);

    const SegmentData& GetSegmentData() const { return mSegmentData; }

    const InMemorySegmentReaderPtr& GetSegmentReader() const { return mSegmentReader; }

    virtual const index_base::SegmentInfoPtr& GetSegmentInfo() const { return mSegmentInfo; }

    const file_system::DirectoryPtr& GetDirectory();

    exdocid_t GetBaseDocId() const;

    virtual segmentid_t GetSegmentId() const;

    timestamp_t GetTimestamp() const;

    void SetOperationWriter(const index_base::SegmentContainerPtr& operationWriter)
    {
        mOperationWriter = operationWriter;
    }

    index_base::SegmentContainerPtr GetOperationWriter() const { return mOperationWriter; }

    void SetSubInMemorySegment(InMemorySegmentPtr inMemorySegment);

    InMemorySegmentPtr Snapshot();

    InMemorySegmentPtr GetSubInMemorySegment() const { return mSubInMemSegment; }

    virtual size_t GetTotalMemoryUse() const;

    size_t GetDumpMemoryUse() const { return mDumpMemoryUse; }
    void UpdateMemUse();

    util::MemoryReserverPtr CreateMemoryReserver(const std::string& name);

    int64_t GetUsedQuota() const;
    void EndSegment();

public:
    // for test
    void SetBaseDocId(exdocid_t docid);

private:
    // file_system::DirectoryPtr CreateDirectory();
    void SetTemperatureMeta(index_base::SegmentInfo* segmentInfo);

    void MultiThreadDump();

    void CreateDumpItems(std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);

private:
    SegmentWriterPtr mSegmentWriter;
    BuildingSegmentData mSegmentData;
    SegmentInfoPtr mSegmentInfo;
    InMemorySegmentReaderPtr mSegmentReader;
    SegmentContainerPtr mOperationWriter;
    InMemorySegmentPtr mSubInMemSegment;
    config::BuildConfig mBuildConfig;
    util::UnsafeSimpleMemoryQuotaControllerPtr mMemoryController;
    size_t mDumpMemoryUse;
    bool mIsSubSegment;
    util::CounterMapPtr mCounterMap;
    volatile Status mStatus;

private:
    friend class InMemoryPartitionDataTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index_base
