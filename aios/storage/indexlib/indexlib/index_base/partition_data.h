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
#ifndef __INDEXLIB_PARITION_DATA_H
#define __INDEXLIB_PARITION_DATA_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index_base, TemperatureDocInfo);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, PatchFileFinder);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(index_base, IndexFormatVersion);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index_base, SegmentTemperatureMeta);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

namespace indexlib { namespace index_base {
typedef std::vector<SegmentData> SegmentDataVector;

class PartitionData
{
public:
    typedef std::vector<SegmentData> SegmentDataVector;
    typedef SegmentDataVector::const_iterator Iterator;

public:
    virtual ~PartitionData() {}

public:
    virtual index_base::Version GetVersion() const = 0;
    virtual index_base::Version GetOnDiskVersion() const = 0;
    virtual index_base::PartitionMeta GetPartitionMeta() const = 0;
    virtual const file_system::DirectoryPtr& GetRootDirectory() const = 0;
    virtual const index_base::IndexFormatVersion& GetIndexFormatVersion() const = 0;

    virtual Iterator Begin() const = 0;
    virtual Iterator End() const = 0;
    virtual segmentid_t GetLastSegmentId() const = 0;
    virtual SegmentData GetSegmentData(segmentid_t segId) const = 0;
    virtual index::DeletionMapReaderPtr GetDeletionMapReader() const = 0;
    virtual index::PartitionInfoPtr GetPartitionInfo() const = 0;

    virtual PartitionData* Clone() = 0;

    // data lock is required on snapshot
    virtual PartitionData* Snapshot(autil::ThreadMutex* dataLock)
    {
        assert(false);
        return nullptr;
    }
    virtual PartitionDataPtr GetSubPartitionData() const = 0;
    virtual const SegmentDirectoryPtr& GetSegmentDirectory() const = 0;

    virtual InMemorySegmentPtr GetInMemorySegment() const { return InMemorySegmentPtr(); };

    virtual void ResetInMemorySegment() {}

    // TODO:
    virtual void CommitVersion() { assert(false); }
    virtual void RemoveSegments(const std::vector<segmentid_t>& segIds) { assert(false); }

    virtual void UpdatePartitionInfo() { assert(false); }
    virtual InMemorySegmentPtr CreateNewSegment()
    {
        assert(false);
        return InMemorySegmentPtr();
    }
    virtual void AddBuiltSegment(segmentid_t segId, const index_base::SegmentInfoPtr& segInfo) { assert(false); }
    virtual InMemorySegmentPtr CreateJoinSegment()
    {
        assert(false);
        return InMemorySegmentPtr();
    }
    virtual void AddSegmentTemperatureMeta(const index_base::SegmentTemperatureMeta& temperatureMeta) { assert(false); }

    virtual uint32_t GetIndexShardingColumnNum(const config::IndexPartitionOptions& options) const = 0;

    virtual segmentid_t GetLastValidRtSegmentInLinkDirectory() const { return INVALID_SEGMENTID; }

    virtual bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
    {
        assert(false);
        return false;
    }
    virtual const util::CounterMapPtr& GetCounterMap() const
    {
        static util::CounterMapPtr counterMap;
        return counterMap;
    }

    virtual const plugin::PluginManagerPtr& GetPluginManager() const = 0;
    virtual void ResetCounters() {}

    virtual std::string GetLastLocator() const
    {
        assert(false);
        return std::string("");
    }

    virtual PartitionSegmentIteratorPtr CreateSegmentIterator() = 0;

    virtual const TemperatureDocInfoPtr GetTemperatureDocInfo() const = 0;
    virtual void UpdateDumppedSegment()
    {
        assert(false);
        return;
    }

    virtual document::SrcSignature GetSrcSignature() const { return document::SrcSignature(); }

private:
    IE_LOG_DECLARE();
};

// wrapper class for partition data with inner lock
class PartitionDataHolder
{
public:
    PartitionDataHolder() {}

    PartitionDataHolder(const PartitionDataPtr& partData) { Reset(partData); }

    PartitionDataHolder(const PartitionDataHolder& other) { Reset(other.Get()); }

    ~PartitionDataHolder() {}

public:
    void operator=(const PartitionDataHolder& other) { Reset(other.Get()); }

    void Reset(const PartitionDataPtr& partData = PartitionDataPtr())
    {
        autil::ScopedLock lock(mLock);
        mPartitionData = partData;
    }

    PartitionDataPtr Get() const
    {
        autil::ScopedLock lock(mLock);
        return mPartitionData;
    }

private:
    PartitionDataPtr mPartitionData;
    mutable autil::ThreadMutex mLock;
};

DEFINE_SHARED_PTR(PartitionDataHolder);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_PARITION_DATA_H
