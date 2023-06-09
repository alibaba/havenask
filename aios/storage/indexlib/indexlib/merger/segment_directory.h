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
#ifndef __INDEXLIB_MERGER_SEGMENT_DIRECTORY_H
#define __INDEXLIB_MERGER_SEGMENT_DIRECTORY_H

#include <map>
#include <memory>
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);

namespace indexlib { namespace merger {

class SegmentDirectory : public index::SegmentDirectoryBase
{
public:
    typedef std::vector<std::string> PathVec;
    typedef std::map<segmentid_t, index_base::SegmentInfo> SegmentInfoMap;
    typedef std::vector<exdocid_t> BaseDocIdVector;
    typedef int32_t partitionid_t;

    struct Segment {
        Segment(const file_system::DirectoryPtr& segDir, partitionid_t partitionId, segmentid_t segId)
            : segDirectory(segDir)
            , partId(partitionId)
            , physicalSegId(segId)
        {
        }

        Segment(const Segment& other)
            : segDirectory(other.segDirectory)
            , partId(other.partId)
            , physicalSegId(other.physicalSegId)
        {
        }

        Segment& operator=(const Segment& other)
        {
            if (this != &other) {
                segDirectory = other.segDirectory;
                partId = other.partId;
                physicalSegId = other.physicalSegId;
            }
            return *this;
        }

        file_system::DirectoryPtr segDirectory;
        partitionid_t partId;
        segmentid_t physicalSegId;
    };
    typedef std::map<segmentid_t, Segment> Segments;

public:
    class Iterator
    {
    public:
        Iterator(const Segments& segmentPaths) : mSegmentPaths(segmentPaths), mIter(segmentPaths.begin()) {}

        Iterator(const Iterator& iter) : mSegmentPaths(iter.mSegmentPaths), mIter(iter.mSegmentPaths.begin()) {}

        bool HasNext() const { return (mIter != mSegmentPaths.end()); }

        file_system::DirectoryPtr Next()
        {
            file_system::DirectoryPtr segDirectory = (mIter->second).segDirectory;
            mIter++;
            return segDirectory;
        }

    private:
        const Segments& mSegmentPaths;
        Segments::const_iterator mIter;
    };

public:
    SegmentDirectory();
    SegmentDirectory(const file_system::DirectoryPtr& rootDir, const index_base::Version& version);
    virtual ~SegmentDirectory();

public:
    void Init(bool hasSub, bool needDeletionMap = false);

    virtual void AddSegmentTemperatureMeta(const index_base::SegmentTemperatureMeta& temperatureMeta);
    virtual std::string GetLatestCounterMapContent() const;

    virtual file_system::DirectoryPtr GetRootDir() const;

    virtual file_system::DirectoryPtr GetFirstValidRootDir() const;

    virtual file_system::DirectoryPtr GetSegmentDirectory(segmentid_t segId) const;

    virtual SegmentDirectory* Clone() const;

    virtual segmentid_t GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition, segmentid_t physicalSegId) const;

    virtual void GetPhysicalSegmentId(segmentid_t virtualSegId, segmentid_t& physicalSegId, partitionid_t& partId) const
    {
        physicalSegId = virtualSegId;
        partId = 0;
    }

    Iterator CreateIterator() const;

    // virtual bool IsExist(const std::string& filePath) const;

    file_system::DirectoryPtr CreateNewSegment(index_base::SegmentInfo& segInfo);

    void RemoveSegment(segmentid_t segmentId);

    void CommitVersion();

    /* here we can get some segment info */
    /* TODO: later we'll call Init()     */
    void GetBaseDocIds(std::vector<exdocid_t>& baseDocIds);
    bool GetSegmentInfo(segmentid_t segId, index_base::SegmentInfo& segInfo);
    uint32_t GetDocCount(segmentid_t segId);
    uint64_t GetTotalDocCount();

    bool LoadSegmentInfo(const file_system::DirectoryPtr& segDirectory, index_base::SegmentInfo& segInfo);

    const Segments GetSegments() const { return mSegments; }
    void SetSegments(const Segments& segments) { mSegments = segments; }

    const SegmentDirectoryPtr& GetSubSegmentDirectory() const { return mSubSegmentDirectory; }

    index::DeletionMapReaderPtr GetDeletionMapReader() const;

    static void RemoveUselessSegment(const file_system::DirectoryPtr& rootDir);

    std::vector<segmentid_t> GetNewSegmentIds() const;
    void Reset();

protected:
    virtual void InitSegmentInfo();
    virtual void DoInit(bool hasSub, bool needDeletionMap);
    void Reload();

    SegmentDirectoryPtr CreateSubSegDir(bool needDeletionMap) const;
    std::string GetCounterMapContent(const Segment& segment) const;

private:
    void InitPartitionData(bool hasSub, bool needDeletionMap);
    void InitSegIdToSegmentMap();

private:
    file_system::DirectoryPtr mRootDir;

protected:
    index_base::Version mNewVersion;
    Segments mSegments;
    SegmentInfoMap mSegmentInfos;
    BaseDocIdVector mBaseDocIds;
    uint64_t mTotalDocCount;
    volatile bool mSegInfoInited;
    index::DeletionMapReaderPtr mDeletionMapReader;
    SegmentDirectoryPtr mSubSegmentDirectory;

private:
    friend class SegmentDirectoryTest;
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////

inline SegmentDirectory::Iterator SegmentDirectory::CreateIterator() const { return Iterator(mSegments); }
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGER_SEGMENT_DIRECTORY_H
