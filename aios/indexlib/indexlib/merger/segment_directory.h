#ifndef __INDEXLIB_MERGER_SEGMENT_DIRECTORY_H
#define __INDEXLIB_MERGER_SEGMENT_DIRECTORY_H

#include <map>
#include <string>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/file_system/indexlib_file_system.h"

DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);

IE_NAMESPACE_BEGIN(merger);

class SegmentDirectory : public index::SegmentDirectoryBase
{
public:
    typedef std::vector<std::string> PathVec;
    typedef std::map<segmentid_t, index_base::SegmentInfo> SegmentInfoMap;
    typedef std::vector<exdocid_t> BaseDocIdVector;
    typedef int32_t partitionid_t;

    struct Segment
    {
        Segment()
            : partId(-1)
            , physicalSegId(INVALID_SEGMENTID)
        {
        }

        Segment(const std::string& seg, partitionid_t partitionId, 
                segmentid_t segId)
            : segPath(seg)
            , partId(partitionId)
            , physicalSegId(segId)
        {
        }

        Segment(const Segment& other)
            : segPath(other.segPath)
            , partId(other.partId)
            , physicalSegId(other.physicalSegId)
        {
        }

        std::string segPath;
        partitionid_t partId;
        segmentid_t physicalSegId;
    };
    typedef std::map<segmentid_t, Segment> Segments;

public:
    class Iterator
    {
    public:
        Iterator(const Segments& segmentPaths)
            : mSegmentPaths(segmentPaths)
            , mIter(segmentPaths.begin())
        {
        }

        Iterator(const Iterator& iter)
            : mSegmentPaths(iter.mSegmentPaths)
            , mIter(iter.mSegmentPaths.begin())
        {
        }
        
        bool HasNext() const
        {
            return (mIter != mSegmentPaths.end());
        }

        std::string Next()
        {
            std::string segPath = (mIter->second).segPath;
            mIter++;
            return segPath;
        }
    private:
        const Segments& mSegmentPaths;
        Segments::const_iterator mIter;
    };
    
public:
    SegmentDirectory();
    SegmentDirectory(const std::string& root,
                     const index_base::Version& version);
    virtual ~SegmentDirectory();

public:
    void Init(bool hasSub, bool needDeletionMap = false,
              const file_system::IndexlibFileSystemPtr& fileSystem = file_system::IndexlibFileSystemPtr());

    std::string GetSegmentInfoPath(segmentid_t segId) const;

    virtual std::string GetLatestCounterMapContent() const;

    virtual std::string GetRootDir() const;

    virtual std::string GetFirstValidRootDir() const;

    virtual std::string GetSegmentPath(segmentid_t segId) const;

    virtual SegmentDirectory* Clone() const;

    virtual segmentid_t GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition,
            segmentid_t physicalSegId) const;

    virtual void GetPhysicalSegmentId(segmentid_t virtualSegId, 
            segmentid_t& physicalSegId, partitionid_t& partId) const
    { physicalSegId = virtualSegId; partId = 0; }

    Iterator CreateIterator() const;

    //TODO: some case mock this interface
    virtual void ListAttrPatch(const std::string& attrName, segmentid_t segId, 
                                std::vector<std::pair<std::string, segmentid_t> >& patches) const;
 
    virtual void ListAttrPatch(const std::string& attrName, segmentid_t segId, 
                               std::vector<std::string>& patches) const;

    virtual bool IsExist(const std::string& filePath) const;

    file_system::DirectoryPtr CreateNewSegment(index_base::SegmentInfo& segInfo);
    
    void RemoveSegment(segmentid_t segmentId);
    
    void CommitVersion();

    /* here we can get some segment info */
    /* TODO: later we'll call Init()     */
    void GetBaseDocIds(std::vector<exdocid_t>& baseDocIds);
    bool GetSegmentInfo(segmentid_t segId, index_base::SegmentInfo& segInfo);
    uint32_t GetDocCount(segmentid_t segId);
    uint64_t GetTotalDocCount();
    
    bool LoadSegmentInfo(const std::string& segPath, index_base::SegmentInfo &segInfo);

    const Segments GetSegments() const { return mSegments; }
    void SetSegments(const Segments& segments) { mSegments = segments; }

    const SegmentDirectoryPtr& GetSubSegmentDirectory() const
    { return mSubSegmentDirectory; }

    index::DeletionMapReaderPtr GetDeletionMapReader() const;

    static void RemoveUselessSegment(const std::string& rootDir);

    std::vector<segmentid_t> GetNewSegmentIds() const;
    
protected:
    virtual void InitSegmentInfo();
    virtual void DoInit(bool hasSub, bool needDeletionMap,
                        const file_system::IndexlibFileSystemPtr& fileSystem);
    void Reload();
    void Reset();

    SegmentDirectoryPtr CreateSubSegDir(bool needDeletionMap) const;
    std::string GetCounterMapContent(const Segment& segment) const; 

private:
    void InitPartitionData(bool hasSub, bool needDeletionMap,
                           const file_system::IndexlibFileSystemPtr& fileSystem);
    void InitSegIdToSegmentMap();

private:
    std::string mRootDir;

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

inline std::string SegmentDirectory::GetSegmentInfoPath(segmentid_t segId) const
{
    std::string segPath = GetSegmentPath(segId);   
    return segPath + SEGMENT_INFO_FILE_NAME;
}

inline SegmentDirectory::Iterator SegmentDirectory::CreateIterator() const
{
    return Iterator(mSegments);
}

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGER_SEGMENT_DIRECTORY_H
