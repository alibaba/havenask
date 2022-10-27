#ifndef __INDEXLIB_MULTI_PART_SEGMENT_DIRECTORY_H
#define __INDEXLIB_MULTI_PART_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_directory.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

//only support read
class MultiPartSegmentDirectory : public SegmentDirectory
{
public:
    MultiPartSegmentDirectory();
    MultiPartSegmentDirectory(const MultiPartSegmentDirectory& other);
    ~MultiPartSegmentDirectory();
public:
    void Init(const std::vector<file_system::DirectoryPtr>& directoryVec,
              bool hasSub = false);

    void Init(const std::vector<file_system::DirectoryPtr>& directoryVec,
              const std::vector<index_base::Version>& versions,
              bool hasSub = false);

    virtual SegmentDirectory* Clone();
    virtual file_system::DirectoryPtr GetSegmentParentDirectory(
            segmentid_t segId) const;
    virtual file_system::DirectoryPtr GetSegmentFsDirectory(
        segmentid_t segId, SegmentFileMetaPtr& segmentFileMeta) const;

    virtual void SetSubSegmentDir();
    virtual const index_base::IndexFormatVersion& GetIndexFormatVersion() const;

    const std::vector<SegmentDirectoryPtr>& GetSegmentDirectories() const
    { return mSegmentDirectoryVec; }

    segmentid_t EncodeToVirtualSegmentId(
            size_t partitionIdx, segmentid_t phySegId) const;

    bool DecodeVirtualSegmentId(segmentid_t virtualSegId,
                                segmentid_t &physicalSegId,
                                size_t &partitionId) const;

private:
    SegmentDirectoryPtr GetSegmentDirectory(segmentid_t segId) const;
    
private:
    typedef std::vector<segmentid_t> SegmentIdVector;
    std::vector<SegmentDirectoryPtr> mSegmentDirectoryVec;
    // [begin, end)
    SegmentIdVector mEndSegmentIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartSegmentDirectory);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_MULTI_PART_SEGMENT_DIRECTORY_H
