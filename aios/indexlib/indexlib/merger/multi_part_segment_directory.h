#ifndef __INDEXLIB_MULTI_PART_SEGMENT_DIRECTORY_H
#define __INDEXLIB_MULTI_PART_SEGMENT_DIRECTORY_H

#include <map>
#include <string>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/segment_directory.h"

IE_NAMESPACE_BEGIN(merger);

class MultiPartSegmentDirectory : public SegmentDirectory
{
public:
    /*
     * @roots : the dirs to merge.  
     * @versions in each dirs.
     */
    MultiPartSegmentDirectory(const std::vector<std::string>& roots,
                              const std::vector<index_base::Version>& versions);
    virtual ~MultiPartSegmentDirectory();

public:
    
    std::string GetRootDir() const override 
    {
        //TODO:
        return GetFirstValidRootDir();
    }

    
    std::string GetFirstValidRootDir() const override 
    {
        return mRootDirs[0];
    }

    std::string GetLatestCounterMapContent() const override;
    
    segmentid_t GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition,
                                    segmentid_t physicalSegId) const override;

    void GetPhysicalSegmentId(segmentid_t virtualSegId, 
                              segmentid_t& physicalSegId,
                              partitionid_t& partId) const override;

    
    SegmentDirectory* Clone() const override;

public:
    const std::vector<index_base::Version>& GetVersions() const { return mVersions; }
    const std::vector<std::string>& GetRootDirs() const { return mRootDirs; }

    uint32_t GetPartitionCount() const;

    const std::vector<segmentid_t>& GetSegIdsByPartId(uint32_t partId) const;
    void Reload(const std::vector<index_base::Version>& versions);

protected:
    void InitOneSegment(partitionid_t partId,
                        segmentid_t physicalSegId,
                        segmentid_t virtualSegId,
                        const index_base::Version& version);
    void DoInit(bool hasSub, bool needDeletionMap,
                const file_system::IndexlibFileSystemPtr& fileSystem) override;

protected:
    std::vector<std::string> mRootDirs;
    std::vector<index_base::Version> mVersions;
    std::vector<std::vector<segmentid_t> > mPartSegIds; // virtual segIds

private:
    IE_LOG_DECLARE();
};


DEFINE_SHARED_PTR(MultiPartSegmentDirectory);

///////////////////////////////////////////////

inline uint32_t MultiPartSegmentDirectory::GetPartitionCount() const
{
    return mPartSegIds.size();
}

inline const std::vector<segmentid_t>& 
MultiPartSegmentDirectory::GetSegIdsByPartId(uint32_t partId) const
{
    assert(partId < mPartSegIds.size());
    return mPartSegIds[partId];
}

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MULTI_PART_SEGMENT_DIRECTORY_H
