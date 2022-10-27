#ifndef __INDEXLIB_SEGMENT_DIRECTORY_BASE_H
#define __INDEXLIB_SEGMENT_DIRECTORY_BASE_H

#include <map>
#include <string>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/version.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(index);

// for merger interface
class SegmentDirectoryBase
{
public:
    SegmentDirectoryBase() {}
    SegmentDirectoryBase(const index_base::Version& version)
        : mVersion(version)
    {}
    virtual ~SegmentDirectoryBase() {}

public:
    const index_base::Version& GetVersion() const { return mVersion; }
    
    const index_base::PartitionDataPtr& GetPartitionData() { return mPartitionData; }
    
    void SetPartitionData(const index_base::PartitionDataPtr& partitionData)
    { mPartitionData = partitionData; }

    virtual void ListAttrPatch(const std::string& attrName, segmentid_t segId, 
                               std::vector<std::pair<std::string, segmentid_t> >& patches) const = 0;
 
    virtual void ListAttrPatch(const std::string& attrName, segmentid_t segId, 
                               std::vector<std::string>& patches) const = 0;
protected:
    index_base::Version mVersion;
    index_base::PartitionDataPtr mPartitionData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryBase);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_DIRECTORY_BASE_H
