#ifndef __INDEXLIB_PARTITION_INFO_CREATOR_H
#define __INDEXLIB_PARTITION_INFO_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/test/single_field_partition_data_provider.h"

IE_NAMESPACE_BEGIN(index);

class PartitionInfoCreator
{
public:
    PartitionInfoCreator(const std::string& rootDir)
        : mRootDir(rootDir)
    {
    }
    ~PartitionInfoCreator() {}

public:
    // (versionId, "incSegId[:docCount][,incSegId[:docCount]]*",
    // "rtSegId[:docCount][,rtSegId[:docCount]*")
    // if not specify "docCount", use default value 1
    PartitionInfo CreatePartitionInfo(versionid_t versionId, 
                                      const std::string& incSegmentIds,
                                      const std::string& rtSegmentIds = "",
                                      index_base::PartitionMetaPtr partMeta = index_base::PartitionMetaPtr(),
                                      std::vector<index_base::InMemorySegmentPtr> inMemSegs = std::vector<index_base::InMemorySegmentPtr>());
    PartitionInfoPtr CreatePartitionInfo(const std::string& incDocCounts,
            const std::string& rtDocCounts = "");

    std::string ExtractDocString(const std::string& docCounts);

    void ExtractDocCount(const std::string& segmentInfos, 
                         std::vector<docid_t> &docCount);

private:
    std::string mRootDir;
    test::SingleFieldPartitionDataProviderPtr mProvider;    

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionInfoCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PARTITION_INFO_CREATOR_H
