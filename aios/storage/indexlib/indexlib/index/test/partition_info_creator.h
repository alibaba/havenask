#ifndef __INDEXLIB_PARTITION_INFO_CREATOR_H
#define __INDEXLIB_PARTITION_INFO_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/single_field_partition_data_provider.h"

namespace indexlib { namespace index {

class PartitionInfoCreator
{
private:
    PartitionInfoCreator() {}
    ~PartitionInfoCreator() {}

public:
    // (versionId, "incSegId[:docCount][,incSegId[:docCount]]*",
    // "rtSegId[:docCount][,rtSegId[:docCount]*")
    // if not specify "docCount", use default value 1
    static PartitionInfo CreatePartitionInfo(
        versionid_t versionId, const std::string& incSegmentIds, const std::string& rtSegmentIds = "",
        index_base::PartitionMetaPtr partMeta = index_base::PartitionMetaPtr(),
        std::vector<index_base::InMemorySegmentPtr> inMemSegs = std::vector<index_base::InMemorySegmentPtr>(),
        bool needDelMapReader = true, const std::string& temperatures = "");
    static PartitionInfoPtr CreatePartitionInfo(const std::string& incDocCounts, const std::string& rtDocCounts,
                                                const std::string& rootDir);

    static std::string ExtractDocString(const std::string& docCounts);

    static void ExtractDocCount(const std::string& segmentInfos, std::vector<docid_t>& docCount);

private:
    static void AddTemperatureInfos(const std::string& temperatures, index_base::Version* version);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionInfoCreator);
}} // namespace indexlib::index

#endif //__INDEXLIB_PARTITION_INFO_CREATOR_H
