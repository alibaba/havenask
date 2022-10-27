#include <ha3/common/SwiftPartitionUtil.h>

USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SwiftPartitionUtil);

SwiftPartitionUtil::SwiftPartitionUtil() { 
}

SwiftPartitionUtil::~SwiftPartitionUtil() { 
}

bool SwiftPartitionUtil::rangeToSwiftPartition(
        const Range &range, uint32_t &swiftPartitionId) 
{
    if (range.from() != range.to()) {
        return false;
    }
    swiftPartitionId = range.from();
    return true;
}

bool SwiftPartitionUtil::swiftPartitionToRange(
        uint32_t swiftPartitionId, Range &range)
{
    if (swiftPartitionId > std::numeric_limits<uint16_t>::max()) {
        return false;
    }
    range.set_from(swiftPartitionId);
    range.set_to(swiftPartitionId);
    return true;
}

END_HA3_NAMESPACE(common);
