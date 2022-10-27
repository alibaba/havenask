#include <ha3/common/ResultFormatter.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

double ResultFormatter::getCoveredPercent(
        const Result::ClusterPartitionRanges &ranges)
{
    if (ranges.empty()) {
        return 0.0;
    }
    uint32_t rangeSum = 0;
    for (Result::ClusterPartitionRanges::const_iterator it = ranges.begin();
         it != ranges.end(); ++it)
    {
        const Result::PartitionRanges &oneClusterRanges = it->second;
        for (Result::PartitionRanges::const_iterator it2 = oneClusterRanges.begin();
             it2 != oneClusterRanges.end(); ++it2)
        {
            rangeSum += (it2->second - it2->first + 1);
        }
    }

    return 100.0 * rangeSum / MAX_PARTITION_COUNT / ranges.size();
}

END_HA3_NAMESPACE(common);
