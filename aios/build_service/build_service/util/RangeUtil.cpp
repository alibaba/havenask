#include "build_service/util/RangeUtil.h"
#include <math.h>

using namespace std;
namespace build_service {
namespace util {

BS_LOG_SETUP(util, RangeUtil);

vector<proto::Range> RangeUtil::splitRange(
        uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount)
{
    assert(rangeTo <= 65535);

    vector<proto::Range> ranges;
    if (partitionCount == 0 || rangeFrom > rangeTo) {
        return ranges;
    }

    uint32_t rangeCount = rangeTo - rangeFrom + 1;

    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;

    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partitionCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        proto::Range range;
        range.set_from(from);
        range.set_to(to);
        ranges.push_back(range);
        from = to + 1;
    }

    return ranges;
}

vector<proto::Range> RangeUtil::splitRange(
        uint32_t rangeFrom, uint32_t rangeTo,
        uint16_t partitionCount, uint32_t parallelNum)
{
    vector<proto::Range> ranges = splitRange(rangeFrom, rangeTo, partitionCount);
    if (ranges.empty() || parallelNum == 1) {
        return ranges;
    }

    vector<proto::Range> ret;
    for (size_t i = 0; i < ranges.size(); i++) {
        vector<proto::Range> subRanges = splitRange(
                ranges[i].from(), ranges[i].to(), parallelNum);
        ret.insert(ret.end(), subRanges.begin(), subRanges.end());
    }
    return ret;
}

static bool RangeLessComp(const proto::Range& lft, const proto::Range& rht)
{
    return lft.from() < rht.from() && lft.to() < rht.to();
}

proto::Range RangeUtil::getRangeInfo(
    uint32_t rangeFrom, uint32_t rangeTo,
    uint16_t partitionCount, uint32_t parallelNum, 
    uint32_t partitionIdx, uint32_t parallelIdx) {
    vector<proto::Range> ranges = splitRange(rangeFrom, rangeTo, partitionCount);
    proto::Range partRange = ranges[partitionIdx];
    vector<proto::Range> instanceRanges = splitRange(partRange.from(), partRange.to(), parallelNum);
    return instanceRanges[parallelIdx];
}

bool RangeUtil::getParallelInfoByRange(
        uint32_t rangeFrom, uint32_t rangeTo,
        uint16_t partitionCount, uint32_t parallelNum, 
        const proto::Range& range, uint32_t &partitionIdx, uint32_t &parallelIdx)
{
    if (range.from() >= range.to())
    {
        return false;
    }
    
    vector<proto::Range> ranges = splitRange(rangeFrom, rangeTo, partitionCount);
    for (uint32_t i = 0; i < ranges.size(); i++)
    {
        const proto::Range& partRange = ranges[i];
        if (range.from() < partRange.from() || range.to() > partRange.to())
        {
            continue;
        }
        
        vector<proto::Range> subRanges =
            splitRange(partRange.from(), partRange.to(), parallelNum);
        vector<proto::Range>::iterator iter = lower_bound(
                subRanges.begin(), subRanges.end(), range, RangeLessComp);
        if (iter != subRanges.end() && iter->from() == range.from() && iter->to() == range.to())
        {
            partitionIdx = i;
            parallelIdx = distance(subRanges.begin(), iter);
            return true;
        }
    }
    return false;
}


}
}
