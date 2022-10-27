#include <ha3/util/RangeUtil.h>

using namespace std;
BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, RangeUtil);

RangeUtil::RangeUtil() { 
}

RangeUtil::~RangeUtil() { 
}

bool RangeUtil::getRange(uint32_t partCount, uint32_t partId, util::PartitionRange &range) {
    RangeVec vec = RangeUtil::splitRange(0, 65535, partCount);
    if (partId >= vec.size()) {
        return false;
    }
    range = vec[partId];
    return true;
}


RangeVec RangeUtil::splitRange(uint32_t rangeFrom, uint32_t rangeTo, 
                               uint32_t partitionCount)
{
    assert(rangeTo <= MAX_PARTITION_RANGE);
    
    RangeVec ranges;
    if (partitionCount == 0 || rangeFrom > rangeTo) {
        return ranges;
    }
    
    uint32_t rangeCount = rangeTo - rangeFrom + 1;
    
    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;
    
    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partitionCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        ranges.push_back(make_pair(from, to));
        from = to + 1;
    }

    return ranges;
}

END_HA3_NAMESPACE(util);

