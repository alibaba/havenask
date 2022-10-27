#ifndef ISEARCH_RANGEUTIL_H
#define ISEARCH_RANGEUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

typedef std::pair<uint16_t, uint16_t> PartitionRange;
typedef std::vector<PartitionRange > RangeVec;

class RangeUtil
{
public:
    RangeUtil();
    ~RangeUtil();
private:
    RangeUtil(const RangeUtil &);
    RangeUtil& operator=(const RangeUtil &);
public:
    static bool getRange(uint32_t partCount, uint32_t partId, util::PartitionRange &range);

    static RangeVec splitRange(uint32_t rangeFrom, uint32_t rangeTo,
                               uint32_t partitionCount);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RangeUtil);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_RANGEUTIL_H
