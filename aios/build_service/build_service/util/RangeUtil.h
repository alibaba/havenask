#ifndef ISEARCH_BS_RANGEUTIL_H
#define ISEARCH_BS_RANGEUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service {
namespace util {

class RangeUtil
{
private:
    RangeUtil();
    ~RangeUtil();
    RangeUtil(const RangeUtil &);
    RangeUtil& operator=(const RangeUtil &);
public:
    static std::vector<proto::Range> splitRange(
            uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount);
    static std::vector<proto::Range> splitRange(
            uint32_t rangeFrom, uint32_t rangeTo,
            uint16_t partitionCount, uint32_t parallelNum);

    static bool getParallelInfoByRange(
            uint32_t rangeFrom, uint32_t rangeTo,
            uint16_t partitionCount, uint32_t parallelNum, 
            const proto::Range& range, uint32_t &partitionIdx,
            uint32_t &parallelIdx);

    static proto::Range getRangeInfo(
        uint32_t rangeFrom, uint32_t rangeTo,
        uint16_t partitionCount, uint32_t parallelNum, 
        uint32_t partitionIdx, uint32_t parallelIdx);
            
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RangeUtil);

}
}

#endif //ISEARCH_BS_RANGEUTIL_H
