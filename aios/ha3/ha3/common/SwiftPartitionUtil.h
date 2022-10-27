#ifndef ISEARCH_SWIFTPARTITIONUTIL_H
#define ISEARCH_SWIFTPARTITIONUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/proto/BasicDefs.pb.h>

BEGIN_HA3_NAMESPACE(common);

class SwiftPartitionUtil
{
public:
    SwiftPartitionUtil();
    ~SwiftPartitionUtil();
private:
    SwiftPartitionUtil(const SwiftPartitionUtil &);
    SwiftPartitionUtil& operator=(const SwiftPartitionUtil &);
public:
    static bool rangeToSwiftPartition(const proto::Range &range, uint32_t &swiftPartitionId);
    static bool swiftPartitionToRange(uint32_t swiftPartitionId, proto::Range &range);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SwiftPartitionUtil);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_SWIFTPARTITIONUTIL_H
