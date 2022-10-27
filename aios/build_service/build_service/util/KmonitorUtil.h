#ifndef ISEARCH_BS_KMON_UTIL_H
#define ISEARCH_BS_KMON_UTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <kmonitor/client/core/MetricsTags.h>
#include <string>
#include <vector>

namespace build_service {
namespace util {

class KmonitorUtil
{
public:
    static void getTags(const proto::PartitionId &pid, kmonitor::MetricsTags &tags);
    static void getTags(const proto::Range &range, const std::string& clusterName,
                        const std::string& dataTable,
                        generationid_t generationId, kmonitor::MetricsTags &tags);
private:
    static std::string getApp();
};
}
}

#endif //ISEARCH_BS_KMON_UTIL_H
