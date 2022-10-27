#ifndef ISEARCH_BS_BSMETRICTAGSHANDLER_H
#define ISEARCH_BS_BSMETRICTAGSHANDLER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include <fslib/util/MetricTagsHandler.h>

namespace build_service {
namespace util {

class BsMetricTagsHandler : public fslib::util::MetricTagsHandler
{
public:
    BsMetricTagsHandler(const proto::PartitionId &pid);
    ~BsMetricTagsHandler();
private:
    BsMetricTagsHandler(const BsMetricTagsHandler &);
    BsMetricTagsHandler& operator=(const BsMetricTagsHandler &);
public:
    void getTags(const std::string& filePath, kmonitor::MetricsTags& tags) const override;
    // begin, do, end
    void setMergePhase(proto::MergeStep phase);
    // full, inc, unknown
    void setBuildStep(proto::BuildStep buildStep);
private:
    proto::PartitionId _pid;
    std::string _partition;
    std::string _buildStep;
    std::string _clusterName;
    std::string _generationId;
    std::string _roleName;
    std::string _mergePhase;
private:
    static std::string SERVICE_NAME_PREFIX;
private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<BsMetricTagsHandler> BsMetricTagsHandlerPtr;

}
}

#endif //ISEARCH_BS_BSMETRICTAGSHANDLER_H
