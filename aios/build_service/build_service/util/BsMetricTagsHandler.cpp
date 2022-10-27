#include "build_service/util/BsMetricTagsHandler.h"
#include "build_service/util/EnvUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace build_service::proto;

namespace build_service {
namespace util {
BS_LOG_SETUP(util, BsMetricTagsHandler);

string BsMetricTagsHandler::SERVICE_NAME_PREFIX = "bs";

BsMetricTagsHandler::BsMetricTagsHandler(const PartitionId &pid)
    : _pid(pid)
{
    _partition = StringUtil::toString(pid.range().from());
    _partition += "_" + StringUtil::toString(pid.range().to());

    setBuildStep(pid.step());
    if (pid.role() == RoleType::ROLE_MERGER) {
        // set merger buildStep & mergePhase in MergerServiceImpl
        _buildStep = "";
    }
    
    if (pid.role() == RoleType::ROLE_BUILDER || pid.role() == RoleType::ROLE_MERGER) {
        _clusterName = pid.clusternames(0);
    }
    _generationId = StringUtil::toString(pid.buildid().generationid());

    _roleName = ProtoUtil::toRoleString(pid.role());
}

BsMetricTagsHandler::~BsMetricTagsHandler() {
}

void BsMetricTagsHandler::getTags(const std::string& filePath, MetricsTags& tags) const
{
    MetricTagsHandler::getTags(filePath, tags);
    if (!_buildStep.empty()) {
        tags.AddTag("buildStep", _buildStep);        
    }
    if (!_clusterName.empty()) {
        tags.AddTag("clusterName", _clusterName);
    }
    tags.AddTag("partition", _partition);
    tags.AddTag("generation", _generationId);
    tags.AddTag("roleName", _roleName);
    if (!_mergePhase.empty()) {
        tags.AddTag("mergePhase", _mergePhase);
    }
    BS_LOG(INFO, "set metrics tag[%s]", tags.ToString().c_str());
}

void BsMetricTagsHandler::setMergePhase(proto::MergeStep mergeStep) {
    if (mergeStep == proto::MergeStep::MS_UNKNOWN) {
        _mergePhase = "unknown";
    } else if (mergeStep == proto::MergeStep::MS_BEGIN_MERGE) {
        _mergePhase = "begin";
    } else if (mergeStep == proto::MergeStep::MS_DO_MERGE) {
        _mergePhase = "do";
    } else if (mergeStep == proto::MergeStep::MS_END_MERGE) {
        _mergePhase = "end";
    }
}

void BsMetricTagsHandler::setBuildStep(proto::BuildStep buildStep) {
    if (buildStep == BuildStep::BUILD_STEP_FULL) {
        _buildStep = "full";
    } else if (buildStep == BuildStep::BUILD_STEP_INC) {
        _buildStep = "inc";
    } else {
        _buildStep = "unknown";
    }        
}
    
}
}
