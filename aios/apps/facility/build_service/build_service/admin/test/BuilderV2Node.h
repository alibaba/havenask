#pragma once

#include "build_service/common_define.h"
#include "build_service/proto/BuildTaskCurrentInfo.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class BuilderV2Node
{
public:
    BuilderV2Node(std::shared_ptr<proto::TaskNode> taskNode);
    ~BuilderV2Node();

public:
    proto::BuildTaskTargetInfo getBuildTaskTargetInfo();
    void setBuildTaskTargetInfo(const proto::BuildTaskTargetInfo& targetInfo);
    proto::BuildTaskCurrentInfo getBuildTaskCurrentInfo();
    void setBuildTaskCurrentInfo(const proto::BuildTaskCurrentInfo& currentInfo);
    proto::BuildMode getBuildMode();
    bool isBuildFinished();
    void setBuildFinish(indexlib::schemaid_t readSchemaId = 0);
    std::string getConfigPath();
    proto::DataDescription getDataDesc();

private:
    std::shared_ptr<proto::TaskNode> _taskNode;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderV2Node);

}} // namespace build_service::admin
