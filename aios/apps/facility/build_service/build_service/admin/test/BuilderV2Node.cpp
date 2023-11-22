#include "build_service/admin/test/BuilderV2Node.h"

#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/ProtoComparator.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, BuilderV2Node);

BuilderV2Node::BuilderV2Node(std::shared_ptr<proto::TaskNode> taskNode) : _taskNode(taskNode) {}

BuilderV2Node::~BuilderV2Node() {}

proto::BuildTaskTargetInfo BuilderV2Node::getBuildTaskTargetInfo()
{
    config::TaskTarget target;
    FromJsonString(target, _taskNode->getTargetStatus().targetdescription());
    std::string taskInfoStr;
    target.getTargetDescription(config::BS_BUILD_TASK_TARGET, taskInfoStr);
    proto::BuildTaskTargetInfo targetInfo;
    FromJsonString(targetInfo, taskInfoStr);
    return targetInfo;
}

void BuilderV2Node::setBuildTaskTargetInfo(const proto::BuildTaskTargetInfo& targetInfo)
{
    config::TaskTarget target;
    target.addTargetDescription(config::BS_BUILD_TASK_TARGET, ToJsonString(targetInfo));
    _taskNode->getTargetStatus().set_targetdescription(ToJsonString(target));
}

proto::BuildTaskCurrentInfo BuilderV2Node::getBuildTaskCurrentInfo()
{
    auto statusStr = _taskNode->getCurrentStatus().statusdescription();
    proto::BuildTaskCurrentInfo currentInfo;
    FromJsonString(currentInfo, statusStr);
    return currentInfo;
}

void BuilderV2Node::setBuildTaskCurrentInfo(const proto::BuildTaskCurrentInfo& currentInfo)
{
    auto status = _taskNode->getCurrentStatus();
    status.set_statusdescription(autil::legacy::ToJsonString(currentInfo));
    std::string statusStr;
    status.SerializeToString(&statusStr);
    _taskNode->setCurrentStatusStr(statusStr, "");
}

proto::BuildMode BuilderV2Node::getBuildMode()
{
    auto targetInfo = getBuildTaskTargetInfo();
    return targetInfo.buildMode;
}

bool BuilderV2Node::isBuildFinished()
{
    auto targetInfo = getBuildTaskTargetInfo();
    if (targetInfo.buildMode & proto::PUBLISH) {
        return targetInfo.finished;
    }
    if (_taskNode->getCurrentStatus().reachedtarget() == _taskNode->getTargetStatus()) {
        return true;
    }
    return false;
}

std::string BuilderV2Node::getConfigPath() { return _taskNode->getTargetConfigPath(); }

proto::DataDescription BuilderV2Node::getDataDesc()
{
    config::TaskTarget target;
    FromJsonString(target, _taskNode->getTargetStatus().targetdescription());
    std::string dataDescStr;
    target.getTargetDescription(config::DATA_DESCRIPTION_KEY, dataDescStr);
    proto::DataDescription dataDesc;
    FromJsonString(dataDesc, dataDescStr);
    return dataDesc;
}

void BuilderV2Node::setBuildFinish(indexlib::schemaid_t readSchemaId)
{
    auto targetInfo = getBuildTaskTargetInfo();
    if (targetInfo.buildMode & proto::BUILD) {
        auto currentStatus = _taskNode->getCurrentStatus();
        *(currentStatus.mutable_reachedtarget()) = _taskNode->getTargetStatus();
        proto::BuildTaskCurrentInfo currentInfo;
        currentStatus.set_statusdescription(autil::legacy::ToJsonString(currentInfo));
        std::string statusStr;
        currentStatus.SerializeToString(&statusStr);
        _taskNode->setCurrentStatusStr(statusStr, "");
    }

    if (targetInfo.buildMode & proto::PUBLISH) {
        auto alignVersion = targetInfo.alignVersionId;
        if (alignVersion == indexlibv2::INVALID_VERSIONID) {
            return;
        }
        auto currentStatus = _taskNode->getCurrentStatus();
        *(currentStatus.mutable_reachedtarget()) = _taskNode->getTargetStatus();
        indexlibv2::framework::VersionMeta meta(alignVersion);
        meta.TEST_SetReadSchema(readSchemaId);
        proto::BuildTaskCurrentInfo currentInfo;
        currentInfo.commitedVersion.versionMeta = meta;
        currentInfo.lastAlignedVersion = currentInfo.commitedVersion;
        currentStatus.set_statusdescription(autil::legacy::ToJsonString(currentInfo));
        std::string statusStr;
        currentStatus.SerializeToString(&statusStr);
        _taskNode->setCurrentStatusStr(statusStr, "");
    }
}

}} // namespace build_service::admin
