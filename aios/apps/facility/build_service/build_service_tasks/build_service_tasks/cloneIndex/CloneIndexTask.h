/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_BS_CLONEINDEXTASK_H
#define ISEARCH_BS_CLONEINDEXTASK_H

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/channel/BsAdminChannel.h"
#include "build_service_tasks/channel/MadroxChannel.h"

namespace build_service { namespace task_base {

DEFINE_SHARED_PTR(RPCChannel);

class CloneIndexTask : public Task
{
public:
    CloneIndexTask();
    ~CloneIndexTask() = default;

    CloneIndexTask(const CloneIndexTask&) = delete;
    CloneIndexTask& operator=(const CloneIndexTask&) = delete;

public:
    static const std::string TASK_NAME;
    static constexpr uint32_t RPC_RETRY_INTERVAL_S = 5; // 5s

public:
    bool init(Task::TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }
    void handleFatalError() override;

private:
    struct ClusterDescription : public autil::legacy::Jsonizable {
        ClusterDescription() : isDone(false), indexLocator(-1), sourceVersionId(-1), partitionCount(0) {}

        ~ClusterDescription() = default;

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("index_locator", indexLocator, indexLocator);
            json.Jsonize("source_version_id", sourceVersionId, sourceVersionId);
            json.Jsonize("partition_count", partitionCount, partitionCount);
            json.Jsonize("source_index_path", sourceIndexPath, sourceIndexPath);
            json.Jsonize("target_index_path", targetIndexPath, targetIndexPath);
        }
        bool isDone;
        int64_t indexLocator;
        versionid_t sourceVersionId;
        uint32_t partitionCount;
        std::string sourceIndexPath;
        std::string targetIndexPath;
    };
    using ClusterMap = std::map<std::string, ClusterDescription>;
    static const std::string CLONE_INDEX_DESCRIPTION_FILE;

private:
    static bool writeCheckpointFiles(const ClusterMap& targetClusterMap);
    bool tryRecoverFromCheckpointFiles(const std::vector<std::pair<std::string, versionid_t>>& clusterInfos,
                                       ClusterMap& targetClusterMap, bool& recovered);

    bool initTargetInfo(const std::vector<std::pair<std::string, versionid_t>>& targetClusterInfos);
    RPCChannelPtr getRpcChannelWithRetry(const std::string& adminSpec);
    bool cloneIndexRpcBlock(const std::string& clusterName);
    bool getMadroxStatus(const std::string& clusterName, const std::string& targetGenerationPath);
    bool getMadroxStatusWithRetry(const std::string& targetGenerationPath);
    using ZkWrapperPtr = std::unique_ptr<cm_basic::ZkWrapper>;
    bool getParamFromTaskTarget(const config::TaskTarget& target,
                                std::vector<std::pair<std::string, versionid_t>>& targetClusterInfos);

    bool getLastVersionId(const proto::GenerationInfo& generationInfo, const std::string& clusterName,
                          uint32_t& versionId);

    bool getSpecifiedVersionLocator(const proto::GenerationInfo& generationInfo, const std::string& clusterName,
                                    uint32_t versionId, int64_t& indexLocator);

    bool checkAndGetClusterInfos(const proto::GenerationInfo& generationInfo);
    bool getServiceInfoRpcBlock(proto::GenerationInfo& generationInfo);
    bool createSnapshot(const std::string& clusterName, uint32_t versionId);
    bool removeSnapshots(const std::string& clusterName);
    static std::string getGenerationPath(const std::string& indexRoot, const std::string& clusterName,
                                         uint32_t generationId);

private:
    template <typename T>
    bool parseFromJsonString(const std::string& jsonStr, std::vector<T>& list)
    {
        list.clear();
        if (jsonStr.empty()) {
            return false;
        }
        try {
            autil::legacy::FromJsonString(list, jsonStr);
        } catch (autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "FromJsonString [%s] catch exception: [%s]", jsonStr.c_str(), e.ToString().c_str());
            return false;
        } catch (...) {
            BS_LOG(ERROR, "exception happen when call FromJsonString [%s] ", jsonStr.c_str());
            return false;
        }
        return true;
    }

private:
    Task::TaskInitParam _taskInitParam;
    volatile bool _isFinished;
    bool _targetInfoInited;
    std::string _indexRoot;
    proto::BuildId _buildId;
    proto::BuildId _srcBuildId;
    config::ControlConfig _controlConfig;
    std::string _srcIndexAdminPath;
    std::string _srcIndexBuildId;
    std::string _madroxAdminPath;
    ClusterMap _targetClusterMap;
    KeyValueMap _kvMap;
    std::shared_ptr<MadroxChannel> _madroxChannel;
    std::shared_ptr<BsAdminChannel> _bsChannel;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CloneIndexTask);

}} // namespace build_service::task_base

#endif // ISEARCH_BS_CLONEINDEXTASK_H
