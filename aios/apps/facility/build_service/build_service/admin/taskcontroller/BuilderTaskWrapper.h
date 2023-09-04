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
#ifndef ISEARCH_BS_BUILDERTASKWRAPPER_H
#define ISEARCH_BS_BUILDERTASKWRAPPER_H

#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/SingleBuilderTask.h"
#include "build_service/admin/taskcontroller/TaskMaintainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"

namespace beeper {
class EventTags;
typedef std::shared_ptr<EventTags> EventTagsPtr;
} // namespace beeper

namespace build_service { namespace admin {

class BuilderTaskWrapper : public AdminTaskBase
{
public:
    BuilderTaskWrapper(const proto::BuildId& buildId, const std::string& cluserName, const std::string& taskId,
                       const TaskResourceManagerPtr& resMgr);
    ~BuilderTaskWrapper();

public:
    bool init(proto::BuildStep buildStep);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool run(proto::WorkerNodes& workerNodes) override;
    bool start(const KeyValueMap& kvMap) override;
    bool finish(const KeyValueMap& kvMap) override;
    void waitSuspended(proto::WorkerNodes& workerNodes) override;
    bool updateConfig() override;

    void SetProperty(const std::string& key, const std::string& value) override { _buildTask->SetProperty(key, value); }

    bool GetProperty(const std::string& key, std::string& value) const override
    {
        return _buildTask->GetProperty(key, value);
    }

    const std::map<std::string, std::string>& GetPropertyMap() const override { return _buildTask->GetPropertyMap(); }

    void SetPropertyMap(const std::map<std::string, std::string>& propertyMap) override
    {
        _buildTask->SetPropertyMap(propertyMap);
    }

public:
    void fillClusterInfo(proto::SingleClusterInfo* clusterInfo, const proto::BuilderNodes& nodes,
                         const CounterCollector::RoleCounterMap& roleCounterMap) const;
    proto::BuildStep getStep() const;
    void clearFullWorkerZkNode(const std::string& generationDir) const;
    bool isBatchMode() const;
    void notifyStopped() override;
    void doSupplementLableInfo(KeyValueMap& info) const override;
    bool getTaskRunningTime(int64_t& intervalInMicroSec) const override;
    std::string getTaskPhaseIdentifier() const override;
    void setBeeperTags(const beeper::EventTagsPtr beeperTags) override;

private:
    bool initEndBuildTask();
    void addCheckpoint(proto::BuilderCheckpoint& builderCheckpoint);
    bool needEndBuild() const;
    std::string getBuildPhrase() const;
    void getEndBuildTaskParam(KeyValueMap& kvMap);

private:
    BuilderTaskWrapper(const BuilderTaskWrapper&);
    BuilderTaskWrapper& operator=(const BuilderTaskWrapper&);

private:
    enum Phrase { BUILD = 0, END_BUILD, UNKNOWN };

private:
    Phrase _phrase;
    std::string _clusterName;
    proto::BuildStep _buildStep;
    SingleBuilderTaskPtr _buildTask;
    TaskMaintainerPtr _endBuildTask;
    int64_t _endBuildBeginTs;
    int32_t _batchMask;

private:
    const static int32_t MINOR_BINARY_VERSION = 2;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderTaskWrapper);

}} // namespace build_service::admin

#endif // ISEARCH_BS_BUILDERTASKWRAPPER_H
