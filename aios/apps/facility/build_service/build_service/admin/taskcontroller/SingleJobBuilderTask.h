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
#pragma once

#include <stdint.h>
#include <string>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/FatalErrorDiscoverer.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace admin {

class SingleJobBuilderTask : public AdminTaskBase
{
public:
    SingleJobBuilderTask(const proto::BuildId& buildId, const std::string& cluserName,
                         const TaskResourceManagerPtr& resMgr);
    ~SingleJobBuilderTask();

private:
    SingleJobBuilderTask& operator=(const SingleJobBuilderTask&);

public:
    bool init(const proto::DataDescriptions& dataDescriptions);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool run(proto::WorkerNodes& workerNodes) override;
    bool start(const KeyValueMap& kvMap) override;
    bool finish(const KeyValueMap& kvMap) override;
    void waitSuspended(proto::WorkerNodes& workerNodes) override;
    bool updateConfig() override;
    bool loadFromConfig(const config::ResourceReaderPtr& resourceReader) override;
    void doSupplementLableInfo(KeyValueMap& info) const override;
    std::string getTaskPhaseIdentifier() const override;

    void clearFullWorkerZkNode(const std::string& generationDir) const;

    void fillJobInfo(proto::JobInfo* jobInfo, const proto::JobNodes& nodes,
                     const CounterCollector::RoleCounterMap& roleCounterMap) const;

private:
    bool run(proto::JobNodes& activeNodes);
    bool markFinishedWorkers(const proto::JobNodes& nodes);
    bool nodeFinished(const proto::JobNodePtr& node) const;
    void generateAndSetTarget(const proto::JobNodes& nodes);
    void initJobNodes(proto::JobNodes& activeNodes);
    bool getSwiftStopTimestamp(const proto::DataDescriptions& dataDescriptions, int64_t& ts);
    void endBuild();

private:
    std::string _clusterName;
    uint32_t _currentDataDescriptionId;
    proto::DataDescriptions _dataDescriptions;
    uint32_t _partitionCount;
    uint32_t _buildParallelNum;
    int64_t _schemaVersion;
    int64_t _swiftStopTs;
    uint64_t _builderStartTimestamp;
    uint64_t _builderStopTimestamp;

    FatalErrorDiscovererPtr _fatalErrorDiscoverer;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleJobBuilderTask);

}} // namespace build_service::admin
