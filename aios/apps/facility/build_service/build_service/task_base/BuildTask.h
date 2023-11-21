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

#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/FlowFactory.h"

namespace build_service { namespace task_base {

class BuildTask : public TaskBase
{
public:
    BuildTask(const std::string& epochId);
    virtual ~BuildTask();

private:
    BuildTask(const BuildTask&);
    BuildTask& operator=(const BuildTask&);

public:
    bool run(const std::string& jobParam, workflow::FlowFactory* brokerFactory, int32_t instanceId, Mode mode,
             bool isTablet = false);
    static bool buildRawFileIndex(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                                  const proto::PartitionId& partitionId);

private:
    bool startBuildFlow(const proto::PartitionId& partitionId, workflow::FlowFactory* brokerFactory, Mode mode,
                        bool isTablet);
    static bool atomicCopy(const std::string& src, const std::string& file);
    static workflow::BuildFlowMode getBuildFlowMode(bool needPartition, Mode mode);

private:
    virtual workflow::BuildFlow* createBuildFlow(bool isTablet) const;
    bool resetResourceReader(const proto::PartitionId& partitionId);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildTask);

}} // namespace build_service::task_base
