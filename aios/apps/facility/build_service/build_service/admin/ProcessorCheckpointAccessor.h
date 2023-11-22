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

#include <algorithm>
#include <deque>
#include <map>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/TimeUtility.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/Locator.h"

namespace build_service { namespace admin {

class ProcessorCheckpointAccessor
{
private:
    struct CheckpointInfo {
        CheckpointInfo(const common::Locator& inputLocator) : locator(inputLocator)
        {
            ts = autil::TimeUtility::currentTimeInMicroSeconds();
        }
        int64_t ts;
        common::Locator locator;
    };
    struct ClusterCheckpointInfo {
        ClusterCheckpointInfo(const std::string& inputStage, const std::string& inputPreStage,
                              const std::string& inputTopicId, bool inputIsLastStage)
            : stage(inputStage)
            , preStage(inputPreStage)
            , topicId(inputTopicId)
            , isLastStage(inputIsLastStage)
        {
        }
        ClusterCheckpointInfo() {}
        std::string stage;
        std::string preStage;
        std::string topicId;
        bool isLastStage;
        std::deque<CheckpointInfo> checkpointList;
    };

public:
    ProcessorCheckpointAccessor(const common::CheckpointAccessorPtr& accessor) : _checkpointAccessor(accessor) {}
    ~ProcessorCheckpointAccessor();

private:
    ProcessorCheckpointAccessor(const ProcessorCheckpointAccessor&);
    ProcessorCheckpointAccessor& operator=(const ProcessorCheckpointAccessor&);

public:
    bool registCheckpointInfo(const std::string& cluster, const std::string& topicId, const std::string& preStage,
                              const std::string& stage, bool isLastStage);
    void addCheckpoint(const std::string& cluster, const std::string& topicId, const std::string& stage,
                       const common::Locator& locator);
    int32_t createMaxProcessorTaskId();
    bool getCheckpoint(const std::string& cluster, const std::string& topicId, common::Locator* locator);
    bool getCheckpoint(const std::string& checkpointName, common::Locator* locator);
    bool getCheckpointFromIndex(const std::string& cluster, int64_t schemaId, common::Locator* locator);
    bool getCheckpointFromIndex(const std::string& cluster, common::Locator* locator);
    bool getCommitedCheckpoint(const std::string& cluster, const std::string& topicId, common::Locator* locator);

private:
    void updateCheckpoint(const std::string& cluster, const std::string& topicId, const common::Locator& locator);
    void updateCommitCheckpoint(const std::string& clusterName, const std::string& topicId,
                                const common::Locator& locator);
    void commitCheckpoint(const std::string& cluster, ClusterCheckpointInfo& clusterCheckpointInfo);
    const std::string& getStage(const std::string& cluster, const std::string& topicId);

private:
    common::CheckpointAccessorPtr _checkpointAccessor;
    std::map<std::pair<std::string, std::string>, ClusterCheckpointInfo> _checkpointInfos;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorCheckpointAccessor);

}} // namespace build_service::admin
