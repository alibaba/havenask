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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class ProcessorCheckpointFormatter
{
public:
    ProcessorCheckpointFormatter() {}
    ~ProcessorCheckpointFormatter() {}

private:
    ProcessorCheckpointFormatter(const ProcessorCheckpointFormatter&);
    ProcessorCheckpointFormatter& operator=(const ProcessorCheckpointFormatter&);

public:
    static std::string getProcessorCheckpointName(const std::string& clusterName, const std::string& topicId)
    {
        std::stringstream ss;
        ss << clusterName << "_" << topicId;
        return ss.str();
    }

    static std::string getProcessorCommitedCheckpointName(const std::string& clusterName, const std::string& topicId)
    {
        std::stringstream ss;
        ss << clusterName << "_" << topicId << "_committed";
        return ss.str();
    }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorCheckpointFormatter);

}} // namespace build_service::admin
