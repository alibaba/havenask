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

#include <map>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class ProcessorOutput : public autil::legacy::Jsonizable
{
public:
    ProcessorOutput();
    ~ProcessorOutput();

public:
    void addOutput(const std::string& clusterName, const config::TaskOutputConfig& output)
    {
        _cluster2Config[clusterName] = output;
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void combine(const ProcessorOutput& other);
    bool isEmpty() const { return _cluster2Config.size() == 0; }
    std::map<std::string, config::TaskOutputConfig> getOutput() { return _cluster2Config; }
    bool getOutputId(const std::string& clusterName, std::string* outputId) const;

private:
    std::map<std::string, config::TaskOutputConfig> _cluster2Config;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorOutput);

}} // namespace build_service::common
