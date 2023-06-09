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
#include "build_service/common/ProcessorOutput.h"

using namespace std;

namespace build_service { namespace common {
BS_LOG_SETUP(admin, ProcessorOutput);

ProcessorOutput::ProcessorOutput() {}

ProcessorOutput::~ProcessorOutput() {}

void ProcessorOutput::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("outputInfos", _cluster2Config, _cluster2Config);
}

void ProcessorOutput::combine(const ProcessorOutput& other)
{
    _cluster2Config.insert(other._cluster2Config.begin(), other._cluster2Config.end());
}

bool ProcessorOutput::getOutputId(const std::string& clusterName, std::string* outputId) const
{
    auto iter = _cluster2Config.find(clusterName);
    if (iter == _cluster2Config.end()) {
        return false;
    }
    *outputId = iter->second.getIdentifier();
    return true;
}

}} // namespace build_service::common
