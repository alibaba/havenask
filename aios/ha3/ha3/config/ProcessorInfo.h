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

#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace config {

class ProcessorInfo : public autil::legacy::Jsonizable
{
public:
    ProcessorInfo();
    ProcessorInfo(std::string processorName, std::string moduleName);
    ~ProcessorInfo();
public:
    std::string getProcessorName() const;
    void setProcessorName(const std::string &processorName);

    std::string getModuleName() const;
    void setModuleName(const std::string &moduleName);

    std::string getParam(const std::string &key) const;
    void addParam(const std::string &key, const std::string &value);

    const KeyValueMap &getParams() const;
    void setParams(const KeyValueMap &params);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    bool operator==(const ProcessorInfo &other) const;

public:
    std::string _processorName;
    std::string _moduleName;
    KeyValueMap _params;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::vector<ProcessorInfo> ProcessorInfos;

} // namespace config
} // namespace isearch

