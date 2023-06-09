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
#ifndef ISEARCH_BS_PROCESSORCHAINSELECTORCONFIG_H
#define ISEARCH_BS_PROCESSORCHAINSELECTORCONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {
class SelectRule : public autil::legacy::Jsonizable
{
public:
    SelectRule();
    ~SelectRule();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const SelectRule& other) const;
    bool operator!=(const SelectRule& other) const;

public:
    std::map<std::string, std::string> matcher;
    std::vector<std::string> destChains;
};
typedef std::vector<SelectRule> SelectRuleVec;

class ProcessorChainSelectorConfig : public autil::legacy::Jsonizable
{
public:
    ProcessorChainSelectorConfig();
    ~ProcessorChainSelectorConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::vector<std::string> selectFields;
    SelectRuleVec selectRules;
};

BS_TYPEDEF_PTR(ProcessorChainSelectorConfig);
typedef std::vector<ProcessorChainSelectorConfig> ProcessorChainSelectorConfigVec;

}} // namespace build_service::config

#endif // ISEARCH_BS_PROCESSORCHAINSELECTORCONFIG_H
