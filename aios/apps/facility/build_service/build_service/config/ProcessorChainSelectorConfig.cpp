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
#include "build_service/config/ProcessorChainSelectorConfig.h"

#include <iosfwd>

using namespace std;

namespace build_service { namespace config {

SelectRule::SelectRule() {}

SelectRule::~SelectRule() {}

bool SelectRule::operator==(const SelectRule& other) const
{
    return matcher == other.matcher && destChains == other.destChains;
}

bool SelectRule::operator!=(const SelectRule& other) const { return !(SelectRule::operator==(other)); }

void SelectRule::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("matcher", matcher, matcher);
    json.Jsonize("dest_chains", destChains, destChains);
}

ProcessorChainSelectorConfig::ProcessorChainSelectorConfig() {}

ProcessorChainSelectorConfig::~ProcessorChainSelectorConfig() {}

void ProcessorChainSelectorConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("select_fields", selectFields, selectFields);
    json.Jsonize("select_rules", selectRules, selectRules);
}

}} // namespace build_service::config
