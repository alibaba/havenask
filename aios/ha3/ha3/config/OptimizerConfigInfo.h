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
#include <vector>

#include "autil/legacy/jsonizable.h"

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace config {

struct OptimizerConfigInfo : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        KeyValueMap defaultParams;
        json.Jsonize("optimizer_name", optimizerName, optimizerName);
        json.Jsonize("module_name", moduleName, moduleName);
        json.Jsonize("parameters", parameters, defaultParams);
    }
    std::string optimizerName;
    std::string moduleName;
    KeyValueMap parameters;
};

typedef std::vector<OptimizerConfigInfo> OptimizerConfigInfos;

} // namespace config
} // namespace isearch
