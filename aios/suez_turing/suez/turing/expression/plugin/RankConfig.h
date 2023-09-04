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
#include "build_service/plugin/ModuleInfo.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

class RankInfo : public autil::legacy::Jsonizable {
public:
    RankInfo() {}
    ~RankInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        JSONIZE(json, "scorer_name", scorerName);
        JSONIZE(json, "class_name", className);
        JSONIZE(json, "module_name", moduleName);
        JSONIZE(json, "parameters", parameters);
    }
    bool operator==(const RankInfo &other) const {
        if (&other == this) {
            return true;
        }
        return scorerName == other.scorerName && className == other.className && moduleName == other.moduleName &&
               parameters == other.parameters;
    }

public:
    std::string scorerName;
    std::string className;
    std::string moduleName;
    KeyValueMap parameters;
};

typedef std::vector<RankInfo> RankInfos;

class RankConfig : public autil::legacy::Jsonizable {
public:
    RankConfig() {}
    ~RankConfig() {}

private:
    RankConfig(const RankConfig &);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        JSONIZE(json, "modules", modules);
        JSONIZE(json, "scorers", rankInfos);
    }
    bool operator==(const RankConfig &other) const {
        if (&other == this) {
            return true;
        }
        if (modules.size() != other.modules.size() || rankInfos.size() != other.rankInfos.size()) {
            return false;
        }
        return modules == other.modules && rankInfos == other.rankInfos;
    }

public:
    build_service::plugin::ModuleInfos modules;
    RankInfos rankInfos;
};

SUEZ_TYPEDEF_PTR(RankConfig);

} // namespace turing
} // namespace suez
