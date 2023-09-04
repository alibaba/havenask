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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

class SorterInfo : public autil::legacy::Jsonizable {
public:
    SorterInfo() {}
    ~SorterInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        JSONIZE(json, "sorter_name", sorterName);
        JSONIZE(json, "class_name", className);
        JSONIZE(json, "module_name", moduleName);
        JSONIZE(json, "parameters", parameters);
    }
    bool operator==(const SorterInfo &other) const {
        if (&other == this) {
            return true;
        }
        return sorterName == other.sorterName && className == other.className && moduleName == other.moduleName &&
               parameters == other.parameters;
    }

public:
    std::string sorterName;
    std::string className;
    std::string moduleName;
    KeyValueMap parameters;
};

typedef std::vector<SorterInfo> SorterInfos;

class SorterConfig : public autil::legacy::Jsonizable {
public:
    SorterConfig() {}
    ~SorterConfig() {}

private:
    SorterConfig(const SorterConfig &);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        JSONIZE(json, "modules", modules);
        JSONIZE(json, "sorters", sorterInfos);
    }

public:
    build_service::plugin::ModuleInfos modules;
    SorterInfos sorterInfos;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(SorterConfig);

} // namespace turing
} // namespace suez
