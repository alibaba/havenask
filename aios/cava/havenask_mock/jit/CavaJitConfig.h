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
#include <cstdint>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "cava/common/Common.h"

namespace cava {

enum SafeCheckLevel {
    SCL_NONE,
};

// control module create behavior
class CavaModuleOptions : public autil::legacy::Jsonizable
{
public:
    CavaModuleOptions()
    {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    }
    bool printModule;
    SafeCheckLevel safeCheck;
    bool isPreload;
};

CAVA_TYPEDEF_PTR(CavaModuleOptions);

class JitCompileOptions : public autil::legacy::Jsonizable
{
public:
    JitCompileOptions()
        : debugIR(false)
        , enablePerf(false)
        , optLevel(2)
    {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    }
public:
    bool debugIR;
    bool enablePerf;
    int32_t optLevel;
    CavaModuleOptions userModuleOptions;
};

class CavaJitConfig : public autil::legacy::Jsonizable
{
public:
    CavaJitConfig() {}
    ~CavaJitConfig() {}
private:
    CavaJitConfig(const CavaJitConfig &);
    CavaJitConfig& operator=(CavaJitConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    }
public:
    std::vector<std::string> extraBitCodeFiles;
    std::string bitCodeFile;
    std::vector<std::string> cavaPreLoads;
    std::vector<std::string> externLibraries;
};

} // end namespace cava
