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

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/common.h"

class CavaCtx;

namespace cava {
class CavaJitModule;

typedef std::shared_ptr<CavaJitModule> CavaJitModulePtr;
} // namespace cava

namespace suez {
namespace turing {

typedef void *(*CreateProtoType)(CavaCtx *);

class CavaModuleInfo {
public:
    CavaModuleInfo(const std::string &className_,
                   ::cava::CavaJitModulePtr &cavaJitModule_,
                   CreateProtoType createFunc_ = nullptr);
    virtual ~CavaModuleInfo();

private:
    CavaModuleInfo(const CavaModuleInfo &);
    CavaModuleInfo &operator=(const CavaModuleInfo &);

public:
    static std::string genCreateFunc(const std::vector<std::string> &strs);
    static CreateProtoType findCreateFunc(const std::vector<std::string> &strs,
                                          ::cava::CavaJitModulePtr &cavaJitModule);
    static std::string genInitFunc(const std::vector<std::string> &strs, const std::vector<std::string> &params);
    static std::string genProcessFunc(const std::vector<std::string> &strs,
                                      const std::vector<std::string> &params,
                                      const std::string &methodName = "process");

public:
    std::string className;
    CreateProtoType createFunc;

protected:
    ::cava::CavaJitModulePtr cavaJitModule;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(CavaModuleInfo);

} // namespace turing
} // namespace suez
