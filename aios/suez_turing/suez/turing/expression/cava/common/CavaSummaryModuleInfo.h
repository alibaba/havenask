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

#include "autil/Log.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/common.h"

class CavaCtx;

namespace ha3 {
class SummaryProvider;
class SummaryDoc;
} // namespace ha3

namespace suez {
namespace turing {

class CavaSummaryModuleInfo : public CavaModuleInfo {
public:
    // boolean init(SummaryProvider provider)
    typedef bool (*InitProtoType)(void *, CavaCtx *, ::ha3::SummaryProvider *);
    // void process(SummaryDoc doc)
    typedef bool (*ProcessProtoType)(void *, CavaCtx *, ::ha3::SummaryDoc *);

    CavaSummaryModuleInfo(const std::string &className,
                          ::cava::CavaJitModulePtr &cavaJitModule,
                          CreateProtoType createFunc,
                          InitProtoType initFunc,
                          ProcessProtoType processFunc);
    ~CavaSummaryModuleInfo();

private:
    CavaSummaryModuleInfo(const CavaSummaryModuleInfo &);
    CavaSummaryModuleInfo &operator=(const CavaSummaryModuleInfo &);

public:
    static CavaModuleInfoPtr create(const std::string &className, ::cava::CavaJitModulePtr &cavaJitModule);
    static InitProtoType findInitFunc(const std::vector<std::string> &strs, ::cava::CavaJitModulePtr &cavaJitModule);
    static ProcessProtoType findProcessFunc(const std::vector<std::string> &strs,
                                            ::cava::CavaJitModulePtr &cavaJitModule);

public:
    InitProtoType initFunc;
    ProcessProtoType processFunc;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(CavaSummaryModuleInfo);

} // namespace turing
} // namespace suez
