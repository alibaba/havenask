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
class Ha3CavaScorerParam;
class MatchDoc;
class ScorerProvider;
} // namespace ha3

namespace suez {
namespace turing {

class ScorerProvider;

class CavaScorerModuleInfo : public CavaModuleInfo {
public:
    // init(ScorerProvider provider)
    typedef bool (*BeginRequestTuringProtoType)(void *, CavaCtx *, suez::turing::ScorerProvider *);
    typedef bool (*BeginRequestHa3ProtoType)(void *, CavaCtx *, ha3::ScorerProvider *);
    // init(ScorerProvider provider, FeatureLib features, UserResource userResource);
    typedef double (*ScoreProtoType)(void *, CavaCtx *, ::ha3::MatchDoc *);
    typedef void (*BatchScoreProtoType)(void *, CavaCtx *, ::ha3::Ha3CavaScorerParam *);

    CavaScorerModuleInfo(const std::string &className_,
                         ::cava::CavaJitModulePtr &cavaJitModule_,
                         CreateProtoType createFunc_,
                         BeginRequestTuringProtoType beginRequestTuringFunc_,
                         BeginRequestHa3ProtoType beginRequestHa3Func_,
                         ScoreProtoType scoreFunc_,
                         BatchScoreProtoType batchScoreFunc_);
    ~CavaScorerModuleInfo();

private:
    CavaScorerModuleInfo(const CavaScorerModuleInfo &);
    CavaScorerModuleInfo &operator=(const CavaScorerModuleInfo &);

public:
    static CavaModuleInfoPtr create(const std::string &className, ::cava::CavaJitModulePtr &cavaJitModule);

private:
    static BeginRequestTuringProtoType findInitTuringFunc(const std::vector<std::string> &strs,
                                                          ::cava::CavaJitModulePtr &cavaJitModule);
    static BeginRequestHa3ProtoType findInitHa3Func(const std::vector<std::string> &strs,
                                                    ::cava::CavaJitModulePtr &cavaJitModule);
    static ScoreProtoType findProcessFunc(const std::vector<std::string> &strs,
                                          ::cava::CavaJitModulePtr &cavaJitModule);
    static BatchScoreProtoType findBatchProcessFunc(const std::vector<std::string> &strs,
                                                    ::cava::CavaJitModulePtr &cavaJitModule);
    static std::string genBatchProcessFunc(const std::vector<std::string> &strs);

public:
    BeginRequestTuringProtoType beginRequestTuringFunc;
    BeginRequestHa3ProtoType beginRequestHa3Func;
    ScoreProtoType scoreFunc;
    BatchScoreProtoType batchScoreFunc;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(CavaScorerModuleInfo);

} // namespace turing
} // namespace suez
