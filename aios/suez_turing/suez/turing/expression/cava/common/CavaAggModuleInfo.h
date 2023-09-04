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

#include "cava/common/Common.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/common.h"

class CavaCtx;

namespace ha3 {
class Aggregator;
}

namespace unsafe {
class MatchDocs;
}

namespace suez {
namespace turing {

class CavaAggModuleInfo : public CavaModuleInfo {
public:
    typedef void *(*AggCreateProtoType)(CavaCtx *, ::ha3::Aggregator *);
    typedef uint (*AggCountProtoType)(void *, CavaCtx *);
    typedef void (*AggBatchProtoType)(void *, CavaCtx *, ::unsafe::MatchDocs *, uint);
    typedef void (*AggResultProtoType)(void *, CavaCtx *, ::unsafe::MatchDocs *, uint);

public:
    CavaAggModuleInfo(const std::string &className,
                      ::cava::CavaJitModulePtr &cavaJitModule,
                      AggCreateProtoType createProtoType_,
                      AggCountProtoType countProtoType_,
                      AggBatchProtoType batchProtoType_,
                      AggResultProtoType resultProtoType_);
    ~CavaAggModuleInfo();

public:
    static CavaModuleInfoPtr create(::cava::CavaJitModulePtr &cavaJitModule);

public:
    AggCreateProtoType createProtoType;
    AggCountProtoType countProtoType;
    AggBatchProtoType batchProtoType;
    AggResultProtoType resultProtoType;
};

SUEZ_TYPEDEF_PTR(CavaAggModuleInfo);

} // namespace turing
} // namespace suez
