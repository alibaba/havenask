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
#include <memory>

#include "indexlib/codegen/code_factory.h"
#include "indexlib/codegen/codegen_checker.h"
#include "indexlib/codegen/codegen_info.h"

namespace indexlib { namespace codegen {

class CodegenObject
{
public:
    virtual ~CodegenObject() = default;
    bool collectAllCode() { return true; }

public:
    typedef std::map<std::string, CodegenCheckerPtr> CodegenCheckers;
    virtual void TEST_collectCodegenResult(CodegenCheckers& checker, std::string id = "") {}
    virtual bool doCollectAllCode() { return true; }
    void combineCodegenInfos(CodegenObject* other) {}
    void combineCodegenInfos(const AllCodegenInfo& codegenInfos) {}

protected:
    std::vector<CodegenInfo> mSubCodegenInfos;
};

}} // namespace indexlib::codegen

#define CREATE_CODEGEN_OBJECT(ret, funcType, className, args...)                                                       \
    {                                                                                                                  \
        [[maybe_unused]] funcType _;                                                                                   \
    }
#define COLLECT_CONST_MEM_VAR(var)
#define COLLECT_TYPE_REDEFINE(typeName, targetType)
#define INIT_CODEGEN_INFO(originClassName, needRename)
#define PARTICIPATE_CODEGEN(className, codegenInfo)
