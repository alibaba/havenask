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
#ifndef __INDEXLIB_CODEGEN_CHECKER_H
#define __INDEXLIB_CODEGEN_CHECKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace codegen {

#define COLLECT_CONST_MEM(checkerPtr, var)                                                                             \
    {                                                                                                                  \
        std::string varName(#var);                                                                                     \
        if (__builtin_constant_p(var)) {                                                                               \
            std::cout << "value:" << varName << " is const" << std::endl;                                              \
            checkerPtr->CollectCodegenInfo(varName, "true");                                                           \
        } else {                                                                                                       \
            std::cout << "value:" << varName << " not const" << std::endl;                                             \
            checkerPtr->CollectCodegenInfo(varName, "false");                                                          \
        }                                                                                                              \
    }

#define COLLECT_TYPE_DEFINE(checkerPtr, type)                                                                          \
    {                                                                                                                  \
        std::string typeName(#type);                                                                                   \
        std::string value(typeid(type).name());                                                                        \
        std::cout << "typeName:" << typeName << ", value:" << value << std::endl;                                      \
        checkerPtr->CollectCodegenInfo(typeName, value);                                                               \
    }

class CodegenChecker
{
public:
    CodegenChecker();
    ~CodegenChecker();

public:
    void CollectCodegenInfo(const std::string& key, const std::string& value) { mCodegenInfo[key] = value; }
    bool CheckTypeRedefine(std::string typeName, std::string actualTypeName);
    bool CheckCompileConstVar(std::string varName);

private:
    typedef std::map<std::string, std::string> KVMap;
    KVMap mCodegenInfo;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CodegenChecker);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_CODEGEN_CHECKER_H
