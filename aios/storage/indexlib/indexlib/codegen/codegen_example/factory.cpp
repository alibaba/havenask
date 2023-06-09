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
#include "indexlib/codegen/codegen_example/factory.h"

using namespace std;

namespace indexlib { namespace codegen {
IE_LOG_SETUP(codegen, Factory);

Factory::Factory() {}

Factory::~Factory() {}

Base* Factory::Create(const std::string& className)
{
    if (className == string("A")) {
        return new ClassA;
    }
    if (className == string("B")) {
        return new ClassB;
    }
    return NULL;
}

void Factory::CollectAllCode(const std::map<std::string, std::string>& renameMap, AllCodegenInfo& allCodegenInfo)
{
    codegen::CodegenInfo codegenInfo;
    STATIC_INIT_CODEGEN_INFO(Factory, true, codegenInfo);
    // STATIC_COLLECT_CONST_MEM_VAR(mVar, varValue, codegenInfo); //for collect const member
    auto iter = renameMap.find("DerivedClassA");
    if (iter != renameMap.end()) {
        STATIC_COLLECT_TYPE_REDEFINE(ClassA, iter->second, codegenInfo);
    }
    iter = renameMap.find("DerivedClassB");
    if (iter != renameMap.end()) {
        STATIC_COLLECT_TYPE_REDEFINE(ClassB, iter->second, codegenInfo);
    }
    allCodegenInfo.AppendAndRename(codegenInfo);
}

void Factory::TEST_collectCodegenResult(CodegenObject::CodegenCheckers& checkers, string id)
{
    CodegenCheckerPtr checker(new CodegenChecker);
    COLLECT_TYPE_DEFINE(checker, ClassA);
    COLLECT_TYPE_DEFINE(checker, ClassB);
    checkers[string("Factory") + id] = checker;
}
}} // namespace indexlib::codegen
