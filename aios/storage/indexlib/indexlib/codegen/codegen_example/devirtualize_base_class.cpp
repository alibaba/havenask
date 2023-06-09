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
#include "indexlib/codegen/codegen_example/devirtualize_base_class.h"

using namespace std;

namespace indexlib { namespace codegen {
IE_LOG_SETUP(codegen, DevirtualizeBaseClass);

DevirtualizeBaseClass::DevirtualizeBaseClass(uint32_t num) { mNum = num; }

DevirtualizeBaseClass::~DevirtualizeBaseClass() {}

void DevirtualizeBaseClass::Init(const std::string& className)
{
    mObject.reset((ClassType*)ClassFactory::Create(className));
}

void DevirtualizeBaseClass::Run()
{
    if (mNum % 2 == 1) {
        std::cout << "odd number" << std::endl;
    } else {
        std::cout << "even number" << std::endl;
    }
    std::cout << mObject->GetName() << std::endl;
}

bool DevirtualizeBaseClass::doCollectAllCode()
{
    INIT_CODEGEN_INFO(DevirtualizeBaseClass, true); // init codegen info, param: className, needRename
    mObject->collectAllCode();
    combineCodegenInfos(mObject.get());
    COLLECT_TYPE_REDEFINE(ClassType, mObject->getTargetClassName());
    COLLECT_CONST_MEM_VAR(mNum);
    map<string, string> renameMap;
    renameMap[mObject->getSourceClassName()] = mObject->getTargetClassName();
    AllCodegenInfo codegenInfos;
    ClassFactory::CollectAllCode(renameMap, codegenInfos);
    COLLECT_TYPE_REDEFINE(ClassFactory, codegenInfos.GetTargetClassName("Factory"));
    combineCodegenInfos(codegenInfos);
    return true;
}

void DevirtualizeBaseClass::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    CodegenCheckerPtr checker(new CodegenChecker);
    COLLECT_CONST_MEM(checker, mNum);
    COLLECT_TYPE_DEFINE(checker, ClassType);
    checkers[std::string("DevirtualizeBaseClass") + id] = checker;
    ClassFactory::TEST_collectCodegenResult(checkers, id);
}
}} // namespace indexlib::codegen
