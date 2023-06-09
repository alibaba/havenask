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
#include "indexlib/codegen/codegen_example/composite_devirtualize_class.h"

using namespace std;

namespace indexlib { namespace codegen {
IE_LOG_SETUP(codegen, CompositeDevirtualizeClass);

CompositeDevirtualizeClass::CompositeDevirtualizeClass() {}

CompositeDevirtualizeClass::~CompositeDevirtualizeClass() {}

void CompositeDevirtualizeClass::Init(const std::string& name)
{
    mDevirtualizeA.reset(new DevirtualizeA(1));
    mDevirtualizeA->Init("A");

    mDevirtualizeB.reset(new DevirtualizeB(2));
    mDevirtualizeB->Init("B");
}

void CompositeDevirtualizeClass::Run()
{
    std::cout << "=====run devirtualize A=====" << std::endl;
    mDevirtualizeA->Run();

    std::cout << "=====run devirtualize B=====" << std::endl;
    mDevirtualizeB->Run();
}

bool CompositeDevirtualizeClass::doCollectAllCode()
{
    INIT_CODEGEN_INFO(CompositeDevirtualizeClass, true); // init codegen info, param: className, needRename
    mDevirtualizeA->collectAllCode();
    combineCodegenInfos(mDevirtualizeA.get());
    COLLECT_TYPE_REDEFINE(DevirtualizeA, mDevirtualizeA->getTargetClassName());

    mDevirtualizeB->collectAllCode();
    combineCodegenInfos(mDevirtualizeB.get());
    COLLECT_TYPE_REDEFINE(DevirtualizeB, mDevirtualizeB->getTargetClassName());
    return true;
}
}} // namespace indexlib::codegen
