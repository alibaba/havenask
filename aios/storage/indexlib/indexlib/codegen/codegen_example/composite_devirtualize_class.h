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
#ifndef __INDEXLIB_COMPOSITE_DEVIRTUALIZE_CLASS_H
#define __INDEXLIB_COMPOSITE_DEVIRTUALIZE_CLASS_H

#include <memory>

#include "indexlib/codegen/codegen_example/devirtualize_base_class.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace codegen {

class CompositeDevirtualizeClass final : public Interface
{
public:
    CompositeDevirtualizeClass();
    ~CompositeDevirtualizeClass();

private:
    typedef DevirtualizeBaseClass DevirtualizeA;
    DEFINE_SHARED_PTR(DevirtualizeA);
    typedef DevirtualizeBaseClass DevirtualizeB;
    DEFINE_SHARED_PTR(DevirtualizeB);

public:
    void Init(const std::string& name) override;
    void Run() override;

    CodegenObject* CreateCodegenObject() override
    {
        using createFunc = CodegenObject*();
        CREATE_OBJECT(createFunc, CompositeDevirtualizeClass);
    }

    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override
    {
        CodegenCheckerPtr checker(new CodegenChecker);
        COLLECT_TYPE_DEFINE(checker, DevirtualizeA);
        COLLECT_TYPE_DEFINE(checker, DevirtualizeB);
        checkers["CompositeDevirtualizeClass"] = checker;
        mDevirtualizeA->TEST_collectCodegenResult(checkers, "A");
        mDevirtualizeB->TEST_collectCodegenResult(checkers, "B");
    }

protected:
    bool doCollectAllCode() override;

private:
    DevirtualizeAPtr mDevirtualizeA;
    DevirtualizeBPtr mDevirtualizeB;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompositeDevirtualizeClass);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_COMPOSITE_DEVIRTUALIZE_CLASS_H
