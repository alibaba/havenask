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
#ifndef __INDEXLIB_DEVIRTUALIZE_BASE_CLASS_H
#define __INDEXLIB_DEVIRTUALIZE_BASE_CLASS_H

#include <memory>

#include "indexlib/codegen/codegen_example/base.h"
#include "indexlib/codegen/codegen_example/factory.h"
#include "indexlib/codegen/codegen_example/interface.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace codegen {

class DevirtualizeBaseClass final : public Interface
{
public:
    DevirtualizeBaseClass(uint32_t num);
    ~DevirtualizeBaseClass();

private:
    typedef Base ClassType;
    DEFINE_SHARED_PTR(ClassType);
    typedef Factory ClassFactory;

public:
    void Init(const std::string& className) override;
    void Run() override;
    CodegenObject* CreateCodegenObject() override
    {
        using createFunc = CodegenObject*(uint32_t);
        CREATE_OBJECT(createFunc, DevirtualizeBaseClass, mNum);
    }
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

protected:
    bool doCollectAllCode() override;

private:
    ClassTypePtr mObject;
    uint32_t mNum;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DevirtualizeBaseClass);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_DEVIRTUALIZE_BASE_CLASS_H
