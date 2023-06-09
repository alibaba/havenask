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
#ifndef __INDEXLIB_FACTORY_H
#define __INDEXLIB_FACTORY_H

#include <memory>

#include "indexlib/codegen/codegen_example/base.h"
#include "indexlib/codegen/codegen_example/derived_class_A.h"
#include "indexlib/codegen/codegen_example/derived_class_B.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace codegen {

class Factory
{
public:
    Factory();
    ~Factory();

private:
    typedef DerivedClassA ClassA;
    typedef DerivedClassB ClassB;

public:
    static Base* Create(const std::string& className);
    static void CollectAllCode(const std::map<std::string, std::string>& renameMap, AllCodegenInfo& codegenInfos);
    static void TEST_collectCodegenResult(CodegenObject::CodegenCheckers& checkers, std::string id);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Factory);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_FACTORY_H
