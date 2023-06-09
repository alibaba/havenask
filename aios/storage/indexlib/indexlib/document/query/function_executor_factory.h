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
#ifndef __INDEXLIB_FUNCTION_EXECUTOR_FACTORY_H
#define __INDEXLIB_FUNCTION_EXECUTOR_FACTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/function_executor.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/ModuleFactory.h"

namespace indexlib { namespace document {

class FunctionExecutorFactory : public plugin::ModuleFactory
{
public:
    FunctionExecutorFactory();
    virtual ~FunctionExecutorFactory();

public:
    void destroy() override { delete this; }

public:
    virtual FunctionExecutor* CreateFunctionExecutor(const QueryFunction& function, const util::KeyValueMap& kvMap) = 0;

public:
    const static std::string FACTORY_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FunctionExecutorFactory);

extern "C" plugin::ModuleFactory* createFunctionExecutorFactory();
extern "C" void destroyFunctionExecutorFactory(plugin::ModuleFactory* factory);
}} // namespace indexlib::document

#endif //__INDEXLIB_FUNCTION_EXECUTOR_FACTORY_H
