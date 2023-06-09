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
#ifndef __INDEXLIB_FUNCTION_EXECUTOR_FACTORY_WRAPPER_H
#define __INDEXLIB_FUNCTION_EXECUTOR_FACTORY_WRAPPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/function_executor_factory.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/module_factory_wrapper.h"

namespace indexlib { namespace document {

class FunctionExecutorFactoryWrapper;
DEFINE_SHARED_PTR(FunctionExecutorFactoryWrapper);

class FunctionExecutorFactoryWrapper : public plugin::ModuleFactoryWrapper<FunctionExecutorFactory>
{
public:
    FunctionExecutorFactoryWrapper();
    ~FunctionExecutorFactoryWrapper();

public:
    using plugin::ModuleFactoryWrapper<FunctionExecutorFactory>::Init;
    using plugin::ModuleFactoryWrapper<FunctionExecutorFactory>::GetPluginManager;
    using plugin::ModuleFactoryWrapper<FunctionExecutorFactory>::IsBuiltInFactory;

public:
    FunctionExecutor* CreateFunctionExecutor(const QueryFunction& function, const util::KeyValueMap& kvMap) const;

protected:
    FunctionExecutorFactory* CreateBuiltInFactory() const override;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::document

#endif //__INDEXLIB_FUNCTION_EXECUTOR_FACTORY_WRAPPER_H
