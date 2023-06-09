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

#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/plugin/SorterModuleFactory.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"

namespace isearch {
namespace sorter {

class BuildinSorterModuleFactory : public suez::turing::SorterModuleFactory
{
public:
    BuildinSorterModuleFactory();
    ~BuildinSorterModuleFactory();
private:
    BuildinSorterModuleFactory(const BuildinSorterModuleFactory &);
    BuildinSorterModuleFactory& operator=(const BuildinSorterModuleFactory &);
public:
    virtual void destroy() override { delete this; }
    virtual suez::turing::Sorter* createSorter(const char *sorterName,
                                 const KeyValueMap &sorterParameters,
                                 suez::ResourceReader *resourceReader) override;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BuildinSorterModuleFactory> BuildinSorterModuleFactoryPtr;

} // namespace sorter
} // namespace isearch

