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

#include <string>

#include "autil/Log.h"
#include "build_service/plugin/ModuleFactory.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"

namespace suez {
namespace turing {
class Sorter;

static const std::string MODULE_FUNC_SORTER = "_Sorter";

class SorterModuleFactory : public build_service::plugin::ModuleFactory {
public:
    SorterModuleFactory() {}
    virtual ~SorterModuleFactory() {}

private:
    SorterModuleFactory(const SorterModuleFactory &);
    SorterModuleFactory &operator=(const SorterModuleFactory &);

public:
    virtual Sorter *
    createSorter(const char *sorterName, const KeyValueMap &parameters, suez::ResourceReader *resourceReader) = 0;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(SorterModuleFactory);

} // namespace turing
} // namespace suez
