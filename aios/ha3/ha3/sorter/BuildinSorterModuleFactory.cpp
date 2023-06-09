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
#include "ha3/sorter/BuildinSorterModuleFactory.h"

#include <cstddef>
#include <string>

#include "build_service/plugin/ModuleFactory.h"

#include "ha3/isearch.h"
#include "ha3/sorter/DefaultSorter.h"
#include "ha3/sorter/NullSorter.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace sorter {
AUTIL_LOG_SETUP(ha3, BuildinSorterModuleFactory);

BuildinSorterModuleFactory::BuildinSorterModuleFactory() { 
}

BuildinSorterModuleFactory::~BuildinSorterModuleFactory() { 
}

suez::turing::Sorter *BuildinSorterModuleFactory::createSorter(const char *sorterName,
        const KeyValueMap &sorterParameters,
        suez::ResourceReader *resourceReader)
{
    if (std::string(sorterName) == "DefaultSorter") {
        return new DefaultSorter();
    }
    if (std::string(sorterName) == "NullSorter") {
        return new NullSorter();
    }
    return NULL;
}

extern "C"
build_service::plugin::ModuleFactory* createFactory_Sorter() {
    return new BuildinSorterModuleFactory();
}

extern "C"
void destroyFactory_Sorter(build_service::plugin::ModuleFactory *factory) {
    factory->destroy();
}

} // namespace sorter
} // namespace isearch

