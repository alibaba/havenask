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
#include "suez/turing/expression/plugin/SorterWrapperCreator.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/plugin/SorterWrapper.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(expression, SorterWrapperCreator);

namespace suez {
namespace turing {

SorterWrapperCreator::SorterWrapperCreator(autil::mem_pool::Pool *pool, SorterManager *sorterManager)
    : _pool(pool), _sorterManager(sorterManager) {}

SorterWrapperCreator::~SorterWrapperCreator() {
    for (auto wrapper : _sorterWrappers) {
        POOL_DELETE_CLASS(wrapper);
    }
    _sorterWrappers.clear();
}

SorterWrapper *SorterWrapperCreator::createSorterWrapper(const std::string &name) {
    Sorter *sorter = NULL;
    if (_sorterManager) {
        sorter = _sorterManager->getSorter(name);
    }
    if (!sorter) {
        AUTIL_LOG(WARN, "create sorter: %s failed", name.c_str());
        return NULL;
    }
    auto wrapper = POOL_NEW_CLASS(_pool, SorterWrapper, sorter->clone());
    _sorterWrappers.push_back(wrapper);
    return wrapper;
}

} // namespace turing
} // namespace suez
