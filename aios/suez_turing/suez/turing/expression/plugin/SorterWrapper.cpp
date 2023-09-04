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
#include "suez/turing/expression/plugin/SorterWrapper.h"

#include <stddef.h>

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SorterWrapper);

SorterWrapper::SorterWrapper(Sorter *sorter) : _sorter(sorter), _sorterProvider(NULL) {}

SorterWrapper::~SorterWrapper() { _sorter->destroy(); }

bool SorterWrapper::beginSort(SorterProvider *sorterProvider) {
    _sorterProvider = sorterProvider;
    auto ret = _sorter->beginSort(_sorterProvider);
    auto allocator = sorterProvider->getAllocator();
    allocator->extend();
    if (allocator->hasSubDocAllocator()) {
        allocator->extendSub();
    }
    return ret;
}

void SorterWrapper::sort(SortParam &sortParam) {
    prepareForSort(sortParam.matchDocs);
    _sorter->sort(sortParam);
}

} // namespace turing
} // namespace suez
