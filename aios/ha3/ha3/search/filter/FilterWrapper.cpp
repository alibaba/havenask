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
#include "ha3/search/FilterWrapper.h"

#include <cstddef>

#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/search/Filter.h"
#include "ha3/search/JoinFilter.h"
#include "matchdoc/SubDocAccessor.h"

using namespace std;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FilterWrapper);

FilterWrapper::FilterWrapper()
    : _joinFilter(NULL)
    , _subDocFilter(NULL)
    , _filter(NULL)
    , _subExprFilter(NULL) {}

FilterWrapper::~FilterWrapper() {
    POOL_DELETE_CLASS(_joinFilter);
    POOL_DELETE_CLASS(_subDocFilter);
    POOL_DELETE_CLASS(_filter);
    POOL_DELETE_CLASS(_subExprFilter);
}

bool SubDocFilter::pass(matchdoc::MatchDoc matchDoc) {
    return !_subDocAccessor->allSubDocSetDeletedFlag(matchDoc);
}

} // namespace search
} // namespace isearch
