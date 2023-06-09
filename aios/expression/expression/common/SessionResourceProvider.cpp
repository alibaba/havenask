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
#include "expression/common/SessionResourceProvider.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, SessionResourceProvider);

const string SessionResourceProvider::VARIABLE_GROUP_NAME = "__@_variable_group_@__";
SessionResourceProvider::SessionResourceProvider(const SessionResource &resource)
    : _sessionResource(&resource)
    , _isSubScope(false)
{
}

SessionResourceProvider::~SessionResourceProvider() {
}

void SessionResourceProvider::addSortMeta(const std::string &name,
        matchdoc::ReferenceBase* ref, bool isAsc)
{
    SortMeta meta;
    meta.name = name;
    meta.ref = ref;
    meta.isAsc = isAsc;
    _sessionResource->sortMetas->push_back(meta);
}

const std::vector<SortMeta> &SessionResourceProvider::getSortMeta() const {
    return *_sessionResource->sortMetas;
}

}
