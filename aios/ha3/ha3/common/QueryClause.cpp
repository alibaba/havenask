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
#include "ha3/common/QueryClause.h"

#include <assert.h>
#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"

#include "ha3/common/Query.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, QueryClause);

QueryClause::QueryClause() { 
}

QueryClause::QueryClause(Query* query) {
    _rootQuerys.push_back(query);
}

QueryClause::~QueryClause() { 
    for (size_t i = 0; i < _rootQuerys.size(); ++i) {
        delete _rootQuerys[i];
        _rootQuerys[i] = NULL;
    }
}

void QueryClause::setRootQuery(Query *query, uint32_t layer) {
    if (layer >= _rootQuerys.size()) {
        _rootQuerys.resize(layer + 1);
    }
    DELETE_AND_SET_NULL(_rootQuerys[layer]);
    _rootQuerys[layer] = query;
}

Query *QueryClause::getRootQuery(uint32_t layer) const {
    if (layer >= _rootQuerys.size()) {
        return NULL;
    }
    return _rootQuerys[layer];
}

void QueryClause::insertQuery(Query *query, int32_t pos) {
    assert(query);
    std::vector<Query*>::iterator it = _rootQuerys.begin();
    while (pos-- > 0) {
        ++it;
    }
    _rootQuerys.insert(it, query);
}

void QueryClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_rootQuerys);
    dataBuffer.write(_originalString);
}

void QueryClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_rootQuerys);
    dataBuffer.read(_originalString);
}

string QueryClause::toString() const {
    size_t queryNum = _rootQuerys.size();
    string queryClauseStr;
    for (size_t i = 0; i < queryNum; i++ ){
        queryClauseStr.append(_rootQuerys[i]->toString());
        queryClauseStr.append("|");
    }
    return queryClauseStr;
}

} // namespace common
} // namespace isearch

