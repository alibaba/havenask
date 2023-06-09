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
#include "ha3/common/SortClause.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"

#include "ha3/common/SortDescription.h"
#include "autil/Log.h"

namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, SortClause);

SortClause::SortClause() { 
}

SortClause::~SortClause() { 
    clearSortDescriptions();
}

const std::vector<SortDescription*>& SortClause::getSortDescriptions() const {
    return _sortDescriptions;
}

void SortClause::addSortDescription(SortDescription *sortDescription) {
    if (sortDescription) {
        AUTIL_LOG(DEBUG, "originalString: %s", sortDescription->getOriginalString().c_str());
        _sortDescriptions.push_back(sortDescription);
    }
}

void SortClause::clearSortDescriptions() {
    for (vector<SortDescription*>::iterator it = _sortDescriptions.begin();
         it != _sortDescriptions.end(); it++)
    {
        delete *it;
        *it = NULL;
    }
    _sortDescriptions.clear();
}

void SortClause::addSortDescription(SyntaxExpr* syntaxExpr,
                                    bool isRank,
                                    bool sortAscendFlag) 
{
    SortDescription *sortDescription = new SortDescription();
    sortDescription->setExpressionType(isRank ? SortDescription::RS_RANK : SortDescription::RS_NORMAL);
//    sortDescription->setOriginalString(originalString);
    sortDescription->setSortAscendFlag(sortAscendFlag);
    sortDescription->setRootSyntaxExpr(syntaxExpr);
    addSortDescription(sortDescription);
}

SortDescription* SortClause::getSortDescription(uint32_t pos) {
    if (pos >= _sortDescriptions.size()) {
        return NULL;
    }
    return _sortDescriptions[pos];
}

void SortClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_sortDescriptions);
    dataBuffer.write(_originalString);
}

void SortClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_sortDescriptions);
    dataBuffer.read(_originalString);
}

string SortClause::toString() const {
    string sortStr;
    size_t count = _sortDescriptions.size();
    for (size_t i = 0; i < count; i++) {
        sortStr.append(_sortDescriptions[i]->toString());
        sortStr.append(";");
    }
    return sortStr;
}

} // namespace common
} // namespace isearch

