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
#include "ha3/proxy/AggResultSort.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/AggFunDescription.h"
#include "ha3/common/AggregateDescription.h"
#include "ha3/common/CommonDef.h"
#include "ha3/proxy/VariableSlabComparator.h"
#include "ha3/search/LayerMetas.h"
#include "matchdoc/MatchDocAllocator.h"

using namespace std;
using namespace isearch::common;
using namespace isearch::search;
namespace isearch {
namespace proxy {
AUTIL_LOG_SETUP(ha3, AggResultSort);

int32_t AggResultSort::countFuncPostion(const AggregateDescription *aggDes) {
    if (NULL == aggDes) {
        return -1;
    }
    const vector<AggFunDescription*> &funcs = aggDes->getAggFunDescriptions();
    for (int32_t i = 0; i < (int32_t)funcs.size(); ++i) {
        if (funcs[i]->getFunctionName() == "count") {
            return i;
        }
    }
    return -1;
}

void AggResultSort::sortAggregateResult(
        const AggregateResultPtr &aggResultPtr,
        uint32_t maxGroupCount, int32_t index, AggSortType sortType)
{
    auto &matchDocVec = aggResultPtr->getAggResultVector();
    const auto &allocatorPtr = aggResultPtr->getMatchDocAllocator();
    uint32_t aggCount = matchDocVec.size();
    VariableSlabComparatorPtr comp;
    if (sortType == AggSortType::AggSortByKey && aggCount > maxGroupCount) {
        const auto *ref = aggResultPtr->getAggFunResultReferBase(index);
        comp = VariableSlabComparator::createComparator(ref);
        std::nth_element(matchDocVec.begin(), matchDocVec.begin() + maxGroupCount,
                         matchDocVec.end(), VariableSlabCompImpl(comp.get()));
    }
    if (index == -1 || sortType == AggSortType::AggSortByKey) {
        const auto ref = allocatorPtr->findReferenceWithoutType(common::GROUP_KEY_REF);
        comp = VariableSlabComparator::createComparator(ref);
    } else {
        const auto *ref = aggResultPtr->getAggFunResultReferBase(index);
        comp = VariableSlabComparator::createComparator(ref);
    }
    assert(comp.get());
    size_t needSorted = aggCount > maxGroupCount ? maxGroupCount : aggCount;
    sort(matchDocVec.begin(), matchDocVec.begin() + needSorted,
         VariableSlabCompImpl(comp.get()));
    for (size_t i = maxGroupCount; i < aggCount; ++i) {
        allocatorPtr->deallocate(matchDocVec[i]);
    }
    matchDocVec.resize(needSorted);
}

} // namespace proxy
} // namespace isearch
