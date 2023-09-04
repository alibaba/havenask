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

#include <vector>

#include "autil/Log.h"
#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/provider/SorterProvider.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace suez {
namespace turing {

class SorterWrapper {
public:
    SorterWrapper(Sorter *sorter);
    ~SorterWrapper();

private:
    SorterWrapper(const SorterWrapper &);
    SorterWrapper &operator=(const SorterWrapper &);

public:
    bool beginSort(SorterProvider *sorterProvider);

    void sort(SortParam &sortParam);

    void endSort() { _sorter->endSort(); }

    SorterProvider *getSorterProvider() { return _sorterProvider; }

    std::string sorterName() const { return _sorter->getSorterName(); }

    const std::vector<SortExprMeta> &getSorterExprMetas() const { return _sorter->getSorterExprMetas(); }

private:
    void prepareForSort(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs) {
        ExpressionEvaluator<ExpressionVector> evaluator(_sorterProvider->getNeedEvaluateAttrs(),
                                                        _sorterProvider->getAllocator()->getSubDocAccessor());
        evaluator.batchEvaluateExpressions(matchDocs.data(), matchDocs.size());
        _sorterProvider->batchPrepareTracer(matchDocs.data(), matchDocs.size());
    }

private:
    Sorter *_sorter;
    SorterProvider *_sorterProvider;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(SorterWrapper);

} // namespace turing
} // namespace suez
