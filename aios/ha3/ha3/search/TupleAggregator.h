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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/NormalAggregator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/expression/cava/common/CavaAggModuleInfo.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc
namespace suez {
namespace turing {
class TupleAttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {
typedef typename UnorderedMapTraits<uint64_t>::GroupMapType TupleAggregatorGroupMapType;
class TupleAggregator : public NormalAggregator<uint64_t, uint64_t, TupleAggregatorGroupMapType>
{
public:
    TupleAggregator(suez::turing::AttributeExpressionTyped<uint64_t> *attriExpr,
                    uint32_t maxKeyCount,
                    autil::mem_pool::Pool *pool,
                    uint32_t aggThreshold = 0,
                    uint32_t sampleStep = 1,
                    uint32_t maxSortCount = 0,
                    suez::turing::QueryResource *queryResource = NULL,
                    const std::string &sep = "|");
    ~TupleAggregator();
private:
    TupleAggregator(const TupleAggregator &);
    TupleAggregator& operator=(const TupleAggregator &);
public:
    void initGroupKeyRef() override;
    void setGroupKey(matchdoc::MatchDoc aggMatchDoc,
                     const uint64_t &groupKeyValue) override;
    suez::turing::CavaAggModuleInfoPtr codegen() override {
        return suez::turing::CavaAggModuleInfoPtr();
    }
private:
    suez::turing::TupleAttributeExpression *_tupleAttributeExpression;
    matchdoc::Reference<std::string> *_groupKeyRef;
    std::string _sep;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TupleAggregator> TupleAggregatorPtr;

} // namespace search
} // namespace isearch

