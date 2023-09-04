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

#include <stdint.h>
#include <string>

#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "turing_ops_util/variant/SortExprMeta.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace suez {
namespace turing {
class SorterProvider;

struct SortParam {
    SortParam(autil::mem_pool::Pool *pool) : matchDocs(pool) {}
    SortParam(const autil::mem_pool::PoolVector<matchdoc::MatchDoc> &docs) : matchDocs(docs) {}
    // input
    uint32_t requiredTopK;
    // input and output
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> matchDocs;
    uint32_t totalMatchCount;
    uint32_t actualMatchCount;
};

class Sorter {
public:
    Sorter(const std::string &name = "sorter") : _name(name) {}
    virtual ~Sorter() {}

public:
    // For each request, we will call clone to get a new sorter
    virtual Sorter *clone() = 0;

    // called in before sort, to prepare for each sort.
    virtual bool beginSort(SorterProvider *provider) = 0;

    // do sort for match docs.
    virtual void sort(SortParam &sortParam) = 0;

    // called after each sort
    virtual void endSort() = 0;

    // call to recycle/delete this scorer
    virtual void destroy() = 0;

    const std::string &getSorterName() const { return _name; }
    const std::vector<SortExprMeta> &getSorterExprMetas() const { return _sortMetas; }

protected:
    std::vector<SortExprMeta> _sortMetas;

private:
    std::string _name;
};

SUEZ_TYPEDEF_PTR(Sorter);

} // namespace turing
} // namespace suez
