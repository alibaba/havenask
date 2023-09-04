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
#include "sql/ops/scan/DocIdsScanIterator.h"

#include <assert.h>

#include "ha3/common/DocIdsQuery.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

using autil::Result;

namespace sql {

Result<bool> DocIdsScanIterator::batchSeek(size_t batchSize,
                                           std::vector<matchdoc::MatchDoc> &matchDocs) {
    auto query = dynamic_cast<isearch::common::DocIdsQuery *>(_query.get());
    assert(query);
    const std::vector<docid_t> &docIds = query->getDocIds();
    _matchDocAllocator->batchAllocate(docIds, matchDocs, true);
    _totalScanCount += docIds.size();
    return true;
}

} // namespace sql
