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

#include <memory>
#include <stddef.h>
#include <vector>

#include "autil/result/Result.h"
#include "ha3/common/Query.h"
#include "ha3/common/TimeoutTerminator.h"
#include "matchdoc/MatchDocAllocator.h"
#include "sql/ops/scan/ScanIterator.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace sql {

class DocIdsScanIterator : public ScanIterator {
public:
    DocIdsScanIterator(const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                       isearch::common::TimeoutTerminator *timeoutTerminator,
                       const isearch::common::QueryPtr &query)
        : ScanIterator(matchDocAllocator, timeoutTerminator)
        , _query(query) {}
    virtual ~DocIdsScanIterator() {}

public:
    autil::Result<bool> batchSeek(size_t batchSize,
                                  std::vector<matchdoc::MatchDoc> &matchDocs) override;

private:
    isearch::common::QueryPtr _query;
};

typedef std::shared_ptr<DocIdsScanIterator> DocIdsScanIteratorPtr;
} // namespace sql
