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
#include <stdint.h>
#include <vector>

#include "ha3/sql/ops/scan/ScanIterator.h"
#include "ha3/common/Query.h"
#include "matchdoc/MatchDocAllocator.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace isearch {
namespace sql {

class DocIdsScanIterator : public ScanIterator {
public:
    DocIdsScanIterator(const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                       common::TimeoutTerminator *timeoutTerminator,
                       const common::QueryPtr &query)
        : ScanIterator(matchDocAllocator, timeoutTerminator)
        , _query(query)
    {
    }
    virtual ~DocIdsScanIterator() {}

public:
    autil::Result<bool> batchSeek(size_t batchSize,
                                  std::vector<matchdoc::MatchDoc> &matchDocs) override;

private:
    common::QueryPtr _query;
};

typedef std::shared_ptr<DocIdsScanIterator> DocIdsScanIteratorPtr;
} // namespace sql
} // namespace isearch
