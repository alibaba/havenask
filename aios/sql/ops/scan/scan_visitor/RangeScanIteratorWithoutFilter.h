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

#include "autil/result/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/LayerMetas.h"
#include "matchdoc/MatchDocAllocator.h"
#include "sql/ops/scan/ScanIterator.h"

namespace indexlib::index {
class DeletionMapReaderAdaptor;
}

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace sql {

class RangeScanIteratorWithoutFilter : public ScanIterator {
public:
    RangeScanIteratorWithoutFilter(
        const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
        const std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> &delMapReader,
        const isearch::search::LayerMetaPtr &layerMeta,
        isearch::common::TimeoutTerminator *timeoutTerminator = NULL);

    autil::Result<bool> batchSeek(size_t batchSize,
                                  std::vector<matchdoc::MatchDoc> &matchDocs) override;

private:
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> _delMapReader;
    isearch::search::LayerMetaPtr _layerMeta; // hold resource for queryexecutor use raw pointer
    size_t _rangeIdx;
    int32_t _curId;
};

typedef std::shared_ptr<RangeScanIteratorWithoutFilter> RangeScanIteratorWithoutFilterPtr;
} // namespace sql
