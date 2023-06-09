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
#include <queue>

#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/IndexPatchFileReader.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::index {

class SingleTermIndexSegmentPatchIterator : public IndexUpdateTermIterator
{
public:
    SingleTermIndexSegmentPatchIterator(IndexPatchHeap* heap, index::DictKeyInfo termKey, segmentid_t segmentId,
                                        indexid_t indexId)
        : IndexUpdateTermIterator(termKey, segmentId, indexId)
        , _patchHeap(heap)
        , _lastDoc(INVALID_DOCID)
        , _processedDocs(0)
    {
    }
    bool Next(ComplexDocId* doc) override;
    size_t GetProcessedDocs() const override { return _processedDocs; }

private:
    IndexPatchHeap* _patchHeap;
    docid_t _lastDoc;
    size_t _processedDocs;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
