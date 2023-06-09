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
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleTermIndexSegmentPatchIterator);

bool SingleTermIndexSegmentPatchIterator::Next(ComplexDocId* docId)
{
    if (GetIndexId() == INVALID_INDEXID) {
        return false;
    }
    if (!_patchHeap) {
        return false;
    }
    while (!_patchHeap->empty()) {
        IndexPatchFileReader* patchReader = _patchHeap->top();
        if (patchReader->GetCurrentTermKey() != GetTermKey()) {
            return false;
        }
        _patchHeap->pop();
        *docId = patchReader->GetCurrentDocId();
        if (patchReader->HasNext()) {
            patchReader->Next();
            _patchHeap->push(patchReader);
        } else {
            delete patchReader;
        }
        if (docId->DocId() == _lastDoc) {
            continue;
        }
        _lastDoc = docId->DocId();
        ++_processedDocs;
        return true;
    }
    return false;
}
} // namespace indexlib::index
