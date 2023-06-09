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
#include "indexlib/index/normal/inverted_index/accessor/index_patch_work_item.h"

#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/inplace_index_modifier.h"

namespace indexlib { namespace index {

IE_LOG_SETUP(index, IndexPatchWorkItem);

bool IndexPatchWorkItem::Init(const index::DeletionMapReaderPtr& deletionMapReader,
                              index::InplaceIndexModifier* indexModifier)
{
    if (!deletionMapReader) {
        IE_LOG(ERROR, "Init failed: DeletionMap reader is NULL");
        return false;
    }
    if (!indexModifier) {
        IE_LOG(ERROR, "Init failed: InplaceIndexModifier is NULL");
        return false;
    }
    _deletionMapReader = deletionMapReader;
    _indexModifier = indexModifier;
    return true;
}
bool IndexPatchWorkItem::HasNext() const { return _hasNext; }

size_t IndexPatchWorkItem::ProcessNext()
{
    assert(_patchSegIter);
    std::unique_ptr<SingleTermIndexSegmentPatchIterator> termSegIter = _patchSegIter->NextTerm();
    if (!termSegIter) {
        _hasNext = false;
        return 0;
    }
    indexlib::index::IndexUpdateTermIterator* termIter = termSegIter.get();
    _indexModifier->UpdateIndex(termIter);
    return termIter->GetProcessedDocs();
}
}} // namespace indexlib::index
