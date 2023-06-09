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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexSegmentReader.h"

#include "indexlib/index/inverted_index/InvertedIndexUtil.h"

namespace indexlib::index {
namespace {
using document::ModifiedTokens;
using index::DictKeyInfo;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DynamicIndexSegmentReader);

DynamicIndexSegmentReader::DynamicIndexSegmentReader(const DynamicIndexSegmentReader::PostingTable* postingTable,
                                                     bool isNumberIndex, DynamicPostingWriter** nullPostingWriter)
    : _dynamicPostingTable(postingTable)
    , _nullTermPosting(nullPostingWriter)
    , _isNumberIndex(isNumberIndex)
{
}

DynamicIndexSegmentReader::~DynamicIndexSegmentReader() {}

PostingWriter* DynamicIndexSegmentReader::GetPostingWriter(const DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return _nullTermPosting ? *_nullTermPosting : nullptr;
    } else if (_dynamicPostingTable) {
        return _dynamicPostingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, key.GetKey()),
                                          nullptr);
    }
    return nullptr;
}

bool DynamicIndexSegmentReader::GetSegmentPosting(const DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                                                  autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption,
                                                  InvertedIndexSearchTracer* tracer) const
{
    assert(_dynamicPostingTable);
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return false;
    }
    segPosting.Init(baseDocId, 0, postingWriter);
    return true;
}

void DynamicIndexSegmentReader::Update(docid_t docId, const DictKeyInfo& key, bool isDelete)
{
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return;
    }
    auto dynamicWriter = dynamic_cast<DynamicPostingWriter*>(postingWriter);
    if (dynamicWriter == nullptr) {
        AUTIL_LOG(ERROR, "update doc[%d] failed, posting[%s] is not dynamic", docId, typeid(*postingWriter).name());
        return;
    }
    dynamicWriter->Update(docId, isDelete ? ModifiedTokens::Operation::REMOVE : ModifiedTokens::Operation::ADD);
}
} // namespace indexlib::index
