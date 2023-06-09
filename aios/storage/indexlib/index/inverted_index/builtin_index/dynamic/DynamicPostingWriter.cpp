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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingWriter.h"

#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/util/NumericUtil.h"

namespace indexlib::index {
namespace {
using document::ModifiedTokens;
}
AUTIL_LOG_SETUP(indexlib.index, DynamicPostingWriter);

DynamicPostingWriter::DynamicPostingWriter(NodeManager* nodeManager, Stat* stat)
    : _dF(0)
    , _totalTF(0)
    , _termPayload(0)
    , _currentTF(0)
    , _tree(TREE_MAX_SLOT_SHIFT, nodeManager)
    , _stat(stat)
{
}

DynamicPostingWriter::~DynamicPostingWriter() {}

void DynamicPostingWriter::EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap)
{
    return EndDocument(docId, docPayload);
}

void DynamicPostingWriter::EndDocument(docid_t docId, docpayload_t docPayload)
{
    _dF++;
    _currentTF = 0;
    _tree.Insert(docId);
    _lastDocId = docId;
    _stat->totalDocCount++;
}

void DynamicPostingWriter::Update(docid_t docId, ModifiedTokens::Operation op)
{
    if (op == ModifiedTokens::Operation::ADD) {
        if (_tree.Insert(docId)) {
            _dF++;
            _stat->totalDocCount++;
        }
    } else if (op == ModifiedTokens::Operation::REMOVE) {
        if (_tree.Remove(docId)) {
            _dF--;
            _stat->totalDocCount--;
        }
    }
}

void DynamicPostingWriter::Dump(const std::shared_ptr<file_system::FileWriter>& file) { assert(false); }

uint32_t DynamicPostingWriter::GetDumpLength() const { return 0; }

bool DynamicPostingWriter::CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid_t>* newOrder,
                                                      PostingWriter* output) const
{
    assert(false);
    return false;
}

size_t DynamicPostingWriter::GetEstimateDumpTempMemSize() const
{
    assert(false);
    return 0;
}

void DynamicPostingWriter::AddPosition(pos_t pos, pospayload_t posPayload, fieldid_t fieldId)
{
    _currentTF++;
    _totalTF++;
}

} // namespace indexlib::index
