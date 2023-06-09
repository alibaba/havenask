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
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_posting_writer.h"

#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/util/NumericUtil.h"

using namespace std;
using namespace fslib::fs;
using namespace autil::mem_pool;

using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DynamicIndexPostingWriter);

DynamicIndexPostingWriter::DynamicIndexPostingWriter(NodeManager* nodeManager, Stat* stat)
    : _dF(0)
    , _totalTF(0)
    , _termPayload(0)
    , _currentTF(0)
    , _tree(TREE_MAX_SLOT_SHIFT, nodeManager)
    , _stat(stat)
{
}

DynamicIndexPostingWriter::~DynamicIndexPostingWriter() {}

void DynamicIndexPostingWriter::EndDocument(docid_t docId, docpayload_t docPayload)
{
    _dF++;
    _currentTF = 0;
    _tree.Insert(docId);
    _lastDocId = docId;
    _stat->totalDocCount++;
}

void DynamicIndexPostingWriter::Update(docid_t docId, document::ModifiedTokens::Operation op)
{
    if (op == document::ModifiedTokens::Operation::ADD) {
        if (_tree.Insert(docId)) {
            _dF++;
            _stat->totalDocCount++;
        }
    } else if (op == document::ModifiedTokens::Operation::REMOVE) {
        if (_tree.Remove(docId)) {
            _dF--;
            _stat->totalDocCount--;
        }
    }
}

void DynamicIndexPostingWriter::Dump(const file_system::FileWriterPtr& file) { assert(false); }

uint32_t DynamicIndexPostingWriter::GetDumpLength() const { return 0; }
}} // namespace indexlib::index
