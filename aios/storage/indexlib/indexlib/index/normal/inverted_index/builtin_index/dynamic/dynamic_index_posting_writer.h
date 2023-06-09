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

#include "autil/mem_pool/PoolBase.h"
#include "fslib/fs/File.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTree.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/NodeManager.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DynamicIndexPostingWriter : public PostingWriter
{
private:
    static constexpr size_t TREE_MAX_SLOT_SHIFT = 4;

public:
    struct Stat {
        int64_t totalDocCount = 0;
    };

public:
    DynamicIndexPostingWriter(NodeManager* nodeManager, Stat* stat);
    ~DynamicIndexPostingWriter();

public:
    void AddPosition(pos_t pos, pospayload_t posPayload, fieldid_t fieldId) override;
    void EndDocument(docid_t docId, docpayload_t docPayload) override;
    void EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap) override;
    void EndSegment() override {}
    void Dump(const file_system::FileWriterPtr& file) override;
    uint32_t GetDumpLength() const override;
    uint32_t GetTotalTF() const override { return _totalTF; }
    uint32_t GetDF() const override { return _dF; }
    bool NotExistInCurrentDoc() const override { return _currentTF == 0; }
    void SetTermPayload(termpayload_t payload) override { _termPayload = payload; }
    termpayload_t GetTermPayload() const override { return _termPayload; }
    docid_t GetLastDocId() const override { return _lastDocId; }
    void SetCurrentTF(tf_t tf) override { _currentTF = tf; }
    bool CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid_t>* newOrder,
                                    PostingWriter* output) const override
    {
        assert(false);
        return false;
    }

    size_t GetEstimateDumpTempMemSize() const override
    {
        assert(false);
        return 0;
    }

public:
    void AddPosition(pos_t pos, pospayload_t posPayload) { AddPosition(pos, posPayload, 0); }
    void Update(docid_t docId, document::ModifiedTokens::Operation op);
    const DynamicSearchTree* GetPostingTree() const { return &_tree; }

public:
    DynamicSearchTree* TEST_GetPostingTree() { return &_tree; }

private:
    uint32_t _dF;
    uint32_t _totalTF;
    termpayload_t _termPayload;
    docid_t _lastDocId;
    tf_t _currentTF;
    DynamicSearchTree _tree;
    Stat* _stat;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////

inline void DynamicIndexPostingWriter::AddPosition(pos_t pos, pospayload_t posPayload, fieldid_t fieldId)
{
    _currentTF++;
    _totalTF++;
}
inline void DynamicIndexPostingWriter::EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap)
{
    return EndDocument(docId, docPayload);
}

typedef std::shared_ptr<DynamicIndexPostingWriter> DynamicIndexPostingWriterPtr;
}} // namespace indexlib::index
