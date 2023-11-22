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

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/inverted_index/InvertedIndexFields.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::document {
class IndexDocument;
}

namespace indexlib::index {

class BitmapIndexWriter
{
public:
    using HashKeyVector = std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>>;
    using PostingPair = std::pair<uint64_t, BitmapPostingWriter*>;
    using BitmapPostingVector = std::vector<PostingPair, autil::mem_pool::pool_allocator<PostingPair>>;
    using BitmapPostingTable = util::HashMap<uint64_t, BitmapPostingWriter*>;

    BitmapIndexWriter(autil::mem_pool::Pool* byteSlicePool, util::SimplePool* simplePool, bool isNumberIndex,
                      bool isRealTime = false);
    ~BitmapIndexWriter();

    void AddToken(const index::DictKeyInfo& hashKey, pospayload_t posPayload);
    void EndDocument(const document::IndexDocument& indexDocument);
    void EndDocument(const indexlibv2::index::InvertedIndexFields* indexFields, docid64_t docId);
    void UpdateTerm(docid_t docId, index::DictKeyInfo termKey, document::ModifiedTokens::Operation op);
    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool,
              const std::vector<docid_t>* old2NewDocId);

    std::shared_ptr<InMemBitmapIndexSegmentReader> CreateInMemReader();
    uint32_t GetDistinctTermCount() const
    {
        return _nullTermPostingWriter != nullptr ? _bitmapPostingTable.Size() + 1 : _bitmapPostingTable.Size();
    }
    size_t GetEstimateDumpTempMemSize() const { return _estimateDumpTempMemSize; }

public:
    // for test
    BitmapPostingWriter* GetBitmapPostingWriter(const index::DictKeyInfo& key);

private:
    BitmapPostingWriter* CreatePostingWriter();

    void DumpPosting(BitmapPostingWriter* writer, const index::DictKeyInfo& key,
                     const file_system::FileWriterPtr& postingFile, TieredDictionaryWriter<dictkey_t>& dictionaryWriter,
                     const std::vector<docid_t>* old2NewDocId, autil::mem_pool::PoolBase* dumpPool);

    size_t GetDumpPostingFileLength() const;
    void DoAddNullToken(pospayload_t posPayload);

private:
    autil::mem_pool::Pool* _byteSlicePool;
    util::SimplePool* _simplePool;
    BitmapPostingTable _bitmapPostingTable;
    BitmapPostingWriter* _nullTermPostingWriter = nullptr;
    HashKeyVector _hashKeyVector;
    BitmapPostingVector _modifiedPosting;
    bool _modifyNullTermPosting = false;
    bool _isRealTime;
    bool _isNumberIndex;
    size_t _estimateDumpTempMemSize = 0;

private:
    AUTIL_LOG_DECLARE();
};

inline BitmapPostingWriter* BitmapIndexWriter::GetBitmapPostingWriter(const index::DictKeyInfo& key)
{
    if (key.IsNull()) {
        return _nullTermPostingWriter;
    }

    BitmapPostingWriter** writer;
    writer = _bitmapPostingTable.Find(InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, key.GetKey()));
    if (writer != nullptr) {
        return *writer;
    }
    return nullptr;
}

} // namespace indexlib::index
