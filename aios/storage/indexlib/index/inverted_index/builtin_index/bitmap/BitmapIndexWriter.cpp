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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"

#include "autil/memory.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
namespace {
using autil::mem_pool::Pool;
using autil::mem_pool::PoolBase;
} // namespace
AUTIL_LOG_SETUP(indexlib.index, BitmapIndexWriter);

BitmapIndexWriter::BitmapIndexWriter(Pool* byteSlicePool, util::SimplePool* simplePool, bool isNumberIndex,
                                     bool isRealTime)
    : _byteSlicePool(byteSlicePool)
    , _simplePool(simplePool)
    , _bitmapPostingTable(byteSlicePool, HASHMAP_INIT_SIZE)
    , _hashKeyVector(autil::mem_pool::pool_allocator<uint64_t>(simplePool))
    , _modifiedPosting(autil::mem_pool::pool_allocator<PostingPair>(simplePool))
    , _isRealTime(isRealTime)
    , _isNumberIndex(isNumberIndex)
{
}

BitmapIndexWriter::~BitmapIndexWriter()
{
    BitmapPostingTable::Iterator it = _bitmapPostingTable.CreateIterator();
    while (it.HasNext()) {
        BitmapPostingTable::KeyValuePair& kv = it.Next();
        BitmapPostingWriter* pWriter = kv.second;
        IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, pWriter);
    }
    _bitmapPostingTable.Clear();
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, _nullTermPostingWriter);
}

void BitmapIndexWriter::AddToken(const index::DictKeyInfo& hashKey, pospayload_t posPayload)
{
    if (hashKey.IsNull()) {
        DoAddNullToken(posPayload);
        return;
    }

    dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, hashKey.GetKey());
    BitmapPostingWriter** pWriter = _bitmapPostingTable.Find(retrievalHashKey);
    if (pWriter == nullptr) {
        BitmapPostingWriter* writer = CreatePostingWriter();
        // pos doesn't make sense for BitmapPostingWriter
        writer->AddPosition(0, posPayload);
        _bitmapPostingTable.Insert(retrievalHashKey, writer);
        _hashKeyVector.push_back(hashKey.GetKey());
        _modifiedPosting.push_back(std::make_pair(hashKey.GetKey(), writer));
    } else {
        if ((*pWriter)->NotExistInCurrentDoc()) {
            _modifiedPosting.push_back(std::make_pair(hashKey.GetKey(), *pWriter));
        }
        // pos doesn't make sense for BitmapPostingWriter
        (*pWriter)->AddPosition(0, posPayload);
    }
}

void BitmapIndexWriter::UpdateTerm(docid_t docId, index::DictKeyInfo termKey, document::ModifiedTokens::Operation op)
{
    BitmapPostingWriter* writer = nullptr;
    if (termKey.IsNull()) {
        if (!_nullTermPostingWriter and op == document::ModifiedTokens::Operation::ADD) {
            _nullTermPostingWriter = CreatePostingWriter();
        }
        writer = _nullTermPostingWriter;
    } else {
        dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, termKey.GetKey());
        BitmapPostingWriter** pWriter = _bitmapPostingTable.Find(retrievalHashKey);
        if (pWriter) {
            writer = *pWriter;
        } else if (op == document::ModifiedTokens::Operation::ADD) {
            writer = CreatePostingWriter();
            _bitmapPostingTable.Insert(retrievalHashKey, writer);
            _hashKeyVector.push_back(termKey.GetKey());
        }
    }
    if (writer) {
        assert(op != document::ModifiedTokens::Operation::NONE);
        writer->Update(docId, op == document::ModifiedTokens::Operation::REMOVE);
    }
}

void BitmapIndexWriter::EndDocument(const document::IndexDocument& indexDocument)
{
    for (BitmapPostingVector::iterator it = _modifiedPosting.begin(); it != _modifiedPosting.end(); it++) {
        termpayload_t termpayload = indexDocument.GetTermPayload(it->first);
        it->second->SetTermPayload(termpayload);
        it->second->EndDocument(indexDocument.GetDocId(), indexDocument.GetDocPayload(it->first));
        _estimateDumpTempMemSize = std::max(_estimateDumpTempMemSize, it->second->GetEstimateDumpTempMemSize());
    }

    if (_modifyNullTermPosting) {
        assert(_nullTermPostingWriter);
        _nullTermPostingWriter->EndDocument(indexDocument.GetDocId(), 0);
        _estimateDumpTempMemSize =
            std::max(_estimateDumpTempMemSize, _nullTermPostingWriter->GetEstimateDumpTempMemSize());
    }
    _modifiedPosting.clear();
    _modifyNullTermPosting = false;
}

void BitmapIndexWriter::Dump(const file_system::DirectoryPtr& dir, PoolBase* dumpPool,
                             const std::vector<docid_t>* old2NewDocId)
{
    if (_hashKeyVector.size() == 0 && _nullTermPostingWriter == nullptr) {
        return;
    }

    TieredDictionaryWriter<dictkey_t> dictionaryWriter(dumpPool);
    dictionaryWriter.Open(dir, BITMAP_DICTIONARY_FILE_NAME, _hashKeyVector.size());

    file_system::FileWriterPtr postingFile = dir->CreateFileWriter(BITMAP_POSTING_FILE_NAME);

    size_t postingFileLen = GetDumpPostingFileLength();
    postingFile->ReserveFile(postingFileLen).GetOrThrow();

    sort(_hashKeyVector.begin(), _hashKeyVector.end());
    for (HashKeyVector::const_iterator it = _hashKeyVector.begin(); it != _hashKeyVector.end(); ++it) {
        BitmapPostingWriter** pWriter =
            _bitmapPostingTable.Find(InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, *it));
        assert(pWriter != nullptr);
        DumpPosting(*pWriter, index::DictKeyInfo(*it), postingFile, dictionaryWriter, old2NewDocId, dumpPool);
        if (old2NewDocId) {
            // only sort dump give a session pool (temp pool) to enable reset
            dumpPool->reset();
        }
    }

    if (_nullTermPostingWriter) {
        DumpPosting(_nullTermPostingWriter, index::DictKeyInfo::NULL_TERM, postingFile, dictionaryWriter, old2NewDocId,
                    dumpPool);
        if (old2NewDocId) {
            dumpPool->reset();
        }
    }
    assert(old2NewDocId or postingFile->GetLogicLength() == postingFileLen);
    dictionaryWriter.Close();
    postingFile->Close().GetOrThrow();
}

void BitmapIndexWriter::DumpPosting(BitmapPostingWriter* writer, const index::DictKeyInfo& key,
                                    const file_system::FileWriterPtr& postingFile,
                                    TieredDictionaryWriter<dictkey_t>& dictionaryWriter,
                                    const std::vector<docid_t>* old2NewDocId, PoolBase* dumpPool)
{
    uint64_t offset = postingFile->GetLogicLength();
    dictionaryWriter.AddItem(key, offset);

    std::shared_ptr<PostingWriter> scopedDumpWriter;
    PostingWriter* dumpWriter = writer;
    if (old2NewDocId) {
        Pool* pool = dynamic_cast<Pool*>(dumpPool);
        assert(pool);
        BitmapPostingWriter* reorderWriter = POOL_COMPATIBLE_NEW_CLASS(pool, BitmapPostingWriter, pool);
        scopedDumpWriter = autil::shared_ptr_pool(pool, reorderWriter);
        [[maybe_unused]] bool ret = writer->CreateReorderPostingWriter(pool, old2NewDocId, reorderWriter);
        assert(ret);
        dumpWriter = reorderWriter;
    }
    uint32_t postingLen = dumpWriter->GetDumpLength();
    postingFile->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
    dumpWriter->Dump(postingFile);
}

std::shared_ptr<InMemBitmapIndexSegmentReader> BitmapIndexWriter::CreateInMemReader()
{
    std::shared_ptr<InMemBitmapIndexSegmentReader> reader(
        new InMemBitmapIndexSegmentReader(&_bitmapPostingTable, _isNumberIndex, &_nullTermPostingWriter));
    return reader;
}

size_t BitmapIndexWriter::GetDumpPostingFileLength() const
{
    size_t totalLength = 0;
    BitmapPostingTable::Iterator iter = _bitmapPostingTable.CreateIterator();
    while (iter.HasNext()) {
        BitmapPostingTable::KeyValuePair p = iter.Next();
        totalLength += sizeof(uint32_t); // postingLen
        totalLength += p.second->GetDumpLength();
    }

    if (_nullTermPostingWriter) {
        totalLength += sizeof(uint32_t); // postingLen
        totalLength += _nullTermPostingWriter->GetDumpLength();
    }
    return totalLength;
}

BitmapPostingWriter* BitmapIndexWriter::CreatePostingWriter()
{
    void* ptr = _byteSlicePool->allocate(sizeof(BitmapPostingWriter));
    PoolBase* pool = _isRealTime ? (PoolBase*)_byteSlicePool : (PoolBase*)_simplePool;
    return new (ptr) BitmapPostingWriter(pool);
}

void BitmapIndexWriter::DoAddNullToken(pospayload_t posPayload)
{
    if (_nullTermPostingWriter == nullptr) {
        _nullTermPostingWriter = CreatePostingWriter();
    }

    if (_nullTermPostingWriter->NotExistInCurrentDoc()) {
        _modifyNullTermPosting = true;
    }
    _nullTermPostingWriter->AddPosition(0, posPayload);
}
} // namespace indexlib::index
