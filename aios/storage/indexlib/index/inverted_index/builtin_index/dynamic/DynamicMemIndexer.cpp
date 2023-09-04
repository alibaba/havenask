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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
namespace {
using document::ModifiedTokens;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DynamicMemIndexer);

std::string
DynamicMemIndexer::GetDynamicTreeFileName(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    return indexConfig->GetIndexName() + "_@_dynamic_index.trees";
}

DynamicMemIndexer::DynamicMemIndexer(size_t hashMapInitSize, bool isNumberIndex)
    : _hashMapInitSize(hashMapInitSize)
    , _isNumberIndex(isNumberIndex)
{
}

DynamicMemIndexer::~DynamicMemIndexer() {}

void DynamicMemIndexer::Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    _indexConfig = indexConfig;
    _postingTable = std::make_unique<DynamicPostingTable>(_hashMapInitSize);
    _postingTableAccessor = _postingTable.get();
}

void DynamicMemIndexer::UpdateToken(const DictKeyInfo& termKey, bool isNumberIndex, DynamicPostingTable* postingTable,
                                    docid_t docId, ModifiedTokens::Operation op)
{
    if (termKey.IsNull()) {
        if (!postingTable->nullTermPosting) {
            postingTable->nullTermPosting = CreatePostingWriter(postingTable);
        }
        postingTable->nullTermPosting->Update(docId, op);
    } else {
        auto retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(isNumberIndex, termKey.GetKey());
        PostingWriter** pWriter = postingTable->table.Find(retrievalHashKey);
        if (pWriter == nullptr) {
            DynamicPostingWriter* writer = CreatePostingWriter(postingTable);
            writer->Update(docId, op);
            postingTable->table.Insert(retrievalHashKey, writer);
        } else if ((*pWriter)->NotExistInCurrentDoc()) {
            static_cast<DynamicPostingWriter*>(*pWriter)->Update(docId, op);
        }
    }
}

std::shared_ptr<DynamicIndexSegmentReader> DynamicMemIndexer::CreateInMemReader()
{
    return std::make_shared<DynamicIndexSegmentReader>(&(_postingTableAccessor->table), _isNumberIndex,
                                                       &(_postingTableAccessor->nullTermPosting));
}

DynamicPostingWriter* DynamicMemIndexer::CreatePostingWriter() { return CreatePostingWriter(_postingTableAccessor); }

DynamicPostingWriter* DynamicMemIndexer::CreatePostingWriter(DynamicMemIndexer::DynamicPostingTable* postingTable)
{
    autil::mem_pool::Pool* pool = &(postingTable->pool);
    void* ptr = pool->allocate(sizeof(DynamicPostingWriter));
    return ::new (ptr) DynamicPostingWriter(&(postingTable->nodeManager), &(postingTable->stat));
}

DynamicPostingWriter* DynamicMemIndexer::TEST_GetPostingWriter(uint64_t key)
{
    dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, key);
    PostingWriter** pWriter = _postingTable->table.Find(retrievalHashKey);
    if (pWriter) {
        return static_cast<DynamicPostingWriter*>(*pWriter);
    }
    return nullptr;
}

DynamicMemIndexer::DynamicPostingTable::DynamicPostingTable(size_t hashMapInitSize)
    : pool(autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE, autil::mem_pool::Pool::DEFAULT_ALIGN_SIZE)
    , table(&pool, hashMapInitSize)
    , nullTermPosting(nullptr)
    , nodeManager(&pool)
{
}
DynamicMemIndexer::DynamicPostingTable::~DynamicPostingTable()
{
    auto it = table.CreateIterator();
    while (it.HasNext()) {
        auto& kv = it.Next();
        PostingWriter* pWriter = kv.second;
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, pWriter);
    }
    table.Clear();
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, nullTermPosting);
    nodeManager.Clear();
}

void DynamicMemIndexer::UpdateTerm(docid_t docId, DictKeyInfo termKey, ModifiedTokens::Operation op)
{
    // 使用裸指针访问，因为 unique_ptr 在 dump 时会 release() 给 ResourceFile 管理其生命周期
    DynamicMemIndexer::UpdateToken(termKey, _isNumberIndex, _postingTableAccessor, docId, op);
}

void DynamicMemIndexer::TEST_AddToken(const DictKeyInfo& hashKey, pospayload_t posPayload)
{
    if (hashKey.IsNull()) {
        // TODO: null token
        assert(false);
        return;
    }
    dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, hashKey.GetKey());
    PostingWriter** pWriter = _postingTable->table.Find(retrievalHashKey);
    if (pWriter == nullptr) {
        DynamicPostingWriter* writer = CreatePostingWriter(_postingTable.get());
        _postingTable->table.Insert(retrievalHashKey, writer);
    } else {
        (*pWriter)->AddPosition(0, posPayload, 0);
    }
}

void DynamicMemIndexer::Dump(const std::shared_ptr<file_system::Directory>& indexDir)
{
    // 实时 segment 在 dump 前后共享相同的内存空间，dump 只是简单地把 dynamic tree 暂托管给文件系统.
    // 因为 dump 前后内存不变，所以异步 dump 对实时 segment 的 redo operation 可以不做，其实由于 redo
    // 和 build 在不同线程，也不能对倒排的实时部分进行 redo.

    // 如果要改变实时 dump 的行为，需要同步修改异步 dump redo 的逻辑，不然可能会丢失文档.
    //   indexlib::partition::InplaceModifier::UpdateIndex(docid_t, const ModifiedTokens&)

    std::string resourceFileName = DynamicMemIndexer::GetDynamicTreeFileName(_indexConfig);
    _resource = indexDir->CreateResourceFile(resourceFileName);
    assert(_resource);
    _resource->Reset(_postingTable.release());
    _resource->SetDirty(true);
}

size_t DynamicMemIndexer::GetCurrentMemoryUse() { return _postingTableAccessor->pool.getUsedBytes(); }

} // namespace indexlib::index
