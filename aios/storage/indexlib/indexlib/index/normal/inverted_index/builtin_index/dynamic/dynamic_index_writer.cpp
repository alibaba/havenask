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
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"

#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DynamicIndexWriter);

DynamicIndexWriter::DynamicIndexWriter(segmentid_t segmentId, size_t hashMapInitSize, bool isNumberIndex,
                                       const config::IndexPartitionOptions& options)
    : _segmentId(segmentId)
    , _hashMapInitSize(hashMapInitSize)
    , _isNumberIndex(isNumberIndex)
    , _options(options)
    , _buildResourceMetricsNode(nullptr)
{
}

DynamicIndexWriter::~DynamicIndexWriter() {}

void DynamicIndexWriter::Init(const config::IndexConfigPtr& indexConfig,
                              util::BuildResourceMetrics* buildResourceMetrics)
{
    if (_options.IsOnline()) {
        _postingTable = std::make_unique<DynamicPostingTable>(_hashMapInitSize);
        if (buildResourceMetrics) {
            _buildResourceMetricsNode = buildResourceMetrics->AllocateNode();
            IE_LOG(INFO, "allocate build resource node [id:%d] for DynamicIndexWriter[%s]",
                   _buildResourceMetricsNode->GetNodeId(), indexConfig->GetIndexName().c_str());
        }
    } else {
        _updater = std::make_unique<PatchIndexSegmentUpdater>(buildResourceMetrics, _segmentId, indexConfig);
    }
}

void DynamicIndexWriter::UpdateToken(index::DictKeyInfo termKey, bool isNumberIndex, DynamicPostingTable* postingTable,
                                     docid_t docId, document::ModifiedTokens::Operation op)
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
            DynamicIndexPostingWriter* writer = CreatePostingWriter(postingTable);
            writer->Update(docId, op);
            postingTable->table.Insert(retrievalHashKey, writer);
        } else if ((*pWriter)->NotExistInCurrentDoc()) {
            static_cast<DynamicIndexPostingWriter*>(*pWriter)->Update(docId, op);
        }
    }
}

InMemDynamicIndexSegmentReaderPtr DynamicIndexWriter::CreateInMemReader()
{
    InMemDynamicIndexSegmentReaderPtr reader(
        new InMemDynamicIndexSegmentReader(&(_postingTable->table), _isNumberIndex, &(_postingTable->nullTermPosting)));
    return reader;
}

DynamicIndexPostingWriter* DynamicIndexWriter::CreatePostingWriter()
{
    return CreatePostingWriter(_postingTable.get());
}

DynamicIndexPostingWriter*
DynamicIndexWriter::CreatePostingWriter(DynamicIndexWriter::DynamicPostingTable* postingTable)
{
    autil::mem_pool::Pool* pool = &(postingTable->pool);
    void* ptr = pool->allocate(sizeof(DynamicIndexPostingWriter));
    return ::new (ptr) DynamicIndexPostingWriter(&(postingTable->nodeManager), &(postingTable->stat));
}

DynamicIndexPostingWriter* DynamicIndexWriter::TEST_GetPostingWriter(uint64_t key)
{
    dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, key);
    PostingWriter** pWriter = _postingTable->table.Find(retrievalHashKey);
    if (pWriter) {
        return static_cast<DynamicIndexPostingWriter*>(*pWriter);
    }
    return nullptr;
}

DynamicIndexWriter::DynamicPostingTable::DynamicPostingTable(size_t hashMapInitSize)
    : pool(autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE, autil::mem_pool::Pool::DEFAULT_ALIGN_SIZE)
    , table(&pool, hashMapInitSize)
    , nullTermPosting(nullptr)
    , nodeManager(&pool)
{
}
DynamicIndexWriter::DynamicPostingTable::~DynamicPostingTable()
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

void DynamicIndexWriter::UpdateTerm(docid_t docId, index::DictKeyInfo termKey, document::ModifiedTokens::Operation op)
{
    if (_updater) {
        _updater->Update(docId, termKey, op == document::ModifiedTokens::Operation::REMOVE);

    } else {
        DynamicIndexWriter::UpdateToken(termKey, _isNumberIndex, _postingTable.get(), docId, op);
    }
}

void DynamicIndexWriter::TEST_AddToken(const index::DictKeyInfo& hashKey, pospayload_t posPayload)
{
    if (hashKey.IsNull()) {
        // TODO: null token
        assert(false);
        return;
    }
    dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, hashKey.GetKey());
    PostingWriter** pWriter = _postingTable->table.Find(retrievalHashKey);
    if (pWriter == nullptr) {
        DynamicIndexPostingWriter* writer = CreatePostingWriter(_postingTable.get());
        _postingTable->table.Insert(retrievalHashKey, writer);
    } else {
        (*pWriter)->AddPosition(0, posPayload, 0);
    }
}

void DynamicIndexWriter::UpdateBuildResourceMetrics()
{
    if (_updater) {
        _updater->UpdateBuildResourceMetrics();
    } else {
        if (!_buildResourceMetricsNode or !_postingTable) {
            return;
        }
        int64_t poolSize = _postingTable->pool.getUsedBytes();
        _buildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
        _buildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, poolSize);
    }
}

void DynamicIndexWriter::Dump(const file_system::DirectoryPtr& fieldDir)
{
    if (_updater) {
        _updater->DumpToFieldDirectory(fieldDir, _segmentId);
    } else {
        // 实时 segment 在 dump 前后共享相同的内存空间，dump 只是简单地把 dynamic tree 暂托管给文件系统.
        // 因为 dump 前后内存不变，所以异步 dump 对实时 segment 的 redo operation 可以不做，其实由于 redo
        // 和 build 在不同线程，也不能对倒排的实时部分进行 redo.

        // 如果要改变实时 dump 的行为，需要同步修改异步 dump redo 的逻辑，不然可能会丢失文档.
        //   indexlib::partition::InplaceModifier::UpdateIndex(docid_t, const ModifiedTokens&)

        _resource = fieldDir->CreateResourceFile("dynamic_index.trees");
        assert(_resource);
        _resource->Reset(_postingTable.release());
        _resource->SetDirty(true);
    }
}
}} // namespace indexlib::index
