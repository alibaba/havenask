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

#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexSegmentReader.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingWriter.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::index {

// For offline build, DynamicMemIndexer is not used. Offline build will create operation logs and patches will be
// generated from operation logs during merge.
// TODO: Maybe patches should be generated directly during offline build.
class DynamicMemIndexer
{
    using PostingTable = util::HashMap<dictkey_t, PostingWriter*, autil::mem_pool::RecyclePool>;

public:
    static std::string
    GetDynamicTreeResourceFileName(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

public:
    struct DynamicPostingTable {
        autil::mem_pool::RecyclePool pool;
        PostingTable table;
        DynamicPostingWriter* nullTermPosting;
        DynamicPostingWriter::Stat stat;
        NodeManager nodeManager;

        DynamicPostingTable(size_t hashMapInitSize);
        ~DynamicPostingTable();
    };

public:
    DynamicMemIndexer(size_t hashMapInitSize, bool isNumberIndex);
    ~DynamicMemIndexer();

public:
    void Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    void UpdateTerm(docid_t docId, DictKeyInfo termKey, document::ModifiedTokens::Operation op);
    std::shared_ptr<DynamicIndexSegmentReader> CreateInMemReader();
    void Dump(const std::shared_ptr<file_system::Directory>& indexDir);

public:
    static std::string
    GetDynamicTreeFileName(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    static void UpdateToken(const DictKeyInfo& termKey, bool isNumberIndex, DynamicPostingTable* postingTable,
                            docid_t docId, document::ModifiedTokens::Operation op);
    static DynamicPostingWriter* CreatePostingWriter(DynamicPostingTable* postingTable);

public:
    DynamicPostingWriter* TEST_GetPostingWriter(uint64_t key);
    void TEST_AddToken(const DictKeyInfo& hashKey, pospayload_t posPayload);

private:
    DynamicPostingWriter* CreatePostingWriter();

private:
    size_t _hashMapInitSize;
    bool _isNumberIndex;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::unique_ptr<DynamicPostingTable> _postingTable;
    DynamicPostingTable* _postingTableAccessor = nullptr;
    std::shared_ptr<file_system::ResourceFile> _resource;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::index
