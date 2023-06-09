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

#include "autil/HashAlgorithm.h"
#include "autil/LongHashValue.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/Constant.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriterCreator.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::index {
template <typename Key>
class SinglePrimaryKeyBuilder;
}

namespace indexlibv2 { namespace index {

template <typename Key>
class PrimaryKeyWriter : public IMemIndexer
{
public:
    typedef indexlib::util::HashMap<Key, docid_t> HashMapTyped;
    typedef SingleValueAttributeMemIndexer<Key> PKAttributeWriterType;

    explicit PrimaryKeyWriter(indexlib::util::BuildResourceMetrics* buildResourceMetrics)
        : _docCount(0)
        , _buildResourceMetrics(buildResourceMetrics)
    {
    }
    ~PrimaryKeyWriter() = default;

    // TODO yije.zhang: when pk attribute writer mv to index, delete buildResourceMetrics
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool, const indexlib::file_system::DirectoryPtr& dir,
                const std::shared_ptr<framework::DumpParams>& dumpParams) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override
    {
        segmentMetrics->SetDistinctTermCount(_indexConfig->GetIndexName(), GetDistinctTermCount());
        if (_pkAttributeWriter) {
            _pkAttributeWriter->FillStatistics(segmentMetrics);
        }
    }
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::shared_ptr<const HashMapTyped> GetHashMap() const { return _hashMap; }
    std::shared_ptr<PKAttributeWriterType> GetPKAttributeMemIndexer() const { return _pkAttributeWriter; }

    std::string GetIndexName() const override { return _indexConfig->GetIndexName(); }
    autil::StringView GetIndexType() const override { return PRIMARY_KEY_INDEX_TYPE_STR; }
    void Seal() override {}
    bool IsDirty() const override { return _docCount > 0; }

private:
    Status AddDocument(indexlibv2::document::IDocument* doc);
    friend class indexlib::index::SinglePrimaryKeyBuilder<Key>;

private:
    Status DumpHashMap(const indexlib::file_system::FileWriterPtr& fileWriter,
                       const std::shared_ptr<HashMapTyped>& hashMap, autil::mem_pool::PoolBase* dumpPool,
                       const std::shared_ptr<framework::DumpParams>& dumpParams);
    uint32_t GetDistinctTermCount() const;
    bool CheckPrimaryKeyStr(const std::string& str) const;

    FieldType _fieldType;
    PrimaryKeyHashType _primaryKeyHashType;

    std::shared_ptr<HashMapTyped> _hashMap;
    std::shared_ptr<PKAttributeWriterType> _pkAttributeWriter;
    docid_t _docCount;
    indexlib::util::BuildResourceMetrics* _buildResourceMetrics;
    std::shared_ptr<config::IIndexConfig> _indexConfig;
    indexlib::util::MMapAllocatorPtr _allocator;
    autil::mem_pool::PoolPtr _byteSlicePool;
    indexlib::util::SimplePool _simplePool;

    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;
    std::shared_ptr<PrimaryKeyFileWriter<Key>> _primaryKeyFileWriter;
    friend class PrimaryKeyTypedTest;

    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyWriter, T);

template <typename Key>
inline bool PrimaryKeyWriter<Key>::CheckPrimaryKeyStr(const std::string& str) const
{
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> primaryKeyIndexConfig =
        std::static_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(_indexConfig);
    auto indexType = primaryKeyIndexConfig->GetInvertedIndexType();
    return indexlib::index::KeyHasherWrapper::IsOriginalKeyValid(_fieldType, _primaryKeyHashType, str.c_str(),
                                                                 str.size(), indexType == it_primarykey64);
}

template <typename Key>
uint32_t PrimaryKeyWriter<Key>::GetDistinctTermCount() const
{
    if (_hashMap) {
        return _hashMap->Size();
    }
    return 0;
}

template <typename Key>
Status PrimaryKeyWriter<Key>::Dump(autil::mem_pool::PoolBase* dumpPool,
                                   const indexlib::file_system::DirectoryPtr& directory,
                                   const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    indexlib::file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(_indexConfig->GetIndexName());
    indexlib::file_system::FileWriterPtr fileWriter =
        indexDirectory->CreateFileWriter(indexlib::index::PRIMARY_KEY_DATA_FILE_NAME);
    auto status = DumpHashMap(fileWriter, _hashMap, dumpPool, dumpParams);
    if (!status.IsOK()) {
        return status;
    }

    if (_pkAttributeWriter) {
        auto status = _pkAttributeWriter->Dump(dumpPool, indexDirectory, dumpParams);
        RETURN_IF_STATUS_ERROR(status, "dump pk attribute failed");
    }
    return Status::OK();
}

template <typename Key>
Status PrimaryKeyWriter<Key>::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> primaryKeyIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
    if (indexConfig && !primaryKeyIndexConfig) {
        INDEXLIB_FATAL_ERROR(BadParameter, "indexConfig must be PrimaryKeyIndexConfig");
    }
    assert(primaryKeyIndexConfig->GetInvertedIndexType() == it_primarykey64 ||
           primaryKeyIndexConfig->GetInvertedIndexType() == it_primarykey128);
    _primaryKeyFileWriter = PrimaryKeyFileWriterCreator<Key>::CreatePKFileWriter(primaryKeyIndexConfig);
    _indexConfig = indexConfig;
    _allocator.reset(new indexlib::util::MMapAllocator);
    _byteSlicePool.reset(new autil::mem_pool::Pool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
    auto fieldConfig = primaryKeyIndexConfig->GetFieldConfig();
    _fieldType = fieldConfig->GetFieldType();
    _primaryKeyHashType = primaryKeyIndexConfig->GetPrimaryKeyHashType();

    if (primaryKeyIndexConfig->HasPrimaryKeyAttribute()) {
        auto attributeConfig = primaryKeyIndexConfig->GetPKAttributeConfig();
        IndexerParameter params;
        AttributeIndexFactory factory;
        _pkAttributeWriter =
            std::dynamic_pointer_cast<PKAttributeWriterType>(factory.CreateMemIndexer(attributeConfig, params));
        auto status = _pkAttributeWriter->Init(attributeConfig, docInfoExtractorFactory);
        RETURN_IF_STATUS_ERROR(status, "init pk attribute mem indexer failed");
    }

    if (_byteSlicePool.get() != nullptr) {
        _hashMap.reset(new HashMapTyped(_byteSlicePool.get(), HASHMAP_INIT_SIZE));
    } else {
        _hashMap.reset(new HashMapTyped(HASHMAP_INIT_SIZE));
    }
    std::any emptyAny;
    _docCount = 0;
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        _indexConfig, document::extractor::DocumentInfoExtractorType::PRIMARY_KEY, emptyAny);
    return Status::OK();
}

template <typename Key>
Status PrimaryKeyWriter<Key>::DumpHashMap(const indexlib::file_system::FileWriterPtr& file,
                                          const std::shared_ptr<HashMapTyped>& hashMap,
                                          autil::mem_pool::PoolBase* dumpPool,
                                          const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    _primaryKeyFileWriter->Init(_docCount, hashMap->Size(), file, dumpPool);

    auto it = hashMap->CreateIterator();
    auto params = std::dynamic_pointer_cast<DocMapDumpParams>(dumpParams);
    if (params) {
        while (it.HasNext()) {
            auto p = it.Next();
            auto status = _primaryKeyFileWriter->AddPKPair(p.first, params->old2new[p.second]);
            if (!status.IsOK()) {
                return status;
            }
        }
    } else {
        while (it.HasNext()) {
            auto p = it.Next();
            auto status = _primaryKeyFileWriter->AddPKPair(p.first, p.second);
            if (!status.IsOK()) {
                return status;
            }
        }
    }
    return _primaryKeyFileWriter->Close();
}

template <typename Key>
void PrimaryKeyWriter<Key>::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t poolSize = _byteSlicePool->getUsedBytes() + _simplePool.getUsedBytes();
    int64_t dumpTempBufferSize = _primaryKeyFileWriter->EstimateDumpTempMemoryUse(_docCount);

    int64_t attrCurrentMemUse = 0, attrDumpTmpMemUse = 0, attrDumpExpandMemUse = 0, attrDumpFileSize = 0;
    if (_pkAttributeWriter) {
        _pkAttributeWriter->UpdateMemUse(memUpdater);
        memUpdater->GetMemInfo(attrCurrentMemUse, attrDumpTmpMemUse, attrDumpExpandMemUse, attrDumpFileSize);
    }
    memUpdater->UpdateCurrentMemUse(poolSize + attrCurrentMemUse);
    memUpdater->EstimateDumpTmpMemUse(std::max(dumpTempBufferSize, attrDumpTmpMemUse));
    // DumpExpandMemUse is 0
    // memUpdater->EstimateDumpExpandMemUse(0 + 0 /*attrDumpExpandMemUse*/);
    // pk dump process: 1. dump in-mem data to buffer  2. dump buffer to file
    // so, dump_file_size == dump_buffer_size
    memUpdater->EstimateDumpedFileSize(dumpTempBufferSize + attrDumpFileSize);
}
template <typename Key>
Status PrimaryKeyWriter<Key>::Build(const indexlibv2::document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
template <typename Key>
Status PrimaryKeyWriter<Key>::Build(indexlibv2::document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        // doc is dropped.
        if (docBatch->IsDropped(i)) {
            continue;
        }
        auto doc = (*docBatch)[i];
        auto docId = doc->GetDocId();
        if (doc->GetDocOperateType() != ADD_DOC) {
            AUTIL_LOG(DEBUG, "doc[%d] isn't add_doc", docId);
            continue;
        }

        RETURN_STATUS_DIRECTLY_IF_ERROR(AddDocument((*docBatch)[i].get()));
    }
    return Status::OK();
}

template <typename Key>
Status PrimaryKeyWriter<Key>::AddDocument(indexlibv2::document::IDocument* doc)
{
    std::string* keyStr;
    if (!_docInfoExtractor->ExtractField(doc, (void**)&keyStr).IsOK()) {
        AUTIL_LOG(ERROR, "Failed to extract primary key.");
        return Status::Unknown("Failed to extract primary key.");
    }
    auto docId = doc->GetDocId();
    Key primaryKey;
    bool ret = indexlib::index::KeyHasherWrapper::GetHashKey(_fieldType, _primaryKeyHashType, keyStr->c_str(),
                                                             keyStr->size(), primaryKey);
    assert(ret);
    (void)ret;

    _docCount++;
    if (_pkAttributeWriter) {
        _pkAttributeWriter->AddField(docId, primaryKey, /*isNull=*/false);
    }
    _hashMap->FindAndInsert(primaryKey, docId);
    return Status::OK();
}

template <typename Key>
void PrimaryKeyWriter<Key>::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            if (!_docInfoExtractor->IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}
template <typename Key>
bool PrimaryKeyWriter<Key>::IsValidDocument(document::IDocument* doc)
{
    return _docInfoExtractor->IsValidDocument(doc);
}

template <typename Key>
bool PrimaryKeyWriter<Key>::IsValidField(const document::IIndexFields* fields)
{
    assert(false);
    return false;
}

}} // namespace indexlibv2::index
