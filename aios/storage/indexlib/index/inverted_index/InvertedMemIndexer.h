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

#include "autil/Log.h"
#include "autil/SnapshotVector.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "autil/memory.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/inverted_index/IInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {
struct DocMapDumpParams;
class PatchFileInfo;
} // namespace indexlibv2::index

namespace indexlib::util {
class MMapAllocator;
} // namespace indexlib::util

namespace indexlib::document {
class IndexDocument;
class Field;
class Token;
} // namespace indexlib::document

namespace indexlib::config {
class HighFrequencyVocabulary;
}

namespace indexlib::index {
class BitmapIndexWriter;
class IndexSegmentReader;
class SectionAttributeMemIndexer;
class MultiShardInvertedMemIndexer;
class PostingWriter;
struct PostingWriterResource;
class IndexFormatOption;
class DynamicMemIndexer;
class InvertedIndexSegmentUpdater;

class InvertedMemIndexer : public IInvertedMemIndexer
{
public:
    using PostingTable = util::HashMap<dictkey_t, PostingWriter*>;
    using HashKeyVector = autil::SnapshotVector<dictkey_t, autil::mem_pool::pool_allocator<dictkey_t>>;
    using PostingPair = std::pair<dictkey_t, PostingWriter*>;
    using PostingVector = std::vector<PostingPair, autil::mem_pool::pool_allocator<PostingPair>>;
    static constexpr double HASHMAP_INIT_SIZE_FACTOR = 1.3;

    InvertedMemIndexer(const indexlibv2::index::IndexerParameter& indexerParam,
                       const std::shared_ptr<InvertedIndexMetrics>& metrics);
    ~InvertedMemIndexer();

    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(indexlibv2::document::IDocumentBatch* docBatch) override;
    Status Build(const indexlibv2::document::IIndexFields* indexFields, size_t n) override;
    Status AddDocument(document::IndexDocument* doc);
    Status Dump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<file_system::Directory>& indexDirectory,
                const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams) override;
    void ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(indexlibv2::document::IDocument* doc) override;
    bool IsValidField(const indexlibv2::document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override { return _indexConfig->GetIndexName(); }
    autil::StringView GetIndexType() const override { return INVERTED_INDEX_TYPE_STR; }
    void Seal() override;

    void UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    std::shared_ptr<InvertedIndexMetrics> GetMetrics() const override { return _metrics; }

public:
    void UpdateOneTerm(docid_t docId, const DictKeyInfo& dictKeyInfo, document::ModifiedTokens::Operation op);
    virtual Status AddField(const indexlib::document::Field* field);
    virtual void EndDocument(const indexlib::document::IndexDocument& indexDocument);
    virtual std::shared_ptr<IndexSegmentReader> CreateInMemReader();
    std::shared_ptr<indexlibv2::index::AttributeMemReader> CreateSectionAttributeMemReader() const override;

    Status AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos);
    BitmapIndexWriter* GetBitmapIndexWriter() const { return _bitmapIndexWriter; }

    bool IsDirty() const override;

public:
    SectionAttributeMemIndexer* TEST_GetSectionAttributeMemIndexer() const { return _sectionAttributeMemIndexer.get(); }
    indexlibv2::document::extractor::IDocumentInfoExtractor* GetDocInfoExtractor() const override;

protected:
    virtual Status DoDump(autil::mem_pool::PoolBase* dumpPool,
                          const std::shared_ptr<file_system::Directory>& indexDirectory,
                          const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams);

private:
    static constexpr size_t UNCOMPRESS_SHORT_LIST_MIN_LEN = MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1;
    static constexpr size_t UNCOMPRESS_SHORT_LIST_MAX_LEN = MAX_DOC_PER_RECORD - 1;
    static constexpr size_t UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR = BufferedSkipListWriter::ESTIMATE_INIT_MEMORY_USE;

    Status DocBatchToDocs(indexlibv2::document::IDocumentBatch* docBatch,
                          std::vector<document::IndexDocument*>* docs) const;
    Status DoAddNullToken(fieldid_t fieldId);

    void DoAddHashToken(DictKeyInfo termKey, const document::Token* token, fieldid_t fieldIdxInPack,
                        pos_t tokenBasePos);
    size_t CalPostingFileLength() const;
    size_t CalPostingLength(PostingWriter* postingWriter) const;
    Status DoDumpFiles(autil::mem_pool::PoolBase* dumpPool,
                       const std::shared_ptr<file_system::Directory>& outputDirectory,
                       const std::shared_ptr<file_system::Directory>& sectionAttributeOutputDirectory,
                       const std::shared_ptr<indexlibv2::index::DocMapDumpParams>& dumpParams);
    Status MergeModifiedTokens(const std::shared_ptr<file_system::Directory>& inputIndexDir,
                               const std::shared_ptr<indexlibv2::index::PatchFileInfo>& inputPatchFileInfo,
                               const std::shared_ptr<file_system::Directory>& outputIndexDir);
    std::pair<Status, dictvalue_t> DumpPosting(PostingWriter* writer,
                                               const std::shared_ptr<file_system::FileWriter>& fileWriter,
                                               const std::vector<docid_t>* old2NewDocId,
                                               PostingWriterResource* resource,
                                               InvertedIndexMetrics::SortDumpMetrics* metrics);
    std::pair<Status, dictvalue_t> DumpNormalPosting(PostingWriter* writer,
                                                     const std::shared_ptr<file_system::FileWriter>& fileWriter);
    void UpdateToCompressShortListCount(uint32_t df);
    const PostingWriter* GetPostingListWriter(const DictKeyInfo& key) const;
    uint32_t GetDistinctTermCount() const;
    bool NeedEstimateDumpTempMemSize() const;
    Status DoBuild(indexlibv2::document::IDocumentBatch* docBatch);
    void FlushBuffer();

protected:
    std::vector<fieldid_t> _fieldIds; // field ids related to this indexer
    pos_t _basePos = 0;
    indexlibv2::index::IndexerParameter _indexerParam;

private:
    util::SimplePool _simplePool;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<util::MMapAllocator> _allocator;
    std::shared_ptr<autil::mem_pool::Pool> _byteSlicePool;
    std::shared_ptr<autil::mem_pool::RecyclePool> _bufferPool;

    std::shared_ptr<IndexFormatOption> _indexFormatOption;
    std::shared_ptr<config::HighFrequencyVocabulary> _highFreqVol;

    PostingWriterResource* _postingWriterResource = nullptr;
    BitmapIndexWriter* _bitmapIndexWriter = nullptr;
    std::unique_ptr<indexlibv2::document::extractor::IDocumentInfoExtractor> _docInfoExtractor;
    PostingTable* _postingTable = nullptr;

    bool _indexSupportNull = false;
    size_t _hashMapInitSize;
    PostingWriter* _nullTermPosting = nullptr;
    bool _modifyNullTermPosting = false;
    PostingVector _modifiedPosting;
    HashKeyVector _hashKeyVector;
    std::unique_ptr<SectionAttributeMemIndexer> _sectionAttributeMemIndexer;

    std::unique_ptr<DynamicMemIndexer> _dynamicMemIndexer;
    std::unique_ptr<InvertedIndexSegmentUpdater> _invertedIndexSegmentUpdater;

    size_t _toCompressShortListCount = 0;
    std::shared_ptr<InvertedIndexMetrics> _metrics;
    size_t _estimateDumpTempMemSize = 0;
    bool _sealed = false;
    int32_t _docCount = 0;

    friend class MultiShardInvertedMemIndexer;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
