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

#include <map>
#include <memory>
#include <vector>

#include "autil/SnapshotVector.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_writer.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/util/HashMap.h"

DECLARE_REFERENCE_CLASS(index, BitmapIndexWriter);
DECLARE_REFERENCE_CLASS(index, DynamicIndexWriter);

namespace indexlib { namespace index {
class BitmapPostingWriter;

class NormalIndexWriter : public index::IndexWriter
{
public:
    typedef std::shared_ptr<autil::mem_pool::RecyclePool> RecyclePoolPtr;
    static constexpr double HASHMAP_INIT_SIZE_FACTOR = 1.3;

public:
    NormalIndexWriter(segmentid_t segmentId, size_t lastSegmentDistinctTermCount,
                      const config::IndexPartitionOptions& options);
    NormalIndexWriter(size_t lastSegmentDistinctTermCount, const config::IndexPartitionOptions& options)
        : NormalIndexWriter(INVALID_SEGMENTID, lastSegmentDistinctTermCount, options)
    {
    }

    virtual ~NormalIndexWriter();

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;

    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override;

    void UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override;
    void AddField(const document::Field* field) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;

    void EndSegment() override;

    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;

    uint64_t GetNormalTermDfSum() const override;

    IndexSegmentReaderPtr CreateInMemReader() override;

    void SetTemperatureLayer(const std::string& layer) override;

public:
    // for multi sharding index writer
    void AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos);

public:
    typedef autil::SnapshotVector<dictkey_t, autil::mem_pool::pool_allocator<dictkey_t>> HashKeyVector;
    typedef std::pair<dictkey_t, PostingWriter*> PostingPair;
    typedef std::vector<PostingPair, autil::mem_pool::pool_allocator<PostingPair>> PostingVector;
    typedef util::HashMap<dictkey_t, PostingWriter*> PostingTable;

private:
    void UpdateToCompressShortListCount(uint32_t df);

    uint32_t GetDistinctTermCount() const override;

    void InitMemoryPool() override;

    void DumpPosting(const std::shared_ptr<DictionaryWriter>& dictWriter, PostingWriter* writer,
                     const index::DictKeyInfo& key);

    void UpdateTerm(docid_t docId, index::DictKeyInfo termKey, document::ModifiedTokens::Operation op);
    size_t CalPostingLength(PostingWriter* postingWriter) const;
    dictvalue_t DumpPosting(PostingWriter* writer);
    dictvalue_t DumpNormalPosting(PostingWriter* writer);
    bool GetDictInlinePostingValue(const PostingWriter* writer, uint64_t& inlinePostingValue);
    size_t CalPostingFileLength() const;
    void DoAddHashToken(index::DictKeyInfo termKey, const document::Token* token, fieldid_t fieldIdxInPack,
                        pos_t tokenBasePos);
    void DoAddNullToken(fieldid_t fieldId);
    void PrintIndexDocument(const document::IndexDocument& indexDocument);

protected:
    void UpdateBuildResourceMetrics() override;

public: // for test
    void TEST_SetVocabulary(const std::shared_ptr<config::HighFrequencyVocabulary>& vol) { mHighFreqVol = vol; }
    size_t GetHashMapInitSize() const { return mHashMapInitSize; }

    BitmapPostingWriter* GetBitmapPostingWriter(const index::DictKeyInfo& key);
    const PostingWriter* GetPostingListWriter(const index::DictKeyInfo& key) const;
    void SetPostingListWriter(const index::DictKeyInfo& hashKey, PostingWriter* postingWriter);

private:
protected:
    static size_t UNCOMPRESS_SHORT_LIST_MIN_LEN;
    static size_t UNCOMPRESS_SHORT_LIST_MAX_LEN;
    static size_t UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR;

protected:
    segmentid_t mSegmentId;
    std::shared_ptr<LegacyIndexFormatOption> mIndexFormatOption;
    pos_t mBasePos;

    util::SimplePool mSimplePool;
    RecyclePoolPtr mBufferPool;
    PostingTable* mPostingTable;
    bool mModifyNullTermPosting;
    PostingVector mModifiedPosting;
    HashKeyVector mHashKeyVector;
    PostingWriter* mNullTermPosting;

    file_system::FileWriterPtr mPostingFile;

    std::unique_ptr<index::SectionAttributeWriter> mSectionAttributeWriter;
    index::BitmapIndexWriter* mBitmapIndexWriter;
    std::shared_ptr<config::HighFrequencyVocabulary> mHighFreqVol;

    index::DynamicIndexWriter* mDynamicIndexWriter;

    PostingWriterResource* mPostingWriterResource;
    config::IndexPartitionOptions mOptions;
    size_t mToCompressShortListCount;
    size_t mHashMapInitSize;
    bool mIndexSupportNull;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexWriter);
}} // namespace indexlib::index
