#ifndef __INDEXLIB_NORMAL_INDEX_WRITER_H
#define __INDEXLIB_NORMAL_INDEX_WRITER_H

#include <map>
#include <vector>
#include <tr1/memory>
#include <autil/mem_pool/pool_allocator.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(index);

class NormalIndexWriter : public index::IndexWriter
{
public:
    typedef std::tr1::shared_ptr<autil::mem_pool::RecyclePool> RecyclePoolPtr;
    static constexpr double HASHMAP_INIT_SIZE_FACTOR = 1.3;
    
public:
    NormalIndexWriter(size_t lastSegmentDistinctTermCount,
                      const config::IndexPartitionOptions& options); 
    
    virtual ~NormalIndexWriter();

public:
    void Init(const config::IndexConfigPtr& indexConfig,
              util::BuildResourceMetrics* buildResourceMetrics,
              const index_base::PartitionSegmentIteratorPtr& segIter =
              index_base::PartitionSegmentIteratorPtr()) override;
    
    size_t EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionSegmentIteratorPtr& segIter =
                              index_base::PartitionSegmentIteratorPtr()) override;

    void AddField(const document::Field* field) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;

    void EndSegment() override;

    void Dump(const file_system::DirectoryPtr& dir,
              autil::mem_pool::PoolBase* dumpPool) override;

    uint64_t GetNormalTermDfSum() const override;

    IndexSegmentReaderPtr CreateInMemReader() override;

public:
    // for multi sharding index writer
    void AddToken(const document::Token* token, fieldid_t fieldId, 
                  pos_t tokenBasePos);

private:
    uint32_t GetDistinctTermCount() const override;
    void UpdateBuildResourceMetrics() override;
        
    void InitMemoryPool() override;
    
    void DumpPosting(const index::DictionaryWriterPtr& dictWriter,
                     index::PostingWriter* writer, dictkey_t key);
    bool GetDictInlinePostingValue(
            const index::PostingWriter* writer,
            uint64_t& inlinePostingValue);

    void DumpNormalPosting(const index::DictionaryWriterPtr& dictWriter,
                           index::PostingWriter* writer, dictkey_t key);
    void DumpDictInlinePosting(const index::DictionaryWriterPtr& dictWriter,
                               uint64_t postingValue, dictkey_t key);

    size_t CalPostingFileLength() const;

    void DoAddHashToken(const dictkey_t hashKey,
                        const document::Token* token,
                        fieldid_t fieldId, pos_t tokenBasePos);

    void PrintIndexDocument(const document::IndexDocument& indexDocument);

// for test
public:
    void SetVocabulary(const config::HighFrequencyVocabularyPtr& vol)
    { mHighFreqVol = vol;}

    index::BitmapPostingWriter* GetBitmapPostingWriter(dictkey_t key);
    const index::PostingWriter* GetPostingListWriter(dictkey_t key) const;
    void SetPostingListWriter(dictkey_t hashKey, index::PostingWriter* postingWriter);

protected:
    index::PostingWriter* CreatePostingWriter();

public:
    typedef std::vector<dictkey_t, autil::mem_pool::pool_allocator<dictkey_t> > HashKeyVector;
    typedef std::pair<dictkey_t, index::PostingWriter*> PostingPair;
    typedef std::vector<PostingPair, autil::mem_pool::pool_allocator<PostingPair> > PostingVector;
    typedef util::HashMap<dictkey_t, index::PostingWriter*> PostingTable;

private:
    static size_t UNCOMPRESS_SHORT_LIST_MIN_LEN;
    static size_t UNCOMPRESS_SHORT_LIST_MAX_LEN;
    static size_t UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR; 

protected:
    IndexFormatOptionPtr mIndexFormatOption;
    pos_t mBasePos;
    
    RecyclePoolPtr mBufferPool;
    PostingTable* mPostingTable;
    PostingVector mModifiedPosting;
    HashKeyVector mHashKeyVector;

    file_system::FileWriterPtr mPostingFile;

    index::SectionAttributeWriterPtr mSectionAttributeWriter;
    index::BitmapIndexWriter* mBitmapIndexWriter;
    config::HighFrequencyVocabularyPtr mHighFreqVol;
    IndexFormatWriterCreator* mWriterCreator;
    index::PostingWriterResource*  mPostingWriterResource;    
    config::IndexPartitionOptions mOptions;
    size_t mToCompressShortListCount;
    size_t mHashMapInitSize;
    
private:
    friend class NormalIndexWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMAL_INDEX_WRITER_H
