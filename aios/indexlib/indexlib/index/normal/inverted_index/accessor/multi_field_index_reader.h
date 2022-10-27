#ifndef __INDEXLIB_MULTI_FIELD_INDEX_READER_H
#define __INDEXLIB_MULTI_FIELD_INDEX_READER_H

#include <tr1/unordered_map>
#include <string>
#include <tr1/memory>
#include <autil/AtomicCounter.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"

DECLARE_REFERENCE_CLASS(index, IndexAccessoryReader);
DECLARE_REFERENCE_CLASS(index, IndexMetrics);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

IE_NAMESPACE_BEGIN(index);

class MultiFieldIndexReader : public IndexReader
{
public:
    MultiFieldIndexReader(IndexMetrics* indexMetrics = NULL);
    MultiFieldIndexReader(const MultiFieldIndexReader& multiFieldIndexReader);
    ~MultiFieldIndexReader();

public:
    void AddIndexReader(const config::IndexConfigPtr& indexConf,
                        const IndexReaderPtr& reader);

    virtual const IndexReaderPtr& GetIndexReader(const std::string& field) const;

    const IndexReaderPtr& GetIndexReaderWithIndexId(indexid_t indexId) const;

    size_t GetIndexReaderCount() const {return mIndexReaders.size();}

    PostingIterator *Lookup(const common::Term& term,
            uint32_t statePoolSize = 1000,
            PostingType type = pt_default,
            autil::mem_pool::Pool *sessionPool = NULL) override;

    future_lite::Future<PostingIterator*> LookupAsync(const common::Term* term, uint32_t statePoolSize,
        PostingType type, autil::mem_pool::Pool* sessionPool) override;

    PostingIterator* PartialLookup(const common::Term& term, const DocIdRangeVector& ranges,
        uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool) override;

    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    void SetAccessoryReader(
            const IndexAccessoryReaderPtr& accessoryReader) override;

    std::string GetIndexName() const;

    KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override;

    
    bool GenFieldMapMask(const std::string &indexName,
                         const std::vector<std::string>& termFieldNames,
                         fieldmap_t &targetFieldMap) override;

    IndexReader* Clone() const override;

    void EnableAccessCountors() { mEnableAccessCountors = true; }

private:
    typedef std::vector<IndexReaderPtr> IndexReaderVec;
    typedef std::tr1::unordered_map<std::string, size_t> IndexName2PosMap;
    typedef std::tr1::unordered_map<indexid_t, size_t> IndexId2PosMap;

    IndexName2PosMap mName2PosMap;
    IndexId2PosMap mIndexId2PosMap;
    IndexReaderVec mIndexReaders;
    IndexAccessoryReaderPtr mAccessoryReader;
    mutable IndexMetrics* mIndexMetrics;
    mutable bool mEnableAccessCountors;

    static IndexReaderPtr RET_EMPTY_PTR;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<MultiFieldIndexReader> MultiFieldIndexReaderPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_FIELD_INDEX_READER_H
