#ifndef ISEARCH_INDEXPARTITIONREADERWRAPPER_H
#define ISEARCH_INDEXPARTITIONREADERWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Term.h>
#include <ha3/search/AttributeMetaInfo.h>
#include <indexlib/partition/index_partition_reader.h>
#include <indexlib/index/partition_info.h>
#include <indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h>
#include <indexlib/index/normal/inverted_index/accessor/posting_iterator.h>
#include <indexlib/partition/partition_reader_snapshot.h>
#include <ha3/search/LayerMetas.h>

BEGIN_HA3_NAMESPACE(search);

struct LookupResult {
    LookupResult()
        : postingIt(NULL)
        , main2SubIt(NULL)
        , sub2MainIt(NULL)
        , isSubPartition(false)
    {}

    IE_NAMESPACE(index)::PostingIterator *postingIt;
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *main2SubIt;
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *sub2MainIt;
    bool isSubPartition;
};

class IndexPartitionReaderWrapper
{
public:
    static const uint32_t MAIN_PART_ID;
    static const uint32_t SUB_PART_ID;
    static const uint32_t JOIN_PART_START_ID;
public:
    typedef IE_NAMESPACE(index)::JoinDocidAttributeIterator DocMapAttrIterator;
    typedef IE_NAMESPACE(index)::PostingIterator PostingIterator;
    typedef std::tr1::unordered_map<std::string, LookupResult> PostingCache;
    typedef std::vector<IE_NAMESPACE(index)::PostingIterator*> DelPostingVector;
    typedef IE_NAMESPACE(index)::IndexReaderPtr IndexReaderPtr;
    typedef IE_NAMESPACE(partition)::IndexPartitionReaderPtr IndexPartitionReaderPtr;
    typedef IE_NAMESPACE(index)::PartitionInfoPtr PartitionInfoPtr;
public:  
    IndexPartitionReaderWrapper(
            const std::map<std::string, uint32_t>* indexName2IdMap, 
            const std::map<std::string, uint32_t>* attrName2IdMap, 
            const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs);

    IndexPartitionReaderWrapper(
            std::map<std::string, uint32_t>& indexName2IdMapSwap, 
            std::map<std::string, uint32_t>& attrName2IdMapSwap, 
            const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs); // for agg op
    virtual ~IndexPartitionReaderWrapper();
private:
    IndexPartitionReaderWrapper(const IndexPartitionReaderWrapper&);
    IndexPartitionReaderWrapper& operator = (const IndexPartitionReaderWrapper&);
public:
    LookupResult lookup(const common::Term &term,
                        const LayerMeta *layerMeta);
    const IndexPartitionReaderPtr &getReader() const {
        assert(_indexPartReaderPtrs.size() > 0);
        return _indexPartReaderPtrs[MAIN_PART_ID];
    }
    const IndexPartitionReaderPtr &getSubReader() const {
        assert(_indexPartReaderPtrs.size() > 0);
        if (_indexPartReaderPtrs.size() > SUB_PART_ID) {
            return _indexPartReaderPtrs[SUB_PART_ID];
        }
        static IndexPartitionReaderPtr EMPTY;
        return EMPTY;
    }
    const PartitionInfoPtr &getPartitionInfo() const {
        return _partitionInfoPtr;
    }
    const PartitionInfoPtr &getSubPartitionInfo() const {
        return _subPartitionInfoPtr;
    }
    bool genFieldMapMask(const std::string &indexName,
                         const std::vector<std::string>& termFieldNames,
                         fieldmap_t &targetFieldMap);
    df_t getTermDF(const common::Term &term);
    versionid_t getCurrentVersion() const;
    DocMapAttrIterator *getMainToSubIter() {
        makesureIteratorExist();
        return _main2SubIt;
    }
    void addDelPosting(IE_NAMESPACE(index)::PostingIterator *postIter);
    bool getPartedDocIdRanges(const DocIdRangeVector& rangeHint,
                              size_t totalWayCount, size_t wayIdx,
                              DocIdRangeVector& ranges) const;
    bool getPartedDocIdRanges(const DocIdRangeVector& rangeHint,
                              size_t totalWayCount,
                              std::vector<DocIdRangeVector>& rangeVectors) const;

public:
    void setTopK(uint32_t topK) {
        _topK = topK;
    }
    uint32_t getTopK() const {
        return _topK;
    }
    void setSessionPool(autil::mem_pool::Pool* sessionPool) {
        _sessionPool = sessionPool;
    }
protected:
    virtual void getTermKeyStr(const common::Term &term, const LayerMeta *layerMeta,
                               std::string &keyStr);
public:
    // for test
    const std::vector<IndexPartitionReaderPtr>& getIndexPartReaders() const {
        return _indexPartReaderPtrs;
    }
    IndexReaderPtr getIndexReader(const std::string &indexName) {
        bool unused;
        IndexReaderPtr indexReader;
        getIndexReader(indexName, indexReader, unused);
        return indexReader;
    }
    bool getAttributeMetaInfo(
            const std::string &attrName,
            AttributeMetaInfo& metaInfo) const;
    bool getIndexReader(const std::string &indexName,
                        IndexReaderPtr &indexReader,
                        bool &isSubIndex);
    void truncateRewrite(
        const std::string &truncateName,
        IE_NAMESPACE(util)::Term& indexTerm,
        PostingType &pt1,
        PostingType &pt2);
    LookupResult doLookupWithoutCache(
        const IndexReaderPtr &indexReaderPtr,
        bool isSubIndex,
        const IE_NAMESPACE(util)::Term &indexTerm,
        PostingType pt1,
        PostingType pt2,
        const LayerMeta *layerMeta);
    LookupResult makeLookupResult(
        bool isSubIndex,
        IE_NAMESPACE(index)::PostingIterator *postIter);
private:
    bool tryGetFromPostingCache(const std::string &key, LookupResult &result);
    IE_NAMESPACE(util)::Term* createIndexTerm(const common::Term &term);
    LookupResult doLookup(const common::Term &term, const std::string &key,
                          const LayerMeta *layerMeta);
    void makesureIteratorExist();
protected:
    virtual PostingIterator* doLookupByRanges(
            const IndexReaderPtr &indexReaderPtr,
            const IE_NAMESPACE(util)::Term *indexTerm,
            PostingType pt1, PostingType pt2,
            const LayerMeta *layerMeta, bool isSubIndex);
    bool getSubRanges(const LayerMeta *mainLayerMeta,
                      DocIdRangeVector &subRanges);
protected:
    std::map<std::string, uint32_t> _indexName2IdMapSwap;
    std::map<std::string, uint32_t> _attrName2IdMapSwap;
    const std::map<std::string, uint32_t> *_indexName2IdMap;
    const std::map<std::string, uint32_t> *_attrName2IdMap;
    std::vector<IndexPartitionReaderPtr> _indexPartReaderPtrs; 
    PartitionInfoPtr _partitionInfoPtr;
    PartitionInfoPtr _subPartitionInfoPtr;
    PostingCache _postingCache;
    DelPostingVector _delPostingVec;
    uint32_t _topK;
    autil::mem_pool::Pool* _sessionPool;
    // for sub partition seek
    DocMapAttrIterator *_main2SubIt;
    DocMapAttrIterator *_sub2MainIt;
    
private:
    friend class IndexPartitionReaderWrapperTest;
private:
    HA3_LOG_DECLARE();
};


HA3_TYPEDEF_PTR(IndexPartitionReaderWrapper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_INDEXPARTITIONREADERWRAPPER_H
