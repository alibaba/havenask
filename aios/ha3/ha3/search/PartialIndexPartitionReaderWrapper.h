#ifndef ISEARCH_PARTIALINDEXPARTITIONREADERWRAPPER_H
#define ISEARCH_PARTIALINDEXPARTITIONREADERWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>

BEGIN_HA3_NAMESPACE(search);

class PartialIndexPartitionReaderWrapper : public IndexPartitionReaderWrapper {
public:
    PartialIndexPartitionReaderWrapper(
            const std::map<std::string, uint32_t>* indexName2IdMap, 
            const std::map<std::string, uint32_t>* attrName2IdMap, 
            const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs);

private:
    PartialIndexPartitionReaderWrapper(const PartialIndexPartitionReaderWrapper&);
    PartialIndexPartitionReaderWrapper& operator = (const PartialIndexPartitionReaderWrapper&);

protected:
    void getTermKeyStr(const common::Term &term, const LayerMeta *layerMeta,
                       std::string &keyStr) override;
    PostingIterator* doLookupByRanges(
            const IndexReaderPtr &indexReaderPtr,
            const IE_NAMESPACE(util)::Term *indexTerm,
            PostingType pt1, PostingType pt2,
            const LayerMeta *layerMeta, bool isSubIndex) override;

private:
    friend class PartialIndexPartitionReaderWrapperTest;
private:
    HA3_LOG_DECLARE();
};


HA3_TYPEDEF_PTR(PartialIndexPartitionReaderWrapper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_PARTIALINDEXPARTITIONREADERWRAPPER_H
