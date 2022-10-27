#include <ha3/search/PartialIndexPartitionReaderWrapper.h>

USE_HA3_NAMESPACE(common);
using namespace std;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, PartialIndexPartitionReaderWrapper);

PartialIndexPartitionReaderWrapper::PartialIndexPartitionReaderWrapper(
        const std::map<std::string, uint32_t>* indexName2IdMap,
        const std::map<std::string, uint32_t>* attrName2IdMap,
        const std::vector<IndexPartitionReaderPtr>& indexPartReaderPtrs)
    : IndexPartitionReaderWrapper(
            indexName2IdMap, attrName2IdMap, indexPartReaderPtrs)
{

}

void PartialIndexPartitionReaderWrapper::getTermKeyStr(
        const Term &term, const LayerMeta *layerMeta, string &keyStr) {
    IndexPartitionReaderWrapper::getTermKeyStr(term, layerMeta, keyStr);
    if (NULL != layerMeta) {
        keyStr.append(1, '\t');
        keyStr += layerMeta->getRangeString();
    }
}

IE_NAMESPACE(index)::PostingIterator*
    PartialIndexPartitionReaderWrapper::doLookupByRanges(
        const IndexReaderPtr &indexReaderPtr,
        const IE_NAMESPACE(util)::Term *indexTerm,
        PostingType pt1, PostingType pt2,
        const LayerMeta *layerMeta, bool isSubIndex)
{
    if (NULL == layerMeta) {
        return IndexPartitionReaderWrapper::doLookupByRanges(indexReaderPtr,
                indexTerm, pt1, pt2, layerMeta, isSubIndex);
    }
    if (0 == layerMeta->size()) {
        return NULL;
    }
    DocIdRangeVector ranges;
    if (!isSubIndex) {
        for (auto &docIdRangeMeta : *layerMeta) {
            ranges.emplace_back(docIdRangeMeta.begin,
                            docIdRangeMeta.end + 1);
        }
    } else {
        if(!getSubRanges(layerMeta, ranges)) {
            HA3_LOG(WARN, "layerMeta:[%s] get sub ranges failed.",
                    layerMeta->toString().c_str());
            return IndexPartitionReaderWrapper::doLookupByRanges(indexReaderPtr,
                    indexTerm, pt1, pt2, NULL, isSubIndex);
        }
    }

    PostingIterator* iter = indexReaderPtr->PartialLookup(*indexTerm, ranges,
            _topK, pt1, _sessionPool);
    if (!iter) {
        iter = indexReaderPtr->PartialLookup(*indexTerm, ranges, _topK, pt2,
                _sessionPool);
    }
    return iter;
}

END_HA3_NAMESPACE(search);
