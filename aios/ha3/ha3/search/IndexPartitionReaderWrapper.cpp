#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <indexlib/index/normal/attribute/accessor/attribute_reader.h>
#include <indexlib/common/number_term.h>
#include <ha3/common/NumberTerm.h>

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
USE_HA3_NAMESPACE(common);
using namespace std;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, IndexPartitionReaderWrapper);

const uint32_t IndexPartitionReaderWrapper::MAIN_PART_ID = 0;
const uint32_t IndexPartitionReaderWrapper::SUB_PART_ID = 1;
const uint32_t IndexPartitionReaderWrapper::JOIN_PART_START_ID = 2;

IndexPartitionReaderWrapper::IndexPartitionReaderWrapper(
        const map<string, uint32_t>* indexName2IdMap,
        const map<string, uint32_t>* attrName2IdMap,
        const vector<IndexPartitionReaderPtr>& indexPartReaderPtrs)
    : _indexPartReaderPtrs(indexPartReaderPtrs)
    , _topK(0)
    , _sessionPool(NULL)
    , _main2SubIt(NULL)
    , _sub2MainIt(NULL)
{
    assert(indexPartReaderPtrs.size() > 0);
    // RealtimePartitionInfo will change whenever add a document
    // so we create a snapshot of PartitionInfo before search
    _indexName2IdMap = indexName2IdMap;
    _attrName2IdMap = attrName2IdMap;
    _partitionInfoPtr = indexPartReaderPtrs[MAIN_PART_ID]->GetPartitionInfo();
    if (indexPartReaderPtrs.size() > SUB_PART_ID && indexPartReaderPtrs[SUB_PART_ID]) {
        _subPartitionInfoPtr = _partitionInfoPtr->GetSubPartitionInfo();
    }
}

IndexPartitionReaderWrapper::IndexPartitionReaderWrapper(
        map<string, uint32_t>& indexName2IdMapSwap,
        map<string, uint32_t>& attrName2IdMapSwap,
        const vector<IndexPartitionReaderPtr>& indexPartReaderPtrs)
    : _indexPartReaderPtrs(indexPartReaderPtrs)
    , _topK(0)
    , _sessionPool(NULL)
    , _main2SubIt(NULL)
    , _sub2MainIt(NULL)
{
    assert(indexPartReaderPtrs.size() > 0);
    // RealtimePartitionInfo will change whenever add a document
    // so we create a snapshot of PartitionInfo before search
    _indexName2IdMapSwap.swap(indexName2IdMapSwap);
    _indexName2IdMap = &_indexName2IdMapSwap;
    _attrName2IdMapSwap.swap(attrName2IdMapSwap);
    _attrName2IdMap = &_attrName2IdMapSwap;

    _partitionInfoPtr = indexPartReaderPtrs[MAIN_PART_ID]->GetPartitionInfo();
    if (indexPartReaderPtrs.size() > SUB_PART_ID && indexPartReaderPtrs[SUB_PART_ID]) {
        _subPartitionInfoPtr = _partitionInfoPtr->GetSubPartitionInfo();
    }
}

IndexPartitionReaderWrapper::~IndexPartitionReaderWrapper() {
    POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _main2SubIt);
    POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _sub2MainIt);

    // clear posting cache
    for(size_t i = 0; i < _delPostingVec.size(); i++) {
        if (_delPostingVec[i] != NULL) {
            POOL_COMPATIBLE_DELETE_CLASS(
                    _delPostingVec[i]->GetSessionPool(), _delPostingVec[i]);
            _delPostingVec[i] = NULL;
        }
    }
    _delPostingVec.clear();
    _postingCache.clear();
}

void IndexPartitionReaderWrapper::addDelPosting(PostingIterator *postIter) {
    _delPostingVec.push_back(postIter);
}

LookupResult IndexPartitionReaderWrapper::lookup(const Term &term,
        const LayerMeta *layerMeta)
{
    LookupResult result;
    LookupResult originalResult;
    string key;
    getTermKeyStr(term, layerMeta, key);
    if (tryGetFromPostingCache(key, originalResult)) {
        result = originalResult;
        if (originalResult.postingIt != NULL) {
            PostingIterator* clonedPosting = originalResult.postingIt->Clone();
            _delPostingVec.push_back(clonedPosting);
            result.postingIt = clonedPosting;
        }
        return result;
    }
    return doLookup(term, key, layerMeta);
}

LookupResult IndexPartitionReaderWrapper::doLookup(
        const Term &term, const string &key, const LayerMeta *layerMeta)
{
    LookupResult result;
    IndexReaderPtr indexReaderPtr;
    bool isSubIndex = false;
    if (!getIndexReader(term.getIndexName(), indexReaderPtr, isSubIndex)) {
        HA3_LOG(WARN, "getIndexReader failed for term: %s:%s",
                term.getIndexName().c_str(), term.getWord().c_str());
        return result;
    }

    IE_NAMESPACE(util)::Term *indexTerm = createIndexTerm(term);
    unique_ptr<IE_NAMESPACE(util)::Term> indexTermPtr(indexTerm);
    PostingType pt1;
    PostingType pt2;
    truncateRewrite(term.getTruncateName(), *indexTermPtr, pt1, pt2);
    result = doLookupWithoutCache(
        indexReaderPtr, isSubIndex, *indexTermPtr, pt1, pt2, layerMeta);
    _postingCache[key] = result;
    return result;
}

void IndexPartitionReaderWrapper::truncateRewrite(
    const std::string &truncateName,
    IE_NAMESPACE(util)::Term& indexTerm,
    PostingType &pt1,
    PostingType &pt2)
{
    if (BITMAP_TRUNCATE_INDEX_NAME == truncateName) {
        pt1 = pt_bitmap;
        pt2 = pt_normal;
    } else {
        indexTerm.SetTruncateName(truncateName);
        pt1 = pt_normal;
        pt2 = pt_bitmap;
    }
}

LookupResult IndexPartitionReaderWrapper::doLookupWithoutCache(
    const IndexReaderPtr &indexReaderPtr,
    bool isSubIndex,
    const IE_NAMESPACE(util)::Term& indexTerm,
    PostingType pt1,
    PostingType pt2,
    const LayerMeta *layerMeta)
{
    PostingIterator* iter = doLookupByRanges(
        indexReaderPtr, &indexTerm, pt1, pt2, layerMeta, isSubIndex);
    return makeLookupResult(isSubIndex, iter);
}

LookupResult IndexPartitionReaderWrapper::makeLookupResult(
    bool isSubIndex,
    PostingIterator *postIter)
{
    LookupResult result;
    _delPostingVec.push_back(postIter);
    result.postingIt = postIter;
    if (isSubIndex && result.postingIt != NULL) {
        result.main2SubIt = _main2SubIt;
        result.sub2MainIt = _sub2MainIt;
        result.isSubPartition = true;
    }
    return result;
}

void IndexPartitionReaderWrapper::makesureIteratorExist() {
#define CREATE_DOCID_ITERATOR(indexPartReader, attrName, retValue) do { \
        AttributeReaderPtr attrReader = indexPartReader->GetAttributeReader(attrName); \
        if (!attrReader) {                                              \
            return;                                                      \
        }                                                               \
        AttributeIteratorBase *attrItBase = attrReader->CreateIterator(_sessionPool); \
        retValue = dynamic_cast<DocMapAttrIterator *>(attrItBase); \
        assert(retValue);                                               \
    } while(0)

    if (_main2SubIt == NULL || _sub2MainIt == NULL) {
        assert(_main2SubIt == NULL && _sub2MainIt == NULL);
        CREATE_DOCID_ITERATOR(_indexPartReaderPtrs[MAIN_PART_ID],
                              MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, _main2SubIt);
        CREATE_DOCID_ITERATOR(_indexPartReaderPtrs[SUB_PART_ID],
                              SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, _sub2MainIt);
    }

#undef CREATE_DOCID_ITERATOR
}


bool IndexPartitionReaderWrapper::getAttributeMetaInfo(
        const string &attrName,
        AttributeMetaInfo& metaInfo) const
{
    if (_attrName2IdMap->empty()) {
        metaInfo.setAttributeName(attrName);
        metaInfo.setAttributeType(AT_MAIN_ATTRIBUTE);
        metaInfo.setIndexPartReader(_indexPartReaderPtrs[MAIN_PART_ID]);
        return true;
    }
    map<string, uint32_t>::const_iterator iter = _attrName2IdMap->find(attrName);
    if (iter == _attrName2IdMap->end()) {
        return false;
    }
    uint32_t id = iter->second;
    metaInfo.setAttributeName(attrName);
    switch (id) {
    case MAIN_PART_ID:
        metaInfo.setIndexPartReader(_indexPartReaderPtrs[MAIN_PART_ID]);
        metaInfo.setAttributeType(AT_MAIN_ATTRIBUTE);
        break;
    case SUB_PART_ID:
        metaInfo.setIndexPartReader(_indexPartReaderPtrs[SUB_PART_ID]);
        metaInfo.setAttributeType(AT_SUB_ATTRIBUTE);
        break;
    default:
        return false;
    }
    return true;
}

bool IndexPartitionReaderWrapper::getIndexReader(
        const string &indexName, IndexReaderPtr &indexReader, bool &isSubIndex)
{
    uint32_t id = MAIN_PART_ID;
    if(_indexPartReaderPtrs.size() == 1) {
        indexReader = _indexPartReaderPtrs[0]->GetIndexReader();
        id = MAIN_PART_ID;
        return true;
    }
    // todo: for test, need remove
    if (_indexName2IdMap->empty()) {
        indexReader = _indexPartReaderPtrs[0]->GetIndexReader();
        id = MAIN_PART_ID;
        return true;
    }
    map<string, uint32_t>::const_iterator it = _indexName2IdMap->find(indexName);
    if (it == _indexName2IdMap->end()) {
        return false;
    }
    id = it->second;
    assert(MAIN_PART_ID <= id && id <= SUB_PART_ID);
    indexReader = _indexPartReaderPtrs[id]->GetIndexReader();
    isSubIndex = (id == SUB_PART_ID);
    if (isSubIndex) {
        makesureIteratorExist();
        assert(_main2SubIt);
        assert(_sub2MainIt);
    }
    return true;
}

df_t IndexPartitionReaderWrapper::getTermDF(const Term &term) {
    LookupResult result;
    string key;
    getTermKeyStr(term, NULL, key);

    if (!tryGetFromPostingCache(key, result)) {
        result = doLookup(term, key, NULL);
    }
    if (result.postingIt != NULL) {
        return result.postingIt->GetTermMeta()->GetDocFreq();
    } else {
        return 0;
    }
}

bool IndexPartitionReaderWrapper::tryGetFromPostingCache(
        const string &key, LookupResult &result)
{
    PostingCache::const_iterator it =  _postingCache.find(key);
    if (it == _postingCache.end()) {
        return false;
    }
    result = it->second;
    return true;
}

IE_NAMESPACE(util)::Term* IndexPartitionReaderWrapper::createIndexTerm(
        const Term &term)
{
    if (term.getTermType() == TT_STRING) {
        assert(term.getTermName() == "Term");
        IE_NAMESPACE(util)::Term *indexTerm = new IE_NAMESPACE(util)::Term();
        indexTerm->SetWord(term.getWord());
        indexTerm->SetIndexName(term.getIndexName());
        return indexTerm;
    } else {
        assert(term.getTermName() == "NumberTerm");
        const NumberTerm *numberTerm = static_cast<const NumberTerm *>(&term);
        IE_NAMESPACE(common)::Int64Term *indexTerm =
            new IE_NAMESPACE(common)::Int64Term(0, "dummy");
        indexTerm->SetWord(numberTerm->getWord());
        indexTerm->SetLeftNumber(numberTerm->getLeftNumber());
        indexTerm->SetRightNumber(numberTerm->getRightNumber());
        indexTerm->SetIndexName(numberTerm->getIndexName());
        return indexTerm;
    }
}

void IndexPartitionReaderWrapper::getTermKeyStr(
        const Term &term, const LayerMeta *layerMeta, string &keyStr) {
    size_t size = term.getToken().getNormalizedText().size()
                 + term.getIndexName().size()
                 + term.getTruncateName().size();
    keyStr.reserve(size + 2);
    keyStr = term.getToken().getNormalizedText().c_str();
    keyStr.append(1, '\t');
    keyStr += term.getIndexName();
    keyStr.append(1, '\t');
    keyStr += term.getTruncateName();
}

versionid_t IndexPartitionReaderWrapper::getCurrentVersion() const {
    assert(_indexPartReaderPtrs.size() > 0);
    const Version &version = _indexPartReaderPtrs[0]->GetVersion();
    return version.GetVersionId();
}

bool IndexPartitionReaderWrapper::genFieldMapMask(
        const string &indexName,
        const vector<string>& termFieldNames,
        fieldmap_t &targetFieldMap)
{
    IndexReaderPtr indexReader;
    bool isSubIndex;
    if (!getIndexReader(indexName, indexReader, isSubIndex)) {
        return false;
    }
    return indexReader->GenFieldMapMask(
            indexName, termFieldNames, targetFieldMap);
}

bool IndexPartitionReaderWrapper::getPartedDocIdRanges(
        const DocIdRangeVector& rangeHint,
        size_t totalWayCount, size_t wayIdx,
        DocIdRangeVector& ranges) const
{
    auto indexPartitionReader = getReader();
    if (nullptr == indexPartitionReader) {
        return false;
    }
    return indexPartitionReader->GetPartedDocIdRanges(
            rangeHint, totalWayCount, wayIdx, ranges);
}

bool IndexPartitionReaderWrapper::getPartedDocIdRanges(
        const DocIdRangeVector& rangeHint,
        size_t totalWayCount,
        std::vector<DocIdRangeVector>& rangeVectors) const
{
    auto indexPartitionReader = getReader();
    if (nullptr == indexPartitionReader) {
        return false;
    }
    return indexPartitionReader->GetPartedDocIdRanges(
            rangeHint, totalWayCount, rangeVectors);
}

bool IndexPartitionReaderWrapper::getSubRanges(const LayerMeta *mainLayerMeta,
        DocIdRangeVector &subRanges) {
    docid_t mainBegin = INVALID_DOCID;
    docid_t mainEnd = INVALID_DOCID;
    docid_t subBegin = INVALID_DOCID;
    docid_t subEnd = INVALID_DOCID;
    for (auto &docIdRangeMeta : *mainLayerMeta) {
        mainBegin = docIdRangeMeta.begin;
        mainEnd = docIdRangeMeta.end;
        subBegin = INVALID_DOCID;
        subEnd = INVALID_DOCID;
        if (0 == mainBegin) {
            subBegin = 0;
        } else {
            if (!_main2SubIt->Seek(mainBegin - 1, subBegin)) {
                return false;
            }
        }
        if (!_main2SubIt->Seek(mainEnd, subEnd)) {
            return false;
        }
        subRanges.emplace_back(subBegin, subEnd);
    }
    return true;
}

PostingIterator* IndexPartitionReaderWrapper::doLookupByRanges(
        const IndexReaderPtr &indexReaderPtr,
        const IE_NAMESPACE(util)::Term *indexTerm,
        PostingType pt1, PostingType pt2,
        const LayerMeta *layerMeta, bool isSubIndex)
{
    PostingIterator* iter = indexReaderPtr->Lookup(*indexTerm, _topK, pt1, _sessionPool);
    if (!iter) {
        iter = indexReaderPtr->Lookup(*indexTerm, _topK, pt2, _sessionPool);
    }
    return iter;
}

END_HA3_NAMESPACE(search);
