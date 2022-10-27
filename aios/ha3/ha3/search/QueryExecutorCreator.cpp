#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/PhraseQueryExecutor.h>
#include <ha3/search/RestrictPhraseQueryExecutor.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/search/AndQueryExecutor.h>
#include <ha3/search/WeakAndQueryExecutor.h>
#include <ha3/search/OrQueryExecutor.h>
#include <ha3/search/AndNotQueryExecutor.h>
#include <ha3/search/NumberQueryExecutor.h>
#include <ha3/search/RangeTermQueryExecutor.h>
#include <ha3/search/SpatialTermQueryExecutor.h>
#include <ha3/search/BitmapTermQueryExecutor.h>
#include <ha3/search/BitmapAndQueryExecutor.h>
#include <ha3/search/BufferedTermQueryExecutor.h>
#include <ha3/search/FieldMapTermQueryExecutor.h>
#include <ha3/search/SubTermQueryExecutor.h>
#include <ha3/search/SubFieldMapTermQueryExecutor.h>
#include <ha3/search/QueryExecutorRestrictor.h>
#include <ha3/search/MultiTermAndQueryExecutor.h>
#include <ha3/search/MultiTermBitmapAndQueryExecutor.h>
#include <ha3/search/MultiTermOrQueryExecutor.h>
#include <autil/MultiValueType.h>
#include <autil/StringUtil.h>
#include <indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h>
#include <indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h>
#include <ha3/sql/util/ValueTypeSwitch.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, QueryExecutorCreator);

QueryExecutorCreator::QueryExecutorCreator(
        MatchDataManager *matchDataManager,
        IndexPartitionReaderWrapper *readerWrapper,
        autil::mem_pool::Pool *pool,
        common::TimeoutTerminator * timer,
        const LayerMeta *layerMeta)
    : _matchDataManager(matchDataManager)
    , _queryExecutor(NULL)
    , _readerWrapper(readerWrapper)
    , _andnotQueryLevel(0)
    , _pool(pool)
    , _timer(timer)
    , _layerMeta(layerMeta)
{
    if (_matchDataManager) {
        _matchDataManager->beginLayer();
    }
}

QueryExecutorCreator::~QueryExecutorCreator() {
    POOL_DELETE_CLASS(_queryExecutor);
}

TermQueryExecutor *QueryExecutorCreator::doCreateTermQueryExecutor(
        IndexPartitionReaderWrapper *readerWrapper,
        const common::Term &queryTerm,
        const LookupResult &result,
        autil::mem_pool::Pool *pool,
        const LayerMeta *layerMeta)
{
    IE_NAMESPACE(index)::PostingIterator *iter = result.postingIt;
    if (!iter) {
        TermQueryExecutor * ret = POOL_NEW_CLASS(pool,
                TermQueryExecutor, iter, queryTerm);
        ret->moveToEnd();
        return ret;
    }
    const RequiredFields &requiredFields = queryTerm.getRequiredFields();
    if (!requiredFields.fields.empty() && iter->GetType() == pi_buffered) {
        fieldmap_t targetFieldMap;
        if (!readerWrapper->genFieldMapMask(queryTerm.getIndexName(),
                        requiredFields.fields, targetFieldMap))
        {
            HA3_LOG(WARN, "Term:[%s], Reader does not have fieldMap!",
                    queryTerm.toString().c_str());
            TermQueryExecutor * ret = POOL_NEW_CLASS(pool,
                    TermQueryExecutor, iter, queryTerm);
            ret->moveToEnd();
            return ret;
        }
        FieldMatchOperatorType fieldMatchOperatorType =
            requiredFields.isRequiredAnd ? FM_AND : FM_OR;
        if (!result.isSubPartition) {
            return POOL_NEW_CLASS(pool, FieldMapTermQueryExecutor, iter,
                    queryTerm, targetFieldMap,
                    fieldMatchOperatorType);
        } else {
            return POOL_NEW_CLASS(pool, SubFieldMapTermQueryExecutor, iter,
                    queryTerm, result.main2SubIt, result.sub2MainIt,
                    targetFieldMap, fieldMatchOperatorType);
        }
    }
    if (result.isSubPartition) {
        return POOL_NEW_CLASS(pool, SubTermQueryExecutor, iter,
                              queryTerm, result.main2SubIt, result.sub2MainIt);
    }
    if (iter->GetType() == pi_bitmap) {
        return POOL_NEW_CLASS(pool, BitmapTermQueryExecutor, iter, queryTerm);
    } else if (iter->GetType() == pi_buffered) {
        return POOL_NEW_CLASS(pool, BufferedTermQueryExecutor, iter, queryTerm);
    } else if (iter->GetType() == pi_seek_and_filter) {
        SeekAndFilterIterator* indexIter = (SeekAndFilterIterator*)iter;
        if (indexIter->GetInnerIteratorType() == pi_spatial) {
            return POOL_NEW_CLASS(pool, SpatialTermQueryExecutor, iter, queryTerm);
        } else {
            HA3_LOG(WARN, "Term:[%s], ha3 does not have support such seek and filter Iterator",
                    queryTerm.toString().c_str());
            TermQueryExecutor * ret = POOL_NEW_CLASS(pool,
                    TermQueryExecutor, iter, queryTerm);
            ret->moveToEnd();
            return ret;
        }
    } else {
        return POOL_NEW_CLASS(pool, TermQueryExecutor, iter, queryTerm);
    }
}

TermQueryExecutor *QueryExecutorCreator::createTermQueryExecutor(
        IndexPartitionReaderWrapper *readerWrapper,
        const common::Term &queryTerm,
        autil::mem_pool::Pool *pool,
        const LayerMeta *layerMeta)
{
    LookupResult result = readerWrapper->lookup(queryTerm, layerMeta);
    return createTermQueryExecutor(readerWrapper, queryTerm, result, pool, layerMeta);
}

TermQueryExecutor *QueryExecutorCreator::createTermQueryExecutor(
            IndexPartitionReaderWrapper *readerWrapper,
            const common::Term &queryTerm,
            const LookupResult &result,
            autil::mem_pool::Pool *pool,
            const LayerMeta *layerMeta)
{
    auto termQueryExecutor = doCreateTermQueryExecutor(readerWrapper, queryTerm, result, pool, layerMeta);
    if (termQueryExecutor) {
        termQueryExecutor->setIndexPartitionReaderWrapper(readerWrapper);
    }
    return termQueryExecutor;
}

void QueryExecutorCreator::visitTermQuery(const TermQuery *query) {
    assert(_readerWrapper);
    TermQueryExecutor *termQueryExecutor = createTermQueryExecutor(
            _readerWrapper, query->getTerm(), _pool, _layerMeta);
    addUnpackQuery(termQueryExecutor, query->getTerm(), query);
    _queryExecutor = termQueryExecutor;
}

void QueryExecutorCreator::visitPhraseQuery(const PhraseQuery *query) {
    assert(_readerWrapper);
    const PhraseQuery::TermArray &terms = query->getTermArray();
    int32_t relativePos = 0;
    int32_t postingMark = 0;
    vector<TermQueryExecutor*> termQueryExecutors;
    PhraseQueryExecutor *queryExecutor;
    if (query->hasQueryRestrictor()) {
        QueryExecutorRestrictor *restrictor = new QueryExecutorRestrictor();
        restrictor->setTimeoutTerminator(_timer);
        restrictor->setLayerMeta(_layerMeta);
        queryExecutor =  POOL_NEW_CLASS(_pool, RestrictPhraseQueryExecutor, restrictor);
    } else {
        queryExecutor =  POOL_NEW_CLASS(_pool, PhraseQueryExecutor);
    }
    bool hasEmptyExecutor = false;
    bool hasSubExecutor = false;
    bool hasMainExecutor = false;
    for (PhraseQuery::TermArray::const_iterator it = terms.begin();
         it != terms.end(); it++)
    {
        if ((*it)->getToken().isStopWord()) {
            ++relativePos;
            continue;
        }
        TermQueryExecutor *termQueryExecutor = createTermQueryExecutor(
                _readerWrapper, **it, _pool, _layerMeta);
        if (termQueryExecutor->isEmpty()) {
            hasEmptyExecutor = true;
        }
        if (termQueryExecutor->hasSubDocExecutor()) {
            hasSubExecutor = true;
        } else {
            hasMainExecutor = true;
        }

        HA3_LOG(TRACE3, "Add Term: |%s|", (*it)->getWord().c_str());
        addUnpackQuery(termQueryExecutor, **it, query);
        termQueryExecutors.push_back(termQueryExecutor);
        if (termQueryExecutor->hasPosition()) {
            queryExecutor->addRelativePostion(relativePos, postingMark);
        }
        ++postingMark;
        ++relativePos;
    }
    queryExecutor->addTermQueryExecutors(termQueryExecutors);
    if (hasEmptyExecutor || 0 == postingMark||
        (hasSubExecutor && hasMainExecutor))
    {
        queryExecutor->moveToEnd();
    }
    _queryExecutor = queryExecutor;
    addUnpackQuery(_queryExecutor, query);
}

void QueryExecutorCreator::visitMultiTermQuery(const MultiTermQuery *query) {
    assert(_readerWrapper);
    const MultiTermQuery::TermArray &terms = query->getTermArray();
    vector<QueryExecutor*> termQueryExecutors;
    QueryOperator opExpr = query->getOpExpr();
    uint32_t emptyCount = 0;
    for (MultiTermQuery::TermArray::const_iterator it = terms.begin();
         it != terms.end(); it++)
    {
        TermQueryExecutor *termQueryExecutor = createTermQueryExecutor(
                _readerWrapper, **it, _pool, _layerMeta);
        if (termQueryExecutor->isEmpty()) {
            ++emptyCount;
        }
        HA3_LOG(TRACE3, "Add Term: |%s|", (*it)->getWord().c_str());
        addUnpackQuery(termQueryExecutor, **it, query);
        termQueryExecutors.push_back(termQueryExecutor);
    }

    MultiQueryExecutor *queryExecutor = NULL;
    uint32_t minShouldMatch = query->getMinShouldMatch();
    if (query->getOpExpr() == OP_OR) {
        queryExecutor = POOL_NEW_CLASS(_pool, MultiTermOrQueryExecutor);
    } else if (query->getOpExpr() == OP_WEAKAND) {
        queryExecutor = POOL_NEW_CLASS(
                _pool, WeakAndQueryExecutor, minShouldMatch);
    } else {
        queryExecutor = createMultiTermAndQueryExecutor(termQueryExecutors);
    }
    if ((terms.size() == 0) ||
        (opExpr == OP_AND && emptyCount > 0) ||
        (opExpr == OP_OR && emptyCount == terms.size()) ||
        (opExpr == OP_WEAKAND && emptyCount > terms.size() - minShouldMatch))
    {
        queryExecutor->moveToEnd();
    }
    queryExecutor->addQueryExecutors(termQueryExecutors);
    _queryExecutor = queryExecutor;
    addUnpackQuery(_queryExecutor, query);
}

void QueryExecutorCreator::visitAndQuery(const AndQuery *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    vector<QueryExecutor*> queryExecutors;
    bool hasEmptyExecutor = false;
    for (vector<QueryPtr>::const_iterator it = children->begin();
         it != children->end(); ++it)
    {
        (*it)->accept(this);
        QueryExecutor *queryExecutor = stealQuery();
        if (queryExecutor->isEmpty()) {
            hasEmptyExecutor = true;
        }
        queryExecutors.push_back(queryExecutor);
    }
    MultiQueryExecutor *multiQueryExecutor = createAndQueryExecutor(
            queryExecutors);
    multiQueryExecutor->addQueryExecutors(queryExecutors);
    if (hasEmptyExecutor) {
        multiQueryExecutor->moveToEnd();
    }
    _queryExecutor = multiQueryExecutor;
    addUnpackQuery(_queryExecutor, query);
}

vector<QueryExecutor *> QueryExecutorCreator::colTermProcess(const ColumnTerm* col, const vector<QueryExecutor *> &termExecutors, QueryOperator op) {
    common::Term fakeTerm;
    vector<QueryExecutor *> colExecutors;
    const auto &offsets = col->getOffsets();
    size_t size = offsets.size() - 1;
    colExecutors.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        auto begin = offsets[i];
        auto end = offsets[i + 1];
        size_t count = end - begin;
        if (0u == count) {
            QueryExecutor *p = POOL_NEW_CLASS(_pool,
                TermQueryExecutor, nullptr, fakeTerm);
            p->moveToEnd();
            colExecutors.push_back(p);
        } else if (1u == count) {
            colExecutors.push_back(termExecutors[begin]);
        } else {
            vector<QueryExecutor *> childQueryExecutors;
            childQueryExecutors.assign(termExecutors.begin() + begin, termExecutors.begin() + end);
            auto opExecutor = mergeQueryExecutor(childQueryExecutors, op);
            colExecutors.push_back(opExecutor);
        }
    }
    return colExecutors;
}

QueryExecutor *QueryExecutorCreator::mergeQueryExecutor(const vector<QueryExecutor *> &childQueryExecutors, QueryOperator op) {
    MultiQueryExecutor *opExecutor = nullptr;
    if (1u == childQueryExecutors.size()) {
        return childQueryExecutors[0];
    }
    switch (op) {
    case OP_AND:
        opExecutor = POOL_NEW_CLASS(_pool, AndQueryExecutor);
        break;
    case OP_WEAKAND:
        opExecutor = POOL_NEW_CLASS(_pool, WeakAndQueryExecutor, 1);
        break;
    default:
        opExecutor = POOL_NEW_CLASS(_pool, OrQueryExecutor);
        break;
    }
    opExecutor->addQueryExecutors(childQueryExecutors);
    return opExecutor;
}

void QueryExecutorCreator::visitColumnTerm(
        const ColumnTerm &term,
        vector<LookupResult> &results)
{
    IndexReaderPtr indexReader;
    bool isSubIndex = false;
    const auto &indexName = term.getIndexName();
    _readerWrapper->getIndexReader(indexName, indexReader, isSubIndex);
    if (! indexReader) {
        HA3_LOG(WARN, "GetIndexReader failed for indexName[%s]", indexName.c_str());
        return;
    }
    auto multiIndexReader = DYNAMIC_POINTER_CAST(MultiFieldIndexReader, indexReader);
    if (! multiIndexReader) {
        HA3_LOG(WARN, "unknown error");
        return;
    }
    indexReader = multiIndexReader->GetIndexReader(indexName);
    if (! indexReader) {
        HA3_LOG(WARN, "GetIndexReader failed for indexName[%s]", indexName.c_str());
        return;
    }
    switch (indexReader->GetIndexType()) {
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
        visitNumberColumnTerm(indexReader, isSubIndex, term, results);
        break;
    case it_primarykey64:
        visitPk64ColumnTerm(indexReader, isSubIndex, term, results);
        break;
    default:
        visitNormalColumnTerm(indexReader, isSubIndex, term, results);
        break;
    }
}

namespace {

    template <class T>
    struct NumberTermTraits {
        void setTermWord(IE_NAMESPACE(util)::Term &term, const T &value) const {
            term.SetTermHashKey(value);
        }
    };

    template <>
    void NumberTermTraits<MultiChar>::setTermWord(
            IE_NAMESPACE(util)::Term &term,
            const MultiChar &value) const
    {
        string word(value.data(), value.size());
        term.SetWord(word);
    }

    template <class T>
    struct NormalTermTraits {
        void setTermWord(IE_NAMESPACE(util)::Term &term, const T &value) const {
            term.SetWord(StringUtil::toString(value));
        }
    };

    template <>
    void NormalTermTraits<MultiChar>::setTermWord(
            IE_NAMESPACE(util)::Term &term,
            const MultiChar &value) const
    {
        string word(value.data(), value.size());
        term.SetWord(word);
    }

    template<class T>
    struct Pk64TermTraits {
        docid_t calDocid(const UInt64PrimaryKeyIndexReaderPtr &p, const T &value) {
            return p->LookupWithNumber(value);
        }
    };

    template <>
    docid_t Pk64TermTraits<MultiChar>::calDocid(
            const UInt64PrimaryKeyIndexReaderPtr &p,
            const MultiChar &value)
    {
        ConstString word(value.data(), value.size());
        return p->Lookup(word);
    }
}

class LiteTerm : public IE_NAMESPACE(util)::Term
{
public:
    using Term::Term;
public:
    void clearTermHashKey() {
        mHasValidHashKey = false;
        mLiteTerm.SetTermHashKey(INVALID_DICT_VALUE);
    }
};

void QueryExecutorCreator::visitNumberColumnTerm(
        const IndexReaderPtr &indexReader,
        bool isSubIndex,
        const ColumnTerm &term,
        vector<LookupResult> &results)
{
#define CASE_MACRO(bt)                                                          \
    case bt: {                                                                  \
        using T = typename MatchDocBuiltinType2CppType<bt, false>::CppType;     \
        lookupNormalIndex<T>(                                                   \
            NumberTermTraits<T>(), indexReader, isSubIndex, term, results);     \
        break;                                                                  \
    }

    switch(term.getValueType()) {
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO)
    case bt_bool:
        lookupNormalIndex<bool>(
            NumberTermTraits<bool>(), indexReader, isSubIndex, term, results);
        break;
    default:
        break;
    }

#undef CASE_MACRO
}

void QueryExecutorCreator::visitPk64ColumnTerm(
        const IndexReaderPtr &indexReader,
        bool isSubIndex,
        const ColumnTerm &term,
        vector<LookupResult> &results)
{
#define CASE_MACRO(bt)                                                          \
    case bt: {                                                                  \
        using T = typename MatchDocBuiltinType2CppType<bt, false>::CppType;     \
        lookupPk64Index<T>(                                                     \
            Pk64TermTraits<T>(), indexReader, isSubIndex, term, results);       \
        break;                                                                  \
    }

    switch(term.getValueType()) {
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO)
    case bt_bool:
        lookupPk64Index<bool>(
            Pk64TermTraits<bool>(), indexReader, isSubIndex, term, results);
        break;
    default:
        break;
    }

#undef CASE_MACRO
}

void QueryExecutorCreator::visitNormalColumnTerm(
        const IndexReaderPtr &indexReader,
        bool isSubIndex,
        const ColumnTerm &term,
        vector<LookupResult> &results)
{
#define CASE_MACRO(bt)                                                          \
    case bt: {                                                                  \
        using T = typename MatchDocBuiltinType2CppType<bt, false>::CppType;     \
        lookupNormalIndex<T>(                                                   \
            NormalTermTraits<T>(), indexReader, isSubIndex, term, results);     \
        break;                                                                  \
    }

    switch(term.getValueType()) {
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO)
    case bt_bool:
        lookupNormalIndex<bool>(
            NormalTermTraits<bool>(), indexReader, isSubIndex, term, results);
        break;
    default:
        break;
    }

#undef CASE_MACRO
}

template <class T, class Traits>
LookupResult QueryExecutorCreator::lookupNormalIndexWithoutCache(
        Traits traits,
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        LiteTerm &term,
        PostingType pt1,
        PostingType pt2,
        const T &value)
{
    term.clearTermHashKey();
    traits.setTermWord(term, value);
    return _readerWrapper->doLookupWithoutCache(
        indexReader, isSubIndex, term, pt1, pt2, _layerMeta);
}

template <class T, class Traits>
void QueryExecutorCreator::lookupNormalIndex(
        Traits traits,
        const IndexReaderPtr &indexReader,
        bool isSubIndex,
        const ColumnTerm &term,
        vector<LookupResult> &results)
{
    auto p = dynamic_cast<const ColumnTermTyped<T>*>(&term);
    if (! p) {
        return;
    }
    LiteTerm lite;
    lite.SetIndexName(p->getIndexName());
    PostingType pt1;
    PostingType pt2;
    _readerWrapper->truncateRewrite(std::string(), lite, pt1, pt2);
    const auto &values = p->getValues();
    const auto termSize = values.size();
    results.reserve(termSize);
    if (term.getEnableCache()) {
        unordered_map<T, LookupResult> cache;
        cache.reserve(termSize);
        for (const auto &value : values) {
            auto iter = cache.find(value);
            if (cache.end() == iter) {
                auto result = lookupNormalIndexWithoutCache(
                    traits, indexReader, isSubIndex, lite, pt1, pt2, value);
                cache[value] = result;
                results.push_back(result);
            } else {
                auto result = iter->second;
                if (nullptr != result.postingIt) {
                    PostingIterator *clonePosting = result.postingIt->Clone();
                    _readerWrapper->addDelPosting(clonePosting);
                    result.postingIt = clonePosting;
                }
                results.push_back(result);
            }
        }
    } else {
        for (const auto &value : values) {
            auto result = lookupNormalIndexWithoutCache(
                traits, indexReader, isSubIndex, lite, pt1, pt2, value);
            results.push_back(result);
        }
    }
}

template <class T, class Traits>
LookupResult QueryExecutorCreator::lookupPk64IndexWithoutCache(
        Traits traits,
        const UInt64PrimaryKeyIndexReaderPtr &pkReader,
        bool isSubIndex,
        const T &value)
{
    docid_t docId = traits.calDocid(pkReader, value);
    auto iter = IE_POOL_COMPATIBLE_NEW_CLASS(
        _pool, PrimaryKeyPostingIterator, docId, _pool);
    return _readerWrapper->makeLookupResult(isSubIndex, iter);
}

template <class T, class Traits>
void QueryExecutorCreator::lookupPk64Index(
        Traits traits,
        const IE_NAMESPACE(index)::IndexReaderPtr &indexReader,
        bool isSubIndex,
        const common::ColumnTerm &term,
        std::vector<LookupResult> &results)
{
    auto p = dynamic_cast<const ColumnTermTyped<T>*>(&term);
    if (! p)  {
        return;
    }
    const auto &values = p->getValues();
    const auto termSize = values.size();
    results.reserve(termSize);
    UInt64PrimaryKeyIndexReaderPtr pkReader = DYNAMIC_POINTER_CAST(
        UInt64PrimaryKeyIndexReader, indexReader);
    if (nullptr == pkReader) {
        HA3_LOG(WARN, "Cast to UInt64PrimaryKeyIndexReader failed");
        return;
    }
    if (term.getEnableCache()) {
        unordered_map<T, LookupResult> cache;
        cache.reserve(termSize);
        for (const auto &value : values) {
            auto iter = cache.find(value);
            if (cache.end() == iter) {
                auto result = lookupPk64IndexWithoutCache(
                    traits, pkReader, isSubIndex, value);
                cache[value] = result;
                results.push_back(result);
            } else {
                auto result = iter->second;
                if (nullptr != result.postingIt) {
                    PostingIterator *clonePosting = result.postingIt->Clone();
                    _readerWrapper->addDelPosting(clonePosting);
                    result.postingIt = clonePosting;
                }
                results.push_back(result);
            }
        }
    } else {
        for (const auto &value : values) {
            auto result = lookupPk64IndexWithoutCache(
                traits, pkReader, isSubIndex, value);
            results.push_back(result);
        }
    }
}

void QueryExecutorCreator::visitTableQuery(const TableQuery *query) {
    common::Term fakeTerm;
    vector<vector<QueryExecutor *>> queryExecutors;
    queryExecutors.reserve(query->getTermArray().size());
    for (auto col : query->getTermArray()) {
        vector<QueryExecutor *> termExecutors;
        vector<LookupResult> results;
        visitColumnTerm(*col, results);
        if (results.empty()) {
            continue;
        }
        for (const auto &result : results) {
            TermQueryExecutor *p = createTermQueryExecutor(_readerWrapper, fakeTerm, result, _pool, _layerMeta);
            termExecutors.push_back(p);
        }
        if (col->getOffsets().empty()) {
            queryExecutors.emplace_back(move(termExecutors));
        } else {
            vector<QueryExecutor *> colExecutors = colTermProcess(col, termExecutors, query->getMultiValueOp());
            queryExecutors.emplace_back(move(colExecutors));
        }
    }
    if (queryExecutors.empty()) {
        QueryExecutor *p = POOL_NEW_CLASS(_pool,
                TermQueryExecutor, nullptr, fakeTerm);
        p->moveToEnd();
        _queryExecutor = p;
        addUnpackQuery(_queryExecutor, query);
        return;
    }
    auto rowSize = queryExecutors[0].size();
    vector<QueryExecutor *> rowExecutors;
    rowExecutors.reserve(rowSize);
    vector<QueryExecutor *> rowVec;
    rowVec.reserve(queryExecutors.size());
    for (size_t row = 0; row < rowSize; ++row) {
        rowVec.clear();
        for(size_t col = 0; col < queryExecutors.size(); ++col) {
            rowVec.push_back(queryExecutors[col][row]);
        }
        rowExecutors.push_back(mergeQueryExecutor(rowVec, query->getColumnOp()));
    }
    _queryExecutor = mergeQueryExecutor(rowExecutors, query->getRowOp());
    addUnpackQuery(_queryExecutor, query);
}

#define macroCreateAndQueryExecutor(funcName, andExecutor, bitmapExecutor)  \
    MultiQueryExecutor *QueryExecutorCreator::funcName(                 \
            const vector<QueryExecutor*> &queryExecutors) {             \
        if (canConvertToBitmapAndQuery(queryExecutors)) {               \
            return POOL_NEW_CLASS(_pool, bitmapExecutor, _pool);        \
        } else {                                                        \
            return POOL_NEW_CLASS(_pool, andExecutor);                  \
        }                                                               \
    }                                                                   \

macroCreateAndQueryExecutor(createAndQueryExecutor, AndQueryExecutor, BitmapAndQueryExecutor)
macroCreateAndQueryExecutor(createMultiTermAndQueryExecutor, MultiTermAndQueryExecutor,
                            MultiTermBitmapAndQueryExecutor)

#undef macroCreateAndQueryExecutor

bool QueryExecutorCreator::canConvertToBitmapAndQuery(
        const vector<QueryExecutor*> &queryExecutors)
{
    for (vector<QueryExecutor*>::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        if ((*it)->getName() == "BitmapTermQueryExecutor") {
            return true;
        }
    }
    return false;
}
void QueryExecutorCreator::visitOrQuery(const OrQuery *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    vector<QueryExecutor*> queryExecutors;
    bool isAllEmpty = true;
    for (vector<QueryPtr>::const_iterator it = children->begin();
         it != children->end(); it++)
    {
        (*it)->accept(this);
        QueryExecutor *childQueryExecutor = stealQuery();
        if (!childQueryExecutor->isEmpty()) {
            isAllEmpty = false;
        }
        queryExecutors.push_back(childQueryExecutor);
    }
    if (queryExecutors.size() == 1) {
        _queryExecutor = queryExecutors[0];
    } else {
        OrQueryExecutor *orQueryExecutor = POOL_NEW_CLASS(
                _pool, OrQueryExecutor);
        orQueryExecutor->addQueryExecutors(queryExecutors);
        _queryExecutor = orQueryExecutor;
        if (isAllEmpty) {
            _queryExecutor->moveToEnd();
        }
    }
    addUnpackQuery(_queryExecutor, query);
}

void QueryExecutorCreator::visitAndNotQuery(const AndNotQuery *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    if (children->size() < 1) {
        return;
    }

    (*children)[0]->accept(this);
    QueryExecutor *childQueryExecutor = stealQuery();
    ++_andnotQueryLevel;
    vector<QueryExecutor*> queryExecutors;
    queryExecutors.push_back(childQueryExecutor);
    vector<QueryPtr>::const_iterator it = children->begin();
    for (++it; it != children->end(); ++it) {
        (*it)->accept(this);
        childQueryExecutor = stealQuery();
        if (childQueryExecutor) {
            queryExecutors.push_back(childQueryExecutor);
        }
    }
    QueryExecutor *leftExecutor = queryExecutors[0];
    QueryExecutor *rightExecutor = NULL;
    if (queryExecutors.size() == 2) {
        rightExecutor = queryExecutors[1];
    } else {
        OrQueryExecutor *orExecutor = POOL_NEW_CLASS(_pool, OrQueryExecutor);
        vector<QueryExecutor*> childQueryExecutors(
                queryExecutors.begin() + 1, queryExecutors.end());
        orExecutor->addQueryExecutors(childQueryExecutors);
        rightExecutor = orExecutor;
    }
    AndNotQueryExecutor *queryExecutor = POOL_NEW_CLASS(
            _pool, AndNotQueryExecutor);
    queryExecutor->addQueryExecutors(leftExecutor, rightExecutor);
    _queryExecutor = queryExecutor;
    if (queryExecutors[0]->isEmpty()) {
        _queryExecutor->moveToEnd();
    }
    --_andnotQueryLevel;
    addUnpackQuery(_queryExecutor, query);
}

void QueryExecutorCreator::visitRankQuery(const RankQuery *query) {
    const vector<QueryPtr>* children = query->getChildQuery();
    assert(children);
    (*children)[0]->accept(this);
    QueryExecutor *queryExecutor = stealQuery();
    vector<QueryPtr>::const_iterator it;
    for (it = children->begin(), ++it; it != children->end(); ++it)
    {
        (*it)->accept(this);
        QueryExecutor *childQueryExecutor = stealQuery();
        if (_matchDataManager) {
            _matchDataManager->addRankQueryExecutor(childQueryExecutor);
        }
    }
    _queryExecutor = queryExecutor;
}

void QueryExecutorCreator::visitNumberQuery(const common::NumberQuery *query) {
    assert(_readerWrapper);
    const common::NumberTerm &queryTerm = query->getTerm();
    LookupResult result = _readerWrapper->lookup(queryTerm, _layerMeta);
    PostingIterator *iter = result.postingIt;
    TermQueryExecutor *termQueryExecutor = NULL;
    if (!iter) {
        termQueryExecutor = POOL_NEW_CLASS(_pool,
                NumberQueryExecutor, iter, queryTerm);
        termQueryExecutor->moveToEnd();
    } else if (result.isSubPartition) {
        termQueryExecutor = POOL_NEW_CLASS(_pool, SubTermQueryExecutor, iter,
                queryTerm, result.main2SubIt, result.sub2MainIt);
    } else if (iter->GetType() == pi_bitmap) {
        termQueryExecutor = POOL_NEW_CLASS(_pool,
                BitmapTermQueryExecutor, iter, queryTerm);
    } else if (iter->GetType() == pi_buffered) {
        termQueryExecutor = POOL_NEW_CLASS(_pool,
                BufferedTermQueryExecutor, iter, queryTerm);
    } else if (iter->GetType() == pi_seek_and_filter) {
        SeekAndFilterIterator* indexIter = (SeekAndFilterIterator*)iter;
        if (indexIter->GetInnerIteratorType() == pi_range) {
            termQueryExecutor = POOL_NEW_CLASS(_pool,
                    RangeTermQueryExecutor, iter, queryTerm);
        } else {
            assert(false);
            return;
        }
    } else {
        termQueryExecutor = POOL_NEW_CLASS(_pool,
                NumberQueryExecutor, iter, queryTerm);
    }
    if (termQueryExecutor) {
        termQueryExecutor->setIndexPartitionReaderWrapper(_readerWrapper);
    }
    addUnpackQuery(termQueryExecutor, queryTerm, query);
    _queryExecutor = termQueryExecutor;
}

QueryExecutor* QueryExecutorCreator::stealQuery() {
    QueryExecutor *queryExecutor = _queryExecutor;
    _queryExecutor = NULL;
    return queryExecutor;
}

void QueryExecutorCreator::addUnpackQuery(TermQueryExecutor *termQueryExecutor,
                                          const Term &term, const Query *query)
{
    assert(_andnotQueryLevel >= 0);
    if (_andnotQueryLevel == 0 && query->getMatchDataLevel() == MDL_TERM && _matchDataManager) {
        termQueryExecutor->setQueryLabel(query->getQueryLabel());
        _matchDataManager->addQueryExecutor(termQueryExecutor);
    }
}

void QueryExecutorCreator::addUnpackQuery(QueryExecutor *queryExecutor,
                                          const Query *query)
{
    assert(_andnotQueryLevel >= 0);
    if (_andnotQueryLevel == 0 && query->getMatchDataLevel() == MDL_SUB_QUERY && _matchDataManager) {
        queryExecutor->setQueryLabel(query->getQueryLabel());
        _matchDataManager->addQueryExecutor(queryExecutor);
    }
}

END_HA3_NAMESPACE(search);

