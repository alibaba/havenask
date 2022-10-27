#ifndef ISEARCH_NORMALAGGREGATOR_H
#define ISEARCH_NORMALAGGREGATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tr1/unordered_map>
#include <autil/HashAlgorithm.h>
#include <autil/StringUtil.h>
#include <autil/MultiValueType.h>
#include <ha3/util/NumericLimits.h>
#include <ha3/common/FilterClause.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <ha3/search/AggregateFunction.h>
#include <ha3/search/AggregateSampler.h>
#include <ha3/search/Aggregator.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/search/Filter.h>
#include <utility>
#include <matchdoc/ValueType.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/MaxAggregateFunction.h>
#include <ha3/search/MinAggregateFunction.h>
#include <ha3/search/SumAggregateFunction.h>
#include <ha3/cava/AggItemMap.h>
#include <suez/turing/common/QueryResource.h>
#include <suez/turing/common/CavaJitWrapper.h>

namespace std {
namespace tr1 {
template<> struct hash<autil::MultiChar> {
    std::size_t operator() (const autil::MultiChar &multiChar) const {
        return autil::HashAlgorithm::hashString(multiChar.data(), multiChar.size(), 0);
    }
};
}
}

BEGIN_HA3_NAMESPACE(search);

template <class Alloc, class key_type,
          class value_type, class EqualKey>
class EmptyNoEraseLogic {
private:
    typedef typename Alloc::template rebind<value_type>::other value_alloc_type;
public:
    typedef typename value_alloc_type::size_type size_type;
    typedef typename value_alloc_type::pointer pointer;
public:
    bool test_deleted(const size_type num_deleted,
                      const key_type &delkey,
                      const key_type &key) const
    {
        return false;
    }
    bool test_empty(const key_type &empty_key,
                    const key_type &key,
                    const value_type &val) const
    {
        return val.second.getDocId() == 0;
    }
};

template <typename KeyType>
struct UnorderedMapTraits {
    typedef std::tr1::unordered_map<KeyType, matchdoc::MatchDoc> GroupMapType;
};

template <typename KeyType>
struct DenseMapTraits {
    typedef std::pair<const KeyType, matchdoc::MatchDoc> PairValueType;
    typedef std::equal_to<KeyType> EqualToType;
    typedef libc_allocator_with_realloc<PairValueType > Alloc;
    typedef EmptyNoEraseLogic<Alloc,
                              KeyType,
                              PairValueType,
                              EqualToType > EmptyNoEraseLogicTyped;
    typedef dense_hash_map<KeyType, matchdoc::MatchDoc,
                           SPARSEHASH_HASH<KeyType>,
                           EqualToType,
                           Alloc,
                           EmptyNoEraseLogicTyped> GroupMapType;
};

template <typename TT, typename T>
struct MapTransformTraits {
    typedef typename DenseMapTraits<TT>::GroupMapType TargetGroupMapType;
};
template <typename TT, typename T>
struct MapTransformTraits<
    TT, std::tr1::unordered_map<T, matchdoc::MatchDoc> > {
    typedef typename UnorderedMapTraits<TT>::GroupMapType TargetGroupMapType;
};


class GroupKeyWriter {
public:
    GroupKeyWriter() {
        _groupKeyRef = NULL;
    }
    ~GroupKeyWriter() {}
public:
    template <typename KeyType>
    inline void initGroupKeyRef(const matchdoc::MatchDocAllocatorPtr &aggAllocatorPtr) {
        _groupKeyRef =
            aggAllocatorPtr->declareWithConstructFlagDefaultGroup<KeyType>(
                    common::GROUP_KEY_REF,
                    matchdoc::ConstructTypeTraits<KeyType>::NeedConstruct(),
                    SL_CACHE);
    }
    template <typename KeyType>
    inline void setGroupKey(matchdoc::MatchDoc aggMatchDoc, const KeyType &groupKeyValue)
    {
        matchdoc::Reference<KeyType> *groupKeyRef =
            static_cast<matchdoc::Reference<KeyType> *>(_groupKeyRef);
        groupKeyRef->set(aggMatchDoc, groupKeyValue);
    }
    matchdoc::ReferenceBase *getGroupKeyRef() {
        return _groupKeyRef;
    }
private:
    matchdoc::ReferenceBase *_groupKeyRef;
};

template<>
inline void GroupKeyWriter::initGroupKeyRef<autil::MultiChar>(
        const matchdoc::MatchDocAllocatorPtr &aggAllocatorPtr)
{
    _groupKeyRef = aggAllocatorPtr->declare<std::string>(common::GROUP_KEY_REF,
            SL_CACHE);
}

template<>
inline void GroupKeyWriter::setGroupKey<autil::MultiChar>(
        matchdoc::MatchDoc aggMatchDoc,
        const autil::MultiChar &groupKeyValue)
{
    matchdoc::Reference<std::string> *groupKeyRef =
        static_cast<matchdoc::Reference<std::string> *>(_groupKeyRef);
    groupKeyRef->set(aggMatchDoc, autil::StringUtil::toString(groupKeyValue));
}

template <typename KeyType, typename GroupMapType>
class GroupMapInitWrapper {
public:
    GroupMapInitWrapper(GroupMapType *groupMap)
        : _groupMap(groupMap) {
    }
public:
    void initGroupMap() {
    }
private:
    GroupMapType *_groupMap;
};

template<typename KeyType>
class GroupMapInitWrapper <KeyType, typename DenseMapTraits<KeyType>::GroupMapType> {
public:
    typedef typename DenseMapTraits<KeyType>::GroupMapType GroupMapType;
    GroupMapInitWrapper(GroupMapType *groupMap)
        : _groupMap(groupMap) {
    }
public:
    void initGroupMap() {
        std::pair<KeyType, matchdoc::MatchDoc> empty;
        empty.first = 0;
        empty.second.setDocId(0);
        _groupMap->set_empty_key_value(empty);
    }
private:
    GroupMapType *_groupMap;
};

template <typename KeyType, typename ExprType = KeyType,
          typename GroupMapType = typename UnorderedMapTraits<KeyType>::GroupMapType>
class NormalAggregator : public Aggregator
{
public:
    typedef GroupMapType GroupMap;

    typedef typename GroupMap::const_iterator ConstKeyIterator;
public:
    NormalAggregator(suez::turing::AttributeExpressionTyped<ExprType> *attriExpr,
                     uint32_t maxKeyCount,
                     autil::mem_pool::Pool *pool,
                     uint32_t aggThreshold = 0,
                     uint32_t sampleStep = 1,
                     uint32_t maxSortCount = 0,
                     tensorflow::QueryResource *queryResource = NULL);

    ~NormalAggregator();

    void addAggregateFunction(AggregateFunction *fun) override;
    const FunctionVector& getAggregateFunctions() const;
    void setFilter(Filter *filter) override;

    void doAggregate(matchdoc::MatchDoc matchDoc) override;
    void estimateResult(double factor) override;
    void beginSample() override;
    void endLayer(double factor) override;
    uint32_t getAggregateCount() override {
        return _aggSampler->getAggregateCount();
    }
    void updateExprEvaluatedStatus() override {
        // do not set expression evaluated if aggregate sample works.
        if (_aggSampler->getFactor() == 1) {
            _expr->setEvaluated();
        }
    }
    uint32_t getKeyCount() const;
    uint32_t getMaxKeyCount() const;
    matchdoc::MatchDoc getResultMatchDoc(const KeyType &keyValue) const;

    template<typename FunType>
    const matchdoc::Reference<FunType> *getFunResultReference(
            const std::string &funString) const;

    template <typename FunType>
    bool getFunResultOfKey(const std::string &funString,
                           const KeyType &keyValue,
                           FunType &funResult) const;

    common::AggregateResultsPtr collectAggregateResult() override;
    void doCollectAggregateResult(common::AggregateResultPtr &aggResultPtr) override;
    suez::turing::AttributeExpression *getGroupKeyExpr() override;
    suez::turing::AttributeExpression *getAggFunctionExpr(uint index) override;
    matchdoc::ReferenceBase *getAggFunctionRef(uint index, uint id) override;
    matchdoc::ReferenceBase *getGroupKeyRef() override;
    uint32_t getMaxSortCount() override {
        return _maxSortCount;
    }
    std::string getName() override {
        return "NormalAgg";
    }
    ConstKeyIterator begin() const;
    ConstKeyIterator end() const;
    void setGroupKeyStr(const std::string &groupKeyStr) {
        _groupKeyStr = groupKeyStr;
    }
    const std::string &getGroupKeyStr() {
        return _groupKeyStr;
    }
    FunctionVector &getFunVector() {
        return _funVector;
    }
    const matchdoc::MatchDocAllocatorPtr &getAggAllocator() const {
        return _aggMatchDocAllocatorPtr;
    }
    bool getNeedSort() {
        return _needSort;
    }
    const std::string &getFuncString() {
        return _funcString;
    }
    suez::turing::CavaAggModuleInfoPtr codegen() override;
    // for sub group key
    void operator()(matchdoc::MatchDoc matchDoc);
public:
    // for test
    const suez::turing::AttributeExpression *getAttributeExpression() const {
        return _expr;
    }
protected:
    virtual KeyType getGroupKeyValue(const KeyType &value) const;
    void doAggregate(const KeyType &value, matchdoc::MatchDoc matchDoc);
    virtual void initGroupKeyRef() {
        _groupKeyWriter.initGroupKeyRef<KeyType>(_aggMatchDocAllocatorPtr);
    }
    virtual void setGroupKey(matchdoc::MatchDoc aggMatchDoc, const KeyType &groupKeyValue) {
        _groupKeyWriter.setGroupKey<KeyType>(aggMatchDoc, groupKeyValue);
    }
    // This function is used by Aggregator with the same type between KeyType and ExprType
    void doAggregateWrapper(const KeyType &groupKeyValue, matchdoc::MatchDoc matchDoc);
    // This function is used by Aggregator with MultiValueTyped group key;
    void doAggregateWrapper(const autil::MultiValueType<KeyType> &groupKeyValue,
                            matchdoc::MatchDoc matchDoc);
    void aggregateOneDoc(matchdoc::MatchDoc matchDoc);
    void doAggregateOneDoc(matchdoc::MatchDoc matchDoc);
private:
    void initFunctions(matchdoc::MatchDoc aggMatchDoc);
    template<typename IteratorType>
    void collectAggFunResults(common::AggregateResultPtr aggResultPtr,
                              IteratorType begin, IteratorType end);
    void collectAggFunResultsWithCut(common::AggregateResultPtr aggResultPtr);

    bool constructCavaCode(std::string &cavaCode);
    bool genJitAggregator(std::string &cavaCode);
    bool genJitAggResultFunc(std::string &cavaCode);
    bool genJitAggBatchFunc(std::string &cavaCode);
    bool genJitAggCreateFunc(std::string &cavaCode);
    bool genJitAggCountFunc(std::string &cavaCode);
    bool genJitAggFields(std::string &cavaCode);
    bool genAggItemClass(std::string &cavaCode);
    bool canCodegen();
    bool getCavaType(std::string &output, VariableType type, bool isMulti);
    std::string getCavaAggRefName(AggregateFunction* func, const std::string &idxStr);
    std::string getCavaAggExprName(AggregateFunction* func, const std::string &idxStr);
    std::string getCavaAggItemField(AggregateFunction* func, const std::string &idxStr);
    std::string getIdxStr(uint idx);
    std::string getCavaAggItemSize();

    bool getCavaAggExprGetFunc(std::string &output, VariableType type, bool isMulti);
    bool getCavaAggRefSetFunc(std::string &output, matchdoc::ValueType valueType);
    bool getCavaAggItemMapType(std::string &output, VariableType type);
    bool getCavaAggMinStr(std::string &output, matchdoc::ValueType valueType);
    bool getCavaAggMaxStr(std::string &output, matchdoc::ValueType valueType);

protected:
    uint32_t _maxKeyCount;
    FunctionVector _funVector;
    GroupMap _groupMap;
    matchdoc::MatchDocAllocatorPtr _aggMatchDocAllocatorPtr;
    Filter* _filter;
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;

    AggregateSampler* _aggSampler;
    std::string _funcString;
    bool _needSort;
    uint32_t _maxSortCount;
    uint32_t _maxKeyCountOnCount;
    GroupKeyWriter _groupKeyWriter;
    std::string _groupKeyStr;

    tensorflow::QueryResource *_queryResource;
    suez::turing::CavaJitWrapper *_cavaJitWrapper;
    suez::turing::Tracer *_tracer;
    std::string _cavaCode;
public:
    struct VecValueType {
        KeyType first;
        matchdoc::MatchDoc second;
        int64_t count;
        VecValueType() {}
        template<typename T>
        VecValueType(const T &v)
            : first(v.first), second(v.second) {}
        bool operator < (const VecValueType &rhs) const {
            return count > rhs.count;
        }
    };
private:
    friend class NormalAggregatorTest;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE_3(search, NormalAggregator, GroupMapType, KeyType, ExprType);

template<bool needInitEmpty> void initEmptyGroupMap() {
}


template<typename KeyType, typename ExprType, typename GroupMapType>
NormalAggregator<KeyType, ExprType, GroupMapType>::NormalAggregator(
        suez::turing::AttributeExpressionTyped<ExprType> *attriExpr,
        uint32_t maxKeyCount,
        autil::mem_pool::Pool *pool,
        uint32_t aggThreshold,
        uint32_t sampleStep,
        uint32_t maxSortCount,
        tensorflow::QueryResource *queryResource)
    : Aggregator(pool)
    , _queryResource(queryResource)
    , _cavaJitWrapper(queryResource ? queryResource->getCavaJitWrapper() : NULL)
    , _tracer(queryResource ? queryResource->getTracer() : NULL)
{
    _filter = NULL;
    _expr = attriExpr;
    if (_expr) {
        _groupKeyStr = _expr->getOriginalString();
    }
    _maxKeyCount = maxKeyCount;
    _maxSortCount = maxKeyCount;
    _aggMatchDocAllocatorPtr.reset(new matchdoc::MatchDocAllocator(_pool));
    _aggSampler = POOL_NEW_CLASS(_pool, AggregateSampler, aggThreshold, sampleStep);
    _funcString = "";
    _needSort = false;
    if (maxSortCount == 0) {
        _maxKeyCountOnCount = maxKeyCount;
    } else {
        _maxKeyCountOnCount = maxSortCount;
    }
    GroupMapInitWrapper<KeyType, GroupMapType> initWrapper(&_groupMap);
    initWrapper.initGroupMap();
}

template<typename KeyType, typename ExprType, typename GroupMapType>
NormalAggregator<KeyType, ExprType, GroupMapType>::~NormalAggregator()  {
    for (typename GroupMap::iterator it = _groupMap.begin();
         it != _groupMap.end(); it++)
    {
        _aggMatchDocAllocatorPtr->deallocate(it->second);
    }
    _groupMap.clear();

    for(FunctionVector::iterator funIt = _funVector.begin();
        funIt != _funVector.end(); funIt++)
    {
        POOL_DELETE_CLASS(*funIt);
    }
    _funVector.clear();

    POOL_DELETE_CLASS(_aggSampler);
    if (_filter != NULL) {
        POOL_DELETE_CLASS(_filter);
        _filter = NULL;
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::setFilter(Filter *filter) {
    _filter = filter;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
typename NormalAggregator<KeyType, ExprType, GroupMapType>::ConstKeyIterator
NormalAggregator<KeyType, ExprType, GroupMapType>::begin() const {
    return _groupMap.begin();
}

template<typename KeyType, typename ExprType, typename GroupMapType>
typename NormalAggregator<KeyType, ExprType, GroupMapType>::ConstKeyIterator
NormalAggregator<KeyType, ExprType, GroupMapType>::end() const {
    return _groupMap.end();
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::addAggregateFunction(AggregateFunction *fun)
{
    initGroupKeyRef();
    fun->declareResultReference(_aggMatchDocAllocatorPtr.get());
    _funVector.push_back(fun);
    if (fun->getFunctionName() == "count") {
        _needSort = true;
        _funcString = fun->toString();
        _maxSortCount = _maxKeyCountOnCount;
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
const typename NormalAggregator<KeyType, ExprType, GroupMapType>::FunctionVector&
NormalAggregator<KeyType, ExprType, GroupMapType>::getAggregateFunctions() const {
    return _funVector;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::initFunctions(matchdoc::MatchDoc aggMatchDoc) {
    for(FunctionVector::iterator it = _funVector.begin();
        it != _funVector.end(); it ++)
    {
        (*it)->initFunction(aggMatchDoc, _pool);
    }

}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::doAggregate(matchdoc::MatchDoc matchDoc) {
    if (_aggSampler->sampling()) {
        aggregateOneDoc(matchDoc);
    } else if (_aggSampler->isBeginOfSample()) {
        beginSample();
    }
}

template <typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::aggregateOneDoc(
        matchdoc::MatchDoc matchDoc)
{
    if (!_expr->isSubExpression()) {
        doAggregateOneDoc(matchDoc);
    } else {
        assert(this->_matchDocAllocator != NULL);
        matchdoc::SubDocAccessor *accessor =
            this->_matchDocAllocator->getSubDocAccessor();
        assert(accessor != NULL);
        accessor->foreach(matchDoc, *this);
    }
}

template <typename KeyType, typename ExprType, typename GroupMapType>
inline void NormalAggregator<KeyType, ExprType, GroupMapType>::doAggregateOneDoc(
        matchdoc::MatchDoc matchDoc)
{
    if (_filter && !_filter->pass(matchDoc)) {
        return;
    }
    const ExprType &groupExprValue = _expr->evaluateAndReturn(matchDoc);
    doAggregateWrapper(groupExprValue, matchDoc);
}

template <typename KeyType, typename ExprType, typename GroupMapType>
inline void NormalAggregator<KeyType, ExprType, GroupMapType>::operator()(
        matchdoc::MatchDoc matchDoc)
{
    doAggregateOneDoc(matchDoc);
}

template <typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::
doAggregateWrapper(const KeyType &groupKeyValue, matchdoc::MatchDoc matchDoc)
{
    doAggregate(groupKeyValue, matchDoc);
}

template <typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::
doAggregateWrapper(const autil::MultiValueType<KeyType> &groupKeyValue,
                   matchdoc::MatchDoc matchDoc)
{
    for (uint32_t i = 0; i < groupKeyValue.size(); i++) {
        doAggregate(groupKeyValue[i], matchDoc);
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::doAggregate(const KeyType &value,
        matchdoc::MatchDoc matchDoc)
{
    //find the aggSlab in map
    matchdoc::MatchDoc aggMatchDoc;
    KeyType keyValue = getGroupKeyValue(value);
    typename GroupMap::iterator keyIt = _groupMap.find(keyValue);
    if (keyIt == _groupMap.end()) {
        if (_groupMap.size() >= _maxSortCount) {
            return;
        }
        aggMatchDoc = _aggMatchDocAllocatorPtr->allocate();
        setGroupKey(aggMatchDoc, keyValue);
        initFunctions(aggMatchDoc);
        _groupMap.insert(std::make_pair(keyValue, aggMatchDoc));
    } else {
        aggMatchDoc = keyIt->second;
    }

    //execute the aggreation function
    for(FunctionVector::iterator funIt = _funVector.begin();
        funIt != _funVector.end(); funIt ++)
    {
        (*funIt)->calculate(matchDoc, aggMatchDoc);
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
template<typename FunType>
const matchdoc::Reference<FunType> *NormalAggregator<KeyType, ExprType, GroupMapType>::
getFunResultReference(const std::string &funString) const
{
    for(FunctionVector::const_iterator funIt = _funVector.begin();
        funIt != _funVector.end(); funIt ++)
    {
        if ((*funIt)->toString() == funString) {
            return dynamic_cast<const matchdoc::Reference<FunType> *>(
                    (*funIt)->getReference());
        }
    }
    return NULL;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
uint32_t NormalAggregator<KeyType, ExprType, GroupMapType>::getKeyCount() const {
    return _groupMap.size();
}

template<typename KeyType, typename ExprType, typename GroupMapType>
matchdoc::MatchDoc NormalAggregator<KeyType, ExprType, GroupMapType>::getResultMatchDoc(
        const KeyType &keyValue) const
{
    KeyType keyValueTemp = getGroupKeyValue(keyValue);
    typename GroupMap::const_iterator it = _groupMap.find(keyValueTemp);
    if (it != _groupMap.end()) {
        return it->second;
    }
    return matchdoc::INVALID_MATCHDOC;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
uint32_t NormalAggregator<KeyType, ExprType, GroupMapType>::getMaxKeyCount() const {
    return _maxKeyCount;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::doCollectAggregateResult(
        common::AggregateResultPtr &aggResultPtr)
{
    if (_needSort && _groupMap.size() > _maxKeyCount) {
        collectAggFunResultsWithCut(aggResultPtr);
    } else {
        collectAggFunResults(aggResultPtr, _groupMap.begin(), _groupMap.end());
    }
    _groupMap.clear();
}

template<typename KeyType, typename ExprType, typename GroupMapType>
common::AggregateResultsPtr NormalAggregator<KeyType, ExprType, GroupMapType>::collectAggregateResult() {
    common::AggregateResultPtr aggResultPtr(new common::AggregateResult());
    aggResultPtr->setGroupExprStr(_groupKeyStr);

    for(FunctionVector::iterator funIt = _funVector.begin();
        funIt != _funVector.end(); funIt++)
    {
        (*funIt)->getReference()->setSerializeLevel(SL_CACHE);
        aggResultPtr->addAggFunName((*funIt)->getFunctionName());
        aggResultPtr->addAggFunParameter((*funIt)->getParameter());
    }
    aggResultPtr->setMatchDocAllocator(_aggMatchDocAllocatorPtr);
    doCollectAggregateResult(aggResultPtr);
    if (_aggMatchDocAllocatorPtr) {
        _aggMatchDocAllocatorPtr->extend();
    }
    common::AggregateResultsPtr aggResultsPtr(new common::AggregateResults());
    aggResultsPtr->push_back(aggResultPtr);
    return aggResultsPtr;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::collectAggFunResultsWithCut(
        common::AggregateResultPtr aggResultPtr)
{
    std::vector<VecValueType> vec(_groupMap.begin(), _groupMap.end());
    const matchdoc::Reference<int64_t> *refer =
        getFunResultReference<int64_t>(_funcString);
    assert(refer);
    for (size_t i = 0; i < vec.size(); ++i) {
        vec[i].count = refer->get(vec[i].second);
    }
    assert(vec.size() > _maxKeyCount);
    std::nth_element(vec.begin(), vec.begin() + _maxKeyCount - 1, vec.end());
    collectAggFunResults(aggResultPtr, vec.begin(), vec.begin() + _maxKeyCount);
    for (size_t i = _maxKeyCount; i < vec.size(); ++i) {
        _aggMatchDocAllocatorPtr->deallocate(vec[i].second);
    }
}
template<typename KeyType, typename ExprType, typename GroupMapType>
KeyType NormalAggregator<KeyType, ExprType, GroupMapType>::getGroupKeyValue(const KeyType &value) const {
    return value;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
template<typename IteratorType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::collectAggFunResults(
        common::AggregateResultPtr aggResultPtr,
        IteratorType begin, IteratorType end)
{
    for (IteratorType groupIt = begin; groupIt != end; ++groupIt) {
        aggResultPtr->addAggFunResults((*groupIt).second);
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::estimateResult(double factor) {
    if (factor == 1.0) {
        return;
    }
    for (typename GroupMap::const_iterator groupIt = _groupMap.begin();
         groupIt != _groupMap.end(); groupIt ++)
    {
        // estimate the function result value after sample
        for (FunctionVector::const_iterator funIt = _funVector.begin();
             funIt != _funVector.end(); funIt ++)
        {
            (*funIt)->estimateResult(factor, (*groupIt).second);
        }
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::beginSample() {
    for (typename GroupMap::const_iterator groupIt = _groupMap.begin();
         groupIt != _groupMap.end(); groupIt ++)
    {
        // estimate the function result value after sample
        for (FunctionVector::const_iterator funIt = _funVector.begin();
             funIt != _funVector.end(); funIt ++)
        {
            (*funIt)->beginSample((*groupIt).second);
        }
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void NormalAggregator<KeyType, ExprType, GroupMapType>::endLayer(double factor) {
    for (typename GroupMap::const_iterator groupIt = _groupMap.begin();
         groupIt != _groupMap.end(); groupIt ++)
    {
        // estimate the function result value after sample
        for (FunctionVector::const_iterator funIt = _funVector.begin();
             funIt != _funVector.end(); funIt ++)
        {
            (*funIt)->endLayer(_aggSampler->getFactor(), factor, (*groupIt).second);
        }
    }
}

template <typename KeyType, typename ExprType, typename GroupMapType>
template <typename FunType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getFunResultOfKey(const std::string &funString,
        const KeyType &keyValue, FunType &funResult) const
{
    auto refer = getFunResultReference<FunType>(funString);
    if (refer == NULL) {
        return false;
    }
    auto aggMatchDoc = getResultMatchDoc(keyValue);
    if(matchdoc::INVALID_MATCHDOC != aggMatchDoc) {
        funResult = refer->get(aggMatchDoc);
        return true;
    }
    return false;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
suez::turing::AttributeExpression *NormalAggregator<KeyType, ExprType, GroupMapType>::getGroupKeyExpr() {
    return _expr;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
suez::turing::AttributeExpression *NormalAggregator<KeyType, ExprType, GroupMapType>::getAggFunctionExpr(
        uint index)
{
    if (_funVector.size() <= index) {
        return NULL;
    }
    return _funVector[index]->getExpr();
}

template <typename KeyType, typename ExprType, typename GroupMapType>
matchdoc::ReferenceBase *NormalAggregator<KeyType, ExprType, GroupMapType>::getAggFunctionRef(uint index, uint id) {
    if (_funVector.size() <= index) {
        return NULL;
    }
    return _funVector[index]->getReference(id);
}

template <typename KeyType, typename ExprType, typename GroupMapType>
matchdoc::ReferenceBase *NormalAggregator<KeyType, ExprType, GroupMapType>::getGroupKeyRef() {
    return _groupKeyWriter.getGroupKeyRef();
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::canCodegen() {
    if (_queryResource == NULL || _cavaJitWrapper == NULL) {
        return false;
    }
    if (_aggSampler->getSampleStep() != 1) {
        return false;
    }
    if (_filter != NULL) {
        return false;
    }
    if (_expr->isSubExpression()) {
        return false;
    }
    for (uint i = 0; i < _funVector.size(); i++) {
        if (!_funVector[i]->canCodegen()) {
            return false;
        }
    }
    return true;
}

#define STRING_SWITCH_CACHE(funcStr, name, idxStr)      \
    if (funcName == #funcStr) {                         \
        switch (idxStr[0]) {                            \
        case '0': return funcStr name "0";              \
        case '1': return funcStr name "1";              \
        case '2': return funcStr name "2";              \
        case '3': return funcStr name "3";              \
        default: return funcName + name + idxStr;       \
        }                                               \
    }

#define ALL_AGG_FUNC_MACRO(macro, name, idxStr) \
    macro("count", name, idxStr)                \
    macro("sum", name, idxStr)                  \
    macro("min", name, idxStr)                  \
    macro("max", name, idxStr)

template<typename KeyType, typename ExprType, typename GroupMapType>
std::string NormalAggregator<KeyType, ExprType, GroupMapType>::getIdxStr(uint idx) {
    switch (idx) {
    case 0 : return "0";
    case 1 : return "1";
    case 2 : return "2";
    case 3 : return "3";
    case 4 : return "4";
    default: return std::to_string(idx);
    }
}

template<typename KeyType, typename ExprType, typename GroupMapType>
std::string NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggItemSize() {
    uint size = _funVector.size() * 8 + 8;
    switch (size) {
    case 16: return "16";
    case 24: return "24";
    case 32: return "32";
    case 40: return "40";
    default: return std::to_string(size);
    }
}


template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaType(std::string &output,
                                                      VariableType type, bool isMulti)
{
#define TYPE_CASE(type, singleType, multiType)  \
    case type: {                                \
        if (isMulti) {                          \
            output = multiType;                 \
        } else {                                \
            output = singleType;                \
        }                                       \
        return true;                            \
    }

    switch (type) {
        TYPE_CASE(vt_int8, "byte", "MInt8");
        TYPE_CASE(vt_int16, "short", "MInt16");
        TYPE_CASE(vt_int32, "int", "MInt32");
        TYPE_CASE(vt_int64, "long", "MInt64");
        TYPE_CASE(vt_uint8, "ubyte", "MUInt8");
        TYPE_CASE(vt_uint16, "ushort", "MUInt16");
        TYPE_CASE(vt_uint32, "uint", "MUInt32");
        TYPE_CASE(vt_uint64, "ulong", "MUInt64");
        TYPE_CASE(vt_float, "float", "MFloat");
        TYPE_CASE(vt_double, "double", "MDouble");
        TYPE_CASE(vt_string, "MChar", "MString");
    default:
        return false;
    }
#undef TYPE_CASE
    return false;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
std::string NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggRefName(
        AggregateFunction* func, const std::string &idxStr)
{
    std::string funcName = func->getFunctionName();
    if (idxStr.size() == 1) {
        // fast path
        ALL_AGG_FUNC_MACRO(STRING_SWITCH_CACHE, "Ref", idxStr);
    }
    // slow path
    std::string ret = funcName;
    ret += "Ref";
    ret += idxStr;
    return ret;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
std::string NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggExprName(
        AggregateFunction* func, const std::string &idxStr)
{
    std::string funcName = func->getFunctionName();
    if (idxStr.size() == 1) {
        ALL_AGG_FUNC_MACRO(STRING_SWITCH_CACHE, "Expr", idxStr);
    }
    // slow path
    std::string ret = funcName;
    ret += "Expr";
    ret += idxStr;
    return ret;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggExprGetFunc(
        std::string &output, VariableType type, bool isMulti)
{
#define TYPE_CASE(type, singleType, multiType)  \
    case type: {                                \
        if (isMulti) {                          \
            output = multiType;                 \
        } else {                                \
            output = singleType;                \
        }                                       \
        return true;                            \
    }

    switch (type) {
        TYPE_CASE(vt_int8, "getInt8", "getMInt8");
        TYPE_CASE(vt_int16, "getInt16", "getMInt16");
        TYPE_CASE(vt_int32, "getInt32", "getMInt32");
        TYPE_CASE(vt_int64, "getInt64", "getMInt64");
        TYPE_CASE(vt_uint8, "getUInt8", "getMUInt8");
        TYPE_CASE(vt_uint16, "getUInt16", "getMUInt16");
        TYPE_CASE(vt_uint32, "getUInt32", "getMUInt32");
        TYPE_CASE(vt_uint64, "getUInt64", "getMUInt64");
        TYPE_CASE(vt_float, "getFloat", "getMFloat");
        TYPE_CASE(vt_double, "getDouble", "getMDouble");
        TYPE_CASE(vt_string, "getMChar", "getMString");
    default:
        return false;
    }
#undef TYPE_CASE
    return false;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
std::string NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggItemField(
        AggregateFunction *func, const std::string &idxStr)
{

    std::string funcName = func->getFunctionName();
    if (idxStr.size() == 1) {
        ALL_AGG_FUNC_MACRO(STRING_SWITCH_CACHE, "", idxStr);
    }
    return funcName + idxStr;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggRefSetFunc(
        std::string &output, matchdoc::ValueType valueType)
{
#define TYPE_CASE(type, singleType, multiType)  \
    case type: {                                \
        if (valueType.isMultiValue()) {         \
            return false;                       \
        } else {                                \
            output = singleType;                \
        }                                       \
        return true;                            \
    }

    if (!valueType.isBuiltInType()) {
        return false;
    }
    switch (valueType.getBuiltinType()) {
        TYPE_CASE(vt_int8, "setInt8", "setMInt8")
        TYPE_CASE(vt_int16, "setInt16", "setMInt16")
        TYPE_CASE(vt_int32, "setInt32", "setMInt32")
        TYPE_CASE(vt_int64, "setInt64", "setMInt64")
        TYPE_CASE(vt_uint8, "setUInt8", "setUMInt8")
        TYPE_CASE(vt_uint16, "setUInt16", "setUMInt16")
        TYPE_CASE(vt_uint32, "setUInt32", "setUMInt32")
        TYPE_CASE(vt_uint64, "setUInt64", "setUMInt64")
        TYPE_CASE(vt_float, "setFloat", "setMFloat");
        TYPE_CASE(vt_double, "setDouble", "setMDouble")
    case vt_string: {
            if (valueType.isMultiValue()) {
                return false;
            }
            if (valueType.isStdType()) {
                output = "setSTLString";
            } else {
                output = "setMChar";
            }
            return true;
        }
    default:
        return false;
    }
#undef TYPE_CASE
    return false;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggItemMapType(std::string &output,
        VariableType type)
{
#define TYPE_CASE(type, aggItemMapType)         \
    case type: {                                \
        output = aggItemMapType;                \
        return true;                            \
    }

    switch (type) {
        TYPE_CASE(vt_int8, "ByteAggItemMap");
        TYPE_CASE(vt_int16, "ShortAggItemMap");
        TYPE_CASE(vt_int32, "IntAggItemMap");
        TYPE_CASE(vt_int64, "LongAggItemMap");
        TYPE_CASE(vt_uint8, "UByteAggItemMap");
        TYPE_CASE(vt_uint16, "UShortAggItemMap");
        TYPE_CASE(vt_uint32, "UIntAggItemMap");
        TYPE_CASE(vt_uint64, "ULongAggItemMap");
        TYPE_CASE(vt_float, "FloatAggItemMap");
        TYPE_CASE(vt_double, "DoubleAggItemMap");
        TYPE_CASE(vt_string, "MCharAggItemMap");
    default:
        return false;
    }
#undef TYPE_CASE
    return false;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggMinStr(std::string &output,
                                                           matchdoc::ValueType valueType)
{
    if (!valueType.isBuiltInType()) {
        return false;
    }
    switch (valueType.getBuiltinType()) {
    case vt_int8: output = "-128"; return true;
    case vt_int16: output = "-32768"; return true;
    case vt_int32: output = "-2147483648"; return true;
    case vt_int64: output = "-(1L<<63)"; return true;
    case vt_uint8: output = "0"; return true;
    case vt_uint16: output = "0"; return true;
    case vt_uint32: output = "0"; return true;
    case vt_uint64: output = "0"; return true;
    case vt_float: output = "-340282346638528859811704183484516925440.000000F"; return true;
    case vt_double: output = "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000D"; return true;
    default:
        return false;
    }
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::getCavaAggMaxStr(std::string &output,
                                                           matchdoc::ValueType valueType)
{
    if (!valueType.isBuiltInType()) {
        return false;
    }
    switch (valueType.getBuiltinType()) {
    case vt_int8: output = "127"; return true;
    case vt_int16: output = "32767"; return true;
    case vt_int32: output = "2147483647"; return true;
    case vt_int64: output = "9223372036854775807L"; return true;
    case vt_uint8: output = "255"; return true;
    case vt_uint16: output = "65535"; return true;
    case vt_uint32: output = "-1"; return true;
    case vt_uint64: output = "-1L"; return true;
    case vt_float: output = "340282346638528859811704183484516925440.000000F"; return true;
    case vt_double: output = "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000D"; return true;
    default:
        return false;
    }
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genAggItemClass(std::string &cavaCode) {
    cavaCode += "class AggItem {\n";
    for (uint i = 0; i < _funVector.size(); ++i) {
        auto funcRef = _funVector[i]->getReference();
        if (!funcRef) {
            return false;
        }
        auto valueType = funcRef->getValueType();
        if (valueType.isMultiValue() || !valueType.isBuiltInType()) {
            return false;
        }
        std::string typeStr;
        if (!getCavaType(typeStr, valueType.getBuiltinType(), false)) {
            return false;
        }
        const std::string &idxStr = getIdxStr(i);
        cavaCode += "    ";
        cavaCode += typeStr;
        cavaCode += " ";
        cavaCode += getCavaAggItemField(_funVector[i], idxStr);
        cavaCode += ";\n"; // long count0;
    }
    // get groupKey
    std::string groupKeyType;
    if (!getCavaType(groupKeyType, _expr->getType(), false)) {
        return false;
    }

    cavaCode += "    ";
    cavaCode += groupKeyType;
    cavaCode += " groupKey;\n"; // long groupKey;
    cavaCode += "};\n";
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genJitAggFields(std::string &cavaCode) {
    cavaCode = R"(
    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
)";
    // public IntAggItemMap itemMap;
    std::string aggItemMapType;
    if (!getCavaAggItemMapType(aggItemMapType, _expr->getType())) {
        return false;
    }
    cavaCode += "    public ";
    cavaCode += aggItemMapType;
    cavaCode += " itemMap;\n";
    // gen RefList
    cavaCode += "    public Reference groupKeyRef;\n";
    for (uint i = 0; i < _funVector.size(); ++i) {
        // public Reference countRef0;
        const std::string &idxStr = getIdxStr(i);
        cavaCode += "    public Reference ";
        cavaCode += getCavaAggRefName(_funVector[i], idxStr);
        cavaCode += ";\n";
    }
    // gen ExprList
    for (uint i = 0; i < _funVector.size(); ++i) {
        //     public AttributeExpression sumExpr1;
        auto countFunc = dynamic_cast<CountAggregateFunction *>(_funVector[i]);
        if (countFunc) {
            continue;
        }
        const std::string &idxStr = getIdxStr(i);
        cavaCode += "    public AttributeExpression ";
        cavaCode += getCavaAggExprName(_funVector[i], idxStr);
        cavaCode += ";\n";
    }
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genJitAggCountFunc(std::string &cavaCode) {
    cavaCode = R"(
    public uint count() {
        return itemMap.size();
    }
)";
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genJitAggCreateFunc(std::string &cavaCode) {
    cavaCode = R"(
    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
)";

    cavaCode += "        jitAggregator.allocator = AggItemAllocator.create((uint)";
    cavaCode += getCavaAggItemSize();
    cavaCode += ");\n";

    std::string aggItemMapType;
    if (!getCavaAggItemMapType(aggItemMapType, _expr->getType())) {
        return false;
    }

    cavaCode += "        jitAggregator.itemMap = ";
    cavaCode += aggItemMapType;
    cavaCode += ".create();\n"; // jitAggregator.itemMap = IntAggItemMap.create();
    // create ref
    for (uint i = 0; i < _funVector.size(); ++i) {
        // jitAggregator.countRef0 = aggregator.getAggFunctionRef(0, 0);
        const std::string &idxStr = getIdxStr(i);
        const std::string &refName = getCavaAggRefName(_funVector[i], idxStr); // sumRef0;
        cavaCode += "        jitAggregator.";
        cavaCode += refName;
        cavaCode += " = aggregator.getAggFunctionRef((uint)" + idxStr + ", (uint)0);\n";
    }
    // create expr
    for (uint i = 0; i < _funVector.size(); ++i) {
        const std::string &idxStr = getIdxStr(i);
        const std::string &exprName = getCavaAggExprName(_funVector[i], idxStr);
        auto countFunc = dynamic_cast<CountAggregateFunction *>(_funVector[i]); //
        if (countFunc) {
            continue;
        }
        cavaCode += "        jitAggregator.";
        cavaCode += exprName;
        cavaCode += " = aggregator.getAggFunctionExpr((uint)";
        cavaCode += idxStr;
        cavaCode += ");\n"; // jitAggregator.sumExpr1 = aggregator.getAggFunctionExpr(1);
    }
    cavaCode += R"(        return jitAggregator;
    }
)";
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genJitAggBatchFunc(std::string &cavaCode) {
    cavaCode = R"(
    public void batch(MatchDocs docs, uint size) {
)";

    // get groupKey
    std::string groupExprType;
    if (!getCavaType(groupExprType, _expr->getType(), _expr->isMultiValue())) {
        return false;
    }
    std::string groupKeyType;
    if (!getCavaType(groupKeyType, _expr->getType(), false)) {
        return false;
    }

    if (_expr->isMultiValue()) {
        cavaCode += "        ";
        cavaCode += groupExprType;
        cavaCode += " groupKey = null;\n"; // MInt64 groupKey = null;
    }
    cavaCode += R"(        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
)";
    std::string exprGetFuncName;
    if (!getCavaAggExprGetFunc(exprGetFuncName,
                               _expr->getType(), _expr->isMultiValue()))
    {
        return false;
    }
    cavaCode += "            ";
    if (_expr->isMultiValue()) {
        cavaCode += "groupKey = groupKeyExpr.";
        cavaCode += exprGetFuncName;
        cavaCode += "(doc, groupKey);\n"; // MInt64 groupKey = groupKeyExpr.getMInt64(doc, groupKey);
        cavaCode += "            for (uint i = 0; i < groupKey.size(); i++) {\n";
        cavaCode += "                ";
        cavaCode += groupKeyType;
        cavaCode += " key = groupKey.getWithoutCheck(i);\n"; // long key = groupkey.getWithoutCheck(i);
        cavaCode += R"(                AggItem item = (AggItem)itemMap.get(key);
                if (item == null) {
                    if (itemMap.size() >= maxSortCount) {
                        continue;
                    }
                    item = (AggItem)allocator.alloc();
                    item.groupKey = key;
)";

    } else {
        cavaCode += groupKeyType;
        cavaCode += " key = groupKeyExpr.";
        cavaCode += exprGetFuncName;
        cavaCode += "(doc);\n"; // long key = groupKeyExpr.getInt64(doc);
        // cavaCode += "System.println(key);\n";
        cavaCode += R"(            AggItem item = (AggItem)itemMap.get(key);
            if (item == null) {
                if (itemMap.size() >= maxSortCount) {
                    continue;
                }
                item = (AggItem)allocator.alloc();
                item.groupKey = key;
)";
    }

    for (uint i = 0; i < _funVector.size(); ++i) {
        const std::string &idxStr = getIdxStr(i);
        const std::string &aggItemField = getCavaAggItemField(_funVector[i], idxStr);
        if (_expr->isMultiValue()) {
            cavaCode += "    ";
        }
        auto funcRef = _funVector[i]->getReference();
        if (!funcRef) {
            return false;
        }
        cavaCode += "                item.";
        cavaCode += aggItemField;
        if (_funVector[i]->getFunctionName() == "count" ||
            _funVector[i]->getFunctionName() == "sum")
        {
            cavaCode += " = 0;\n"; // item.count0 = 0;
        } else if (_funVector[i]->getFunctionName() == "max") {
            cavaCode += " = ";
            std::string minValueStr;
            if (!getCavaAggMinStr(minValueStr, funcRef->getValueType())) {
                return false;
            }
            cavaCode += minValueStr;
            cavaCode += ";\n"; // item.min0 = xxx;
        } else if (_funVector[i]->getFunctionName() == "min") {
            cavaCode += " = ";
            std::string maxValueStr;
            if (!getCavaAggMaxStr(maxValueStr, funcRef->getValueType())) {
                return false;
            }
            cavaCode += maxValueStr;
            cavaCode += ";\n"; // item.max0 = xxx;
        } else {
            return false;
        }
    }
    if (_expr->isMultiValue()) {
        cavaCode += "    ";
    }
    cavaCode += "                itemMap.add(key, (Any)item);\n";
    // end if (item == null) {
    if (_expr->isMultiValue()) {
        cavaCode += "    ";
    }
    cavaCode += "            }\n";

    // expr eval
    for (uint i = 0; i < _funVector.size(); ++i) {
        const std::string &idxStr = getIdxStr(i);
        const std::string &aggItemField = getCavaAggItemField(_funVector[i], idxStr);
        const std::string &aggExprName = getCavaAggExprName(_funVector[i], idxStr);
        if (_funVector[i]->getFunctionName() == "count") {
            continue;
        } else {
            auto funcExpr = _funVector[i]->getExpr();
            if (funcExpr->isMultiValue()) {
                return false;
            }
            std::string funcExprType;
            if (!getCavaType(funcExprType, funcExpr->getType(), false)) {
                return false;
            }
            std::string exprGetFuncName;
            if (!getCavaAggExprGetFunc(exprGetFuncName,
                            funcExpr->getType(), funcExpr->isMultiValue()))
            {
                return false;
            }
            if (_expr->isMultiValue()) {
                cavaCode += "    ";
            }
            cavaCode += "            ";
            cavaCode += funcExprType;
            cavaCode += " ";
            cavaCode += aggItemField;
            cavaCode += " = ";
            cavaCode += aggExprName;
            cavaCode += ".";
            cavaCode += exprGetFuncName;
            cavaCode += "(doc);\n"; // int sum1 =  sumExpr1.getInt32(doc);
        }
    }
    for (uint i = 0; i < _funVector.size(); ++i) {
        const std::string &idxStr = getIdxStr(i);
        const std::string &aggItemField = getCavaAggItemField(_funVector[i], idxStr);
        if (_funVector[i]->getFunctionName() == "count") {
            if (_expr->isMultiValue()) {
                cavaCode += "    ";
            }
            cavaCode += "            item.";
            cavaCode += aggItemField;
            cavaCode += " += 1;\n"; // item.count0 += 1;
            // cavaCode += "System.print((int)item.groupKey); System.print(\"---\"); ";
            // cavaCode += "System.print(item.count0); System.print(\" \"); ";
            // cavaCode += "System.println(i); ";
        } else {
            if (_funVector[i]->getFunctionName() == "sum") {
                if (_expr->isMultiValue()) {
                    cavaCode += "    ";
                }
                cavaCode += "            item.";
                cavaCode += aggItemField;
                cavaCode += " += ";
                cavaCode += aggItemField;
                cavaCode += ";\n"; // item.sum1 += sum1;
            } else if (_funVector[i]->getFunctionName() == "min") {
                if (_expr->isMultiValue()) {
                    cavaCode += "    ";
                }
                cavaCode += "            if (";
                cavaCode += aggItemField;
                cavaCode += " < item.";
                cavaCode += aggItemField;
                cavaCode += ") item.";
                cavaCode += aggItemField;
                cavaCode += " = ";
                cavaCode += aggItemField;
                cavaCode += ";\n"; // if (min2 < item.min2) item.min2 = min2;
                // cavaCode += "System.print((int)item.groupKey); System.print(\" \"); ";
                // cavaCode += "System.print(min0); System.print(\" \"); ";
                // cavaCode += "System.print(item.min0); System.print(\" \"); ";
                // cavaCode += "System.println(\" \"); ";
            } else if (_funVector[i]->getFunctionName() == "max") {
                if (_expr->isMultiValue()) {
                    cavaCode += "    ";
                }
                cavaCode += "            if (";
                cavaCode += aggItemField;
                cavaCode += " > item.";
                cavaCode += aggItemField;
                cavaCode += ") item.";
                cavaCode += aggItemField;
                cavaCode += " = ";
                cavaCode += aggItemField;
                cavaCode += ";\n"; // if (max2 < item.max2) item.max2 = max2;
            }
        }
    }
    // end for (uint i = 0; i < groupKey.size(); i++) {
    if (_expr->isMultiValue()) {
        cavaCode += "            }\n";
    }
    // end for (uint i = 0; i < docs.size(); ++i) {
    cavaCode += "        }\n";
    // batch func end
    cavaCode += "    }\n";
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genJitAggResultFunc(std::string &cavaCode) {
    cavaCode = R"(
    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
)";
    for (uint i = 0; i < _funVector.size(); ++i) {
        const std::string &idxStr = getIdxStr(i);
        auto funcRef = _funVector[i]->getReference();
        if (!funcRef) {
            return false;
        }
        std::string refSetFunc;
        if (!getCavaAggRefSetFunc(refSetFunc, funcRef->getValueType())) {
            return false;
        }

        cavaCode += "            ";
        cavaCode += getCavaAggRefName(_funVector[i], idxStr);
        cavaCode += ".";
        cavaCode += refSetFunc;
        cavaCode += "(doc, item.";
        cavaCode += getCavaAggItemField(_funVector[i], idxStr);
        cavaCode += ");\n"; // countRef0.setLong(doc, item.count0);
    }
    std::string refSetFunc;
    if (!getCavaAggRefSetFunc(refSetFunc, getGroupKeyRef()->getValueType())) {
        return false;
    }

    cavaCode += "            groupKeyRef.";
    cavaCode += refSetFunc;
    cavaCode += "(doc, item.groupKey);\n";
    cavaCode += "            ++i;\n";
    cavaCode += "        }\n";
    cavaCode += "    }\n";
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::genJitAggregator(std::string &cavaCode) {
    cavaCode = R"(
class JitAggregator {
)";
    std::string fields;
    if (!genJitAggFields(fields)) {
        return false;
    }
    std::string countFunc;
    if (!genJitAggCountFunc(countFunc)) {
        return false;
    }
    std::string createFunc;
    if (!genJitAggCreateFunc(createFunc)) {
        return false;
    }
    std::string batchFunc;
    if (!genJitAggBatchFunc(batchFunc)) {
        return false;
    }
    std::string resultFunc;
    if (!genJitAggResultFunc(resultFunc)) {
        return false;
    }
    cavaCode += fields;
    cavaCode += countFunc;
    cavaCode += createFunc;
    cavaCode += batchFunc;
    cavaCode += resultFunc;
    cavaCode += "}\n";
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
bool NormalAggregator<KeyType, ExprType, GroupMapType>::constructCavaCode(std::string &cavaCode) {
    cavaCode = R"(package ha3;
import unsafe.*;

)";

    std::string aggItemClass;
    if (!genAggItemClass(aggItemClass)) {
        return false;
    }

    std::string jitAggregator;
    if (!genJitAggregator(jitAggregator)) {
        return false;
    }
    cavaCode += aggItemClass;
    cavaCode += jitAggregator;
    return true;
}

template <typename KeyType, typename ExprType, typename GroupMapType>
suez::turing::CavaAggModuleInfoPtr NormalAggregator<KeyType, ExprType, GroupMapType>::codegen() {
    // 1. check codegen
    if (!canCodegen()) {
        return suez::turing::CavaAggModuleInfoPtr();
    }
    // 2. construct cava file
    _cavaCode = "";
    bool ret = constructCavaCode(_cavaCode);
    if (!ret) {
        return suez::turing::CavaAggModuleInfoPtr();
    }
    size_t hashKey = _cavaJitWrapper->calcHashKey({_cavaCode});
    auto moduleName = std::to_string(hashKey);
    auto fileName = moduleName;
    // 3. async compile
    cava::CavaModuleOptions options;
    options.safeCheck = cava::SafeCheckLevel::SCL_NONE;
    auto cavaJitModule = _cavaJitWrapper->compileAsync(
            moduleName, fileName, _cavaCode, &options);
    REQUEST_TRACE(TRACE3, "jit cache hit[%s], hashkey: %lu, cava code: %s",
                  cavaJitModule ? "true" : "false", hashKey, _cavaCode.c_str());
    if (cavaJitModule == nullptr) {
        return suez::turing::CavaAggModuleInfoPtr();
    }
    _queryResource->addCavaJitModule(hashKey, cavaJitModule);
    auto ptr = suez::turing::CavaAggModuleInfo::create(cavaJitModule);
    return std::tr1::dynamic_pointer_cast<suez::turing::CavaAggModuleInfo>(ptr);
}

//LOG_SETUP_TEMPLATE(search, NormalAggregator, KeyType);
END_HA3_NAMESPACE(search);

#endif //ISEARCH_NORMALAGGREGATOR_H
