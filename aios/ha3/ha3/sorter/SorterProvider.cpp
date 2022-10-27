#include <ha3/sorter/SorterProvider.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <suez/turing/expression/framework/SimpleAttributeExpressionCreator.h>
#include <ha3/queryparser/RequestSymbolDefine.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, SorterProvider);

static Comparator *createSingleComparator(autil::mem_pool::Pool *pool, const ComparatorItem &item)
{
    auto valueType = item.first->getValueType();
    if (valueType.isMultiValue()) {
        return NULL;
    }
#define COMPARATOR_CREATOR_HELPER(vt_type)                              \
    case vt_type:                                                       \
    {                                                                   \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;     \
        const auto typedRef = dynamic_cast<const matchdoc::Reference<T>*>(item.first); \
        Comparator *comp = POOL_NEW_CLASS(pool, ReferenceComparator<T>, typedRef, item.second); \
        return comp;                                                    \
    }

    auto type = valueType.getBuiltinType();
    switch(type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(COMPARATOR_CREATOR_HELPER);
        COMPARATOR_CREATOR_HELPER(vt_string);
    default:
        return NULL;
    }
}

SorterProvider::SorterProvider(const SorterResource &sorterResource)
    : suez::turing::SorterProvider(sorterResource.matchDocAllocator.get(),
                                   sorterResource.pool,
                                   sorterResource.cavaAllocator,
                                   sorterResource.requestTracer,
                                   sorterResource.partitionReaderSnapshot,
                                   sorterResource.kvpairs)
{
    auto creator = dynamic_cast<AttributeExpressionCreator *>(sorterResource.attrExprCreator);
    if (creator) {
        setAttributeExpressionCreator(creator);
    }
    _sorterResource = sorterResource;
    if (sorterResource.request) {
        auto virtualAttributeClause = sorterResource.request->getVirtualAttributeClause();
        if (virtualAttributeClause) {
            _convertor.initVirtualAttrs(virtualAttributeClause->getVirtualAttributes());
        }
    }
    setSearchPluginResource(&_sorterResource);
    setupTraceRefer(convertRankTraceLevel(getRequest()));
}

SorterProvider::~SorterProvider() {
}

const KeyValueMap &SorterProvider::getSortKVPairs() const {
    FinalSortClause *clause = _sorterResource.request->getFinalSortClause();
    if (clause) {
        return clause->getParams();
    }
    static const KeyValueMap emptyKVPairs;
    return emptyKVPairs;
}

SorterLocation SorterProvider::getLocation() const {
    return _sorterResource.location;
}

const Ha3MatchDocAllocatorPtr &SorterProvider::getMatchDocAllocator() const {
    return _sorterResource.matchDocAllocator;
}

autil::mem_pool::Pool *SorterProvider::getSessionPool() const {
    return _sorterResource.pool;
}

AttributeExpression *SorterProvider::getScoreExpression() const {
    return _sorterResource.scoreExpression;
}

ComboComparator* SorterProvider::createSortComparator(
        const SortExpressionVector& sortExprs)
{
    ComboComparator *cmp = _sorterResource.comparatorCreator->createSortComparator(sortExprs);
    if (!cmp) {
        return NULL;
    }
    const vector<AttributeExpression*> &needImmEvaluateExprs =
        _sorterResource.comparatorCreator->getImmEvaluateExpressions();
    for (size_t i = 0; i < needImmEvaluateExprs.size(); i++) {
        addNeedEvaluateExpression(needImmEvaluateExprs[i]);
    }
    return cmp;
}

ComboComparator *SorterProvider::createComparator(
        const vector<ComparatorItem> &compItems)
{
    autil::mem_pool::Pool *pool = getSessionPool();
    ComboComparator *comboComp = POOL_NEW_CLASS(pool, ComboComparator);
    for (size_t i = 0; i < compItems.size(); ++i) {
        Comparator *comp = createSingleComparator(pool, compItems[i]);
        if (!comp) {
            POOL_DELETE_CLASS(comboComp);
            return NULL;
        }
        comboComp->addComparator(comp);
    }
    return comboComp;
}

AttributeExpression *SorterProvider::createAttributeExpression(
        const SyntaxExpr *syntaxExpr, bool needSerizlize)
{
    if (!syntaxExpr) {
        return NULL;
    }
    syntaxExpr = _convertor.exprToExpr(syntaxExpr);
    AttributeExpression *expr =
        _sorterResource.attrExprCreator->createAttributeExpression(syntaxExpr);
    Ha3MatchDocAllocator *allocator = _sorterResource.matchDocAllocator.get();
    if (!expr || !expr->allocate(allocator)) {
        return NULL;
    }
    if (needSerizlize) {
        matchdoc::ReferenceBase *refer = expr->getReferenceBase();
        if (refer->getSerializeLevel() < SL_CACHE) {
            refer->setSerializeLevel(SL_CACHE);
        }
    }
    return expr;
}

SortExpression* SorterProvider::createSortExpression(
        AttributeExpression* attributeExpression, bool sortFlag)
{
    return _sorterResource.sortExprCreator->createSortExpression(
            attributeExpression, sortFlag);
}

SortExpression* SorterProvider::createSortExpression(
        const string &attributeName, bool sortFlag)
{
    if (attributeName == SORT_CLAUSE_RANK) {
        if (!_sorterResource.scoreExpression) {
            return NULL;
        }
        return _sorterResource.sortExprCreator->createSortExpression(
                _sorterResource.scoreExpression, sortFlag);
    }
    if (getLocation() == SL_SEARCHER) {
        AttributeExpression *attrExpr =
            _sorterResource.attrExprCreator->createAtomicExpr(attributeName);
        if (NULL == attrExpr) {
            HA3_LOG(WARN, "create attribute expression[%s] failed.",
                    attributeName.c_str());
            return NULL;
        }
        if(!validateSortAttrExpr(attrExpr)) {
            HA3_LOG(WARN, "sub doc attribute [%s] is not allowed in sort expression.",
                    attributeName.c_str());
            return NULL;
        }
        if (!attrExpr->allocate(_sorterResource.matchDocAllocator.get())) {
            HA3_LOG(WARN, "allocate attribute expression[%s] failed.",
                    attributeName.c_str());
            return NULL;
        }
        return _sorterResource.sortExprCreator->createSortExpression(
                attrExpr, sortFlag);
    } else {
        //proxy  qrs and searcher cache
        const matchdoc::ReferenceBase *ref = requireAttributeWithoutType(attributeName);
        if (!ref) {
            HA3_LOG(WARN, "require attribute [%s] failed!", attributeName.c_str());
            return NULL;
        }
        auto varType = ref->getValueType().getBuiltinType();
        if (ref->getValueType().isMultiValue()) {
            HA3_LOG(WARN, "sort attribute cannot be multi value");
            return NULL;
        }

        SimpleAttributeExpressionCreator *creator =
            dynamic_cast<SimpleAttributeExpressionCreator *>(_sorterResource.attrExprCreator);
        assert(creator);
        AttributeExpression *attrExpr =
            creator->createAttributeExpression(ref->getName(), varType, false);
        return _sorterResource.sortExprCreator->createSortExpression(
                attrExpr, sortFlag);
    }
}

const vector<SortExprMeta>& SorterProvider::getSortExprMeta() {
    return _sortExprMetaVec;
}

uint32_t SorterProvider::getResultSourceNum() const {
    return _sorterResource.resultSourceNum;
}

void SorterProvider::addSortExprMeta(const string &name,
                                     const matchdoc::ReferenceBase* sortRef,
                                     bool sortFlag)
{
    SortExprMeta sortExprMeta;
    sortExprMeta.sortExprName = name;
    sortExprMeta.sortRef = sortRef;
    sortExprMeta.sortFlag = sortFlag;
    _sortExprMetaVec.push_back(sortExprMeta);
}

const SorterResource& SorterProvider::getSorterResource() const {
    return _sorterResource;
}

void SorterProvider::updateRequiredTopk(uint32_t requiredTopk) {
    *_sorterResource.requiredTopk = requiredTopk;
}

uint32_t SorterProvider::getRequiredTopk() const {
    return *_sorterResource.requiredTopk;
}

bool SorterProvider::validateSortAttrExpr(AttributeExpression *expr) {
    if(expr->isSubExpression()) {
        ConfigClause *configClause = _sorterResource.request->getConfigClause();
        SubDocDisplayType type = configClause->getSubDocDisplayType();
        if (type != SUB_DOC_DISPLAY_FLAT) {
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(sorter);
