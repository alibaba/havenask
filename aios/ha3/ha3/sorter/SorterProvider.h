#ifndef ISEARCH_SORTERPROVIDER_H
#define ISEARCH_SORTERPROVIDER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sorter/SorterResource.h>
#include <ha3/common/AggResultReader.h>
#include <ha3/rank/ComboComparator.h>
#include <ha3/rank/ComparatorCreator.h>
#include <ha3/search/ProviderBase.h>
#include <suez/turing/expression/util/VirtualAttrConvertor.h>
#include <suez/turing/expression/provider/SorterProvider.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <ha3/common/SortExprMeta.h>

BEGIN_HA3_NAMESPACE(sorter);

typedef std::pair<const matchdoc::ReferenceBase *, bool> ComparatorItem;
typedef std::vector<ComparatorItem> ComparatorItemVec;

class SorterProvider : public suez::turing::SorterProvider,
                       public search::ProviderBase
{
public:
    SorterProvider(const SorterResource &sorterResource);
    ~SorterProvider();
private:
    SorterProvider(const SorterProvider & );
    SorterProvider& operator=(const SorterProvider &);
public:
    using suez::turing::ProviderBase::requireAttributeWithoutType;
    using suez::turing::SorterProvider::requireAttributeWithoutType;
    using suez::turing::SorterProvider::requireAttribute;
    using suez::turing::ProviderBase::requireAttribute;
    template<typename T>
    T *declareGlobalVariable(const std::string &variName, bool needSerialize = true);
    template<typename T>
    T *findGlobalVariableInSort(const std::string &variName) const;

    /* not support
      template<typename T>
      std::vector<T *> findGlobalVariableInMerge(const std::string &variName) const;
    */
    
    /*
       This method is only for buildin DefaultSorter,
       any other Sorters should not use it
    */
    suez::turing::AttributeExpression *createAttributeExpression(
            const suez::turing::SyntaxExpr *syntaxExpr,
            bool needSerialize = true);

    template<typename T>
    const matchdoc::Reference<T> *getBuildInReference(const std::string &refName);

    void addSortExprMeta(const std::string &name,
                         const matchdoc::ReferenceBase *sortRef, bool sortFlag);

    const std::vector<common::SortExprMeta>& getSortExprMeta();

public:
    // get resource from provider
    const KeyValueMap &getSortKVPairs() const;

    SorterLocation getLocation() const;

    uint32_t getResultSourceNum() const;

    uint32_t getTotalMatchDocs() const;

    const common::Ha3MatchDocAllocatorPtr &getMatchDocAllocator() const;

    autil::mem_pool::Pool *getSessionPool() const;

    suez::turing::AttributeExpression *getScoreExpression() const;

    const SorterResource& getSorterResource() const;

    void updateRequiredTopk(uint32_t requiredTopk);
    uint32_t getRequiredTopk() const;
public:
    // plugins helper
    rank::ComboComparator *createSortComparator(
            const search::SortExpressionVector &sortExprs);
    rank::ComboComparator *createComparator(
            const std::vector<ComparatorItem> &compItems);
    search::SortExpression *createSortExpression(
            suez::turing::AttributeExpression *attributeExpression, bool sortFlag);
    search::SortExpression *createSortExpression(const std::string &attributeName,
            bool sortFlag);
private:
    bool validateSortAttrExpr(suez::turing::AttributeExpression *expr);
        // for test
    SorterResource* getInnerSorterResource() {
        return &_sorterResource;
    }
    void prepareForSort(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs) {
        suez::turing::ExpressionEvaluator<suez::turing::ExpressionVector> evaluator(
                getNeedEvaluateAttrs(),
                getAllocator()->getSubDocAccessor());
        evaluator.batchEvaluateExpressions(matchDocs.data(), matchDocs.size());
        batchPrepareTracer(matchDocs.data(), matchDocs.size());
    }
private:
    SorterResource _sorterResource;
    std::string _errorMsg;
    std::vector<common::SortExprMeta> _sortExprMetaVec;
private:
    friend class SorterProviderTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SorterProvider);
////////////////////////////////////////////////////////////////////////////

template<typename T>
T *SorterProvider::declareGlobalVariable(const std::string &variName, bool needSerialize)
{
    if (getLocation() != SL_SEARCHER) {
        return NULL;
    } else {
        return doDeclareGlobalVariable<T>(variName, needSerialize);
    }
}

template<typename T>
T *SorterProvider::findGlobalVariableInSort(const std::string &variName) const
{
    if (getLocation() != SL_SEARCHER) {
        return NULL;
    }
    return findGlobalVariable<T>(variName);
}

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_SORTERPROVIDER_H
