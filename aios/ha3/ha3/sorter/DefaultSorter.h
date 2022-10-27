#ifndef ISEARCH_DEFAULTSORTER_H
#define ISEARCH_DEFAULTSORTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/DistinctHitCollector.h>
#include <ha3/sorter/SorterProvider.h>
#include <suez/turing/expression/plugin/Sorter.h>

BEGIN_HA3_NAMESPACE(sorter);

class DefaultSorter : public suez::turing::Sorter
{
public:
    enum DefaultSortType {
        DS_UNKNOWN,
        DS_NORMAL_SORT,
        DS_DISTINCT
    };
public:
    DefaultSorter();
    ~DefaultSorter();
public:
    bool beginSort(suez::turing::SorterProvider *provider) override;
    void sort(suez::turing::SortParam &sortParam) override;
    Sorter* clone() override {
        return new DefaultSorter(*this);
    }
    void endSort() override {
    }
    void destroy() override {
        delete this;
    }
private:
    bool initSortExpressions(HA3_NS(sorter)::SorterProvider *provider,
                             search::SortExpressionVector &sortExpressions);
    bool initSortExpressionsFromSortKey(const std::string &sortKeyDesc,
            search::SortExpressionVector &sortExpressions,
            HA3_NS(sorter)::SorterProvider *provider);
    void initComparator(HA3_NS(sorter)::SorterProvider *provider,
                        const search::SortExpressionVector &sortExpressions);

    bool prepareForDistinctHitCollector(
            common::DistinctDescription *distDesc,
            HA3_NS(sorter)::SorterProvider *provider, search::SortExpression *expr);

    void serializeSortExprRef(
            const search::SortExpressionVector &sortExpressions);
    void addSortExprMeta(const std::string &name,
                         matchdoc::ReferenceBase* sortRef,
                         bool sortFlag, HA3_NS(sorter)::SorterProvider *provider);
    bool needScoreInSort(const common::Request *request) const;
    bool validateSortInfo(const common::Request *request) const;
    bool needSort(HA3_NS(sorter)::SorterProvider *provider) const;

private:
    struct DistincCollectorParam {
        DistincCollectorParam()
            : pool(NULL), sortExpr(NULL) { }
        common::Ha3MatchDocAllocatorPtr allocator;
        autil::mem_pool::Pool *pool;
        rank::DistinctParameter dp;
        search::SortExpression *sortExpr;
    };
private:
    DefaultSortType _type;
    rank::ComboComparator *_cmp;
    DistincCollectorParam _distinctCollectorParam;
    bool _needSort;

private:
    friend class DefaultSorterTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DefaultSorter);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_DEFAULTSORTER_H
