#ifndef ISEARCH_SORTERRESOURCE_H
#define ISEARCH_SORTERRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearchPluginResource.h>
#include <ha3/rank/ComparatorCreator.h>

BEGIN_HA3_NAMESPACE(sorter);

enum SorterLocation {
    SL_UNKNOWN = 1,
    SL_SEARCHER = 2,
    SL_SEARCHCACHEMERGER = 4,
    SL_PROXY = 8,
    SL_QRS = 16
};

SorterLocation transSorterLocation(const std::string &location);


class SorterResource : public search::SearchPluginResource
{
public:
    SorterResource()
        : scoreExpression(NULL)
        , resultSourceNum(1)
        , requiredTopk(NULL)
        , comparatorCreator(NULL)
        , sortExprCreator(NULL)
        , location(SL_UNKNOWN)
        , globalVariables(NULL)
    {
    }
public:
    suez::turing::AttributeExpression *scoreExpression;
    uint32_t resultSourceNum;
    uint32_t *requiredTopk;
    rank::ComparatorCreator *comparatorCreator;
    search::SortExpressionCreator *sortExprCreator;
    SorterLocation location;
    const std::vector<common::AttributeItemMapPtr> *globalVariables;
};

HA3_TYPEDEF_PTR(SorterResource);

END_HA3_NAMESPACE(sorter);

#endif //ISEARCH_SORTERRESOURCE_H
