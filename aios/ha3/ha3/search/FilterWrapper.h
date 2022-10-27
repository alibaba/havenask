#ifndef ISEARCH_FILTERWRAPPER_H
#define ISEARCH_FILTERWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/Filter.h>
#include <ha3/search/JoinFilter.h>

BEGIN_HA3_NAMESPACE(search);

class SubDocFilter {
public:
    SubDocFilter(matchdoc::SubDocAccessor *accessor)
        : _subDocAccessor(accessor)
    {}
public:
    bool pass(matchdoc::MatchDoc matchDoc);
private:
    matchdoc::SubDocAccessor *_subDocAccessor;
};

class FilterWrapper
{
public:
    FilterWrapper();
    ~FilterWrapper();
private:
    FilterWrapper(const FilterWrapper &);
    FilterWrapper& operator=(const FilterWrapper &);
public:
    inline bool pass(matchdoc::MatchDoc matchDoc);
public:
    void setFilter(Filter *filter) {
        if (!filter) {
            return;
        }
        if (filter->needFilterSubDoc()) {
            _subExprFilter = filter;
        } else {
            _filter = filter;
        }
    }
    Filter* getFilter() const { return _filter; }
    Filter* getSubExprFilter() const { return _subExprFilter; }

    void setJoinFilter(JoinFilter *joinFilter) { _joinFilter = joinFilter; }
    JoinFilter* getJoinFilter() const { return _joinFilter; }

    void setSubDocFilter(SubDocFilter *subDocFilter) { _subDocFilter = subDocFilter; }
    SubDocFilter* getSubDocFilter() const { return _subDocFilter; }
private:
    JoinFilter *_joinFilter;
    SubDocFilter *_subDocFilter;
    Filter *_filter;
    Filter *_subExprFilter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FilterWrapper);

inline bool FilterWrapper::pass(matchdoc::MatchDoc matchDoc) {
    if (_joinFilter != NULL && !_joinFilter->pass(matchDoc)) {
        return false;
    }
    if (_subDocFilter != NULL && !_subDocFilter->pass(matchDoc)) {
        return false;
    }
    if (_filter != NULL && !_filter->pass(matchDoc)) {
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FILTERWRAPPER_H
