#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
#include <ha3/sql/rank/ComboComparator.h>

BEGIN_HA3_NAMESPACE(sql);

class SortTvfFunc : public TvfFunc
{
public:
    SortTvfFunc();
    ~SortTvfFunc();
    SortTvfFunc(const SortTvfFunc &) = delete;
    SortTvfFunc& operator=(const SortTvfFunc &) = delete;
public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const TablePtr &input, bool eof, TablePtr &output);
private:
    bool doCompute(const TablePtr &input);
private:
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    ComboComparatorPtr _comparator;
    TablePtr _table;
    std::string _sortStr;
    int32_t _count;
    autil::mem_pool::Pool *_queryPool;
};

HA3_TYPEDEF_PTR(SortTvfFunc);

class SortTvfFuncCreator : public TvfFuncCreator
{
public:
    SortTvfFuncCreator();
    ~SortTvfFuncCreator();
private:
    SortTvfFuncCreator(const SortTvfFuncCreator &) = delete;
    SortTvfFuncCreator& operator=(const SortTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};

HA3_TYPEDEF_PTR(SortTvfFuncCreator);

END_HA3_NAMESPACE(sql);
