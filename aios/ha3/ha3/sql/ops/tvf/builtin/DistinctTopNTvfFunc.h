#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>

BEGIN_HA3_NAMESPACE(sql);

class DistinctTopNTvfFunc : public OnePassTvfFunc
{
public:
    DistinctTopNTvfFunc();
    ~DistinctTopNTvfFunc();
    DistinctTopNTvfFunc(const DistinctTopNTvfFunc &) = delete;
    DistinctTopNTvfFunc& operator=(const DistinctTopNTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const TablePtr &input, TablePtr &output) override;
private:
    bool computeTable(const TablePtr &input);
private:
    std::vector<std::string> _groupFields;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _sortStr;
    std::string _groupStr;
    int64_t _totalLimit;
    int64_t _perGroupLimit;
    int64_t _partTotalLimit;
    int64_t _partPerGroupLimit;
    autil::mem_pool::Pool *_queryPool;
};

HA3_TYPEDEF_PTR(DistinctTopNTvfFunc);

class DistinctTopNTvfFuncCreator : public TvfFuncCreator
{
public:
    DistinctTopNTvfFuncCreator();
    ~DistinctTopNTvfFuncCreator();
private:
    DistinctTopNTvfFuncCreator(const DistinctTopNTvfFuncCreator &) = delete;
    DistinctTopNTvfFuncCreator& operator=(const DistinctTopNTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};

HA3_TYPEDEF_PTR(DistinctTopNTvfFuncCreator);

END_HA3_NAMESPACE(sql);
