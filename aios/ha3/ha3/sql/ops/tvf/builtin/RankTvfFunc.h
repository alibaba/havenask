#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>

BEGIN_HA3_NAMESPACE(sql);

class RankTvfFunc : public OnePassTvfFunc
{
public:
    RankTvfFunc();
    ~RankTvfFunc();
    RankTvfFunc(const RankTvfFunc &) = delete;
    RankTvfFunc& operator=(const RankTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const TablePtr &input, TablePtr &output) override;
private:
    std::vector<std::string> _partitionFields;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _sortStr;
    std::string _partitionStr;
    int32_t _count;
    autil::mem_pool::Pool *_queryPool;
};

HA3_TYPEDEF_PTR(RankTvfFunc);

class RankTvfFuncCreator : public TvfFuncCreator
{
public:
    RankTvfFuncCreator();
    ~RankTvfFuncCreator();
private:
    RankTvfFuncCreator(const RankTvfFuncCreator &) = delete;
    RankTvfFuncCreator& operator=(const RankTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};

HA3_TYPEDEF_PTR(RankTvfFuncCreator);

END_HA3_NAMESPACE(sql);
