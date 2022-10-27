#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>

BEGIN_HA3_NAMESPACE(sql);

class EnableShuffleTvfFunc : public TvfFunc
{
public:
    EnableShuffleTvfFunc();
    ~EnableShuffleTvfFunc();
    EnableShuffleTvfFunc(const EnableShuffleTvfFunc &) = delete;
    EnableShuffleTvfFunc& operator=(const EnableShuffleTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const TablePtr &input, bool eof, TablePtr &output);
};

HA3_TYPEDEF_PTR(EnableShuffleTvfFunc);

class EnableShuffleTvfFuncCreator : public TvfFuncCreator
{
public:
    EnableShuffleTvfFuncCreator();
    ~EnableShuffleTvfFuncCreator();
private:
    EnableShuffleTvfFuncCreator(const EnableShuffleTvfFuncCreator &) = delete;
    EnableShuffleTvfFuncCreator& operator=(const EnableShuffleTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};
HA3_TYPEDEF_PTR(EnableShuffleTvfFuncCreator);

END_HA3_NAMESPACE(sql);
