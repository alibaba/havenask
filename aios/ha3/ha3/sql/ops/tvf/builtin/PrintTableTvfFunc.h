#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>

BEGIN_HA3_NAMESPACE(sql);

class PrintTableTvfFunc : public TvfFunc
{
public:
    PrintTableTvfFunc();
    ~PrintTableTvfFunc();
    PrintTableTvfFunc(const PrintTableTvfFunc &) = delete;
    PrintTableTvfFunc& operator=(const PrintTableTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const TablePtr &input, bool eof, TablePtr &output);
private:
    uint32_t _count;
};

HA3_TYPEDEF_PTR(PrintTableTvfFunc);

class PrintTableTvfFuncCreator : public TvfFuncCreator
{
public:
    PrintTableTvfFuncCreator();
    ~PrintTableTvfFuncCreator();
private:
    PrintTableTvfFuncCreator(const PrintTableTvfFuncCreator &) = delete;
    PrintTableTvfFuncCreator& operator=(const PrintTableTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};

HA3_TYPEDEF_PTR(PrintTableTvfFuncCreator);

END_HA3_NAMESPACE(sql);
