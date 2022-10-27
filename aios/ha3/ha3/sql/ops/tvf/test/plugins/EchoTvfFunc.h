#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>

BEGIN_HA3_NAMESPACE(sql);

class EchoTvfFunc : public TvfFunc
{
public:
    EchoTvfFunc();
    ~EchoTvfFunc();
    EchoTvfFunc(const EchoTvfFunc &) = delete;
    EchoTvfFunc& operator=(const EchoTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const TablePtr &input, bool eof, TablePtr &output);
private:
    uint32_t _count;
};

HA3_TYPEDEF_PTR(EchoTvfFunc);

class EchoTvfFuncCreator : public TvfFuncCreator
{
public:
    EchoTvfFuncCreator();
    ~EchoTvfFuncCreator();
private:
    EchoTvfFuncCreator(const EchoTvfFuncCreator &) = delete;
    EchoTvfFuncCreator& operator=(const EchoTvfFuncCreator &) = delete;
public:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};
HA3_TYPEDEF_PTR(EchoTvfFuncCreator);





END_HA3_NAMESPACE(sql);
