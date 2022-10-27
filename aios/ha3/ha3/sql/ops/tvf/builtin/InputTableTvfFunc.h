#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
BEGIN_HA3_NAMESPACE(sql);

class InputTableTvfFunc : public OnePassTvfFunc
{
public:
    InputTableTvfFunc();
    ~InputTableTvfFunc();
    InputTableTvfFunc(const InputTableTvfFunc &) = delete;
    InputTableTvfFunc& operator=(const InputTableTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const TablePtr &input, TablePtr &output);
    bool fillInputTable(std::vector<std::vector<std::vector<std::string>>>& input,
                        TablePtr &output);
    bool prepareInputTableInfo(const std::string &input,
                               std::vector<std::vector<std::vector<std::string>>>& output);
private:
    autil::mem_pool::Pool *_queryPool{nullptr};
    std::vector<std::vector<std::vector<std::string>>> _inputTableInfo;
private:
    static const char ROW_SEPERATOR;
    static const char COL_SEPERATOR;
    static const char MULTIVALUE_SEPERATOR;
};

HA3_TYPEDEF_PTR(InputTableTvfFunc);

class InputTableTvfFuncCreator : public TvfFuncCreator
{
public:
    InputTableTvfFuncCreator();
    ~InputTableTvfFuncCreator();
private:
    InputTableTvfFuncCreator(const InputTableTvfFuncCreator &) = delete;
    InputTableTvfFuncCreator& operator=(const InputTableTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};
HA3_TYPEDEF_PTR(InputTableTvfFuncCreator);

END_HA3_NAMESPACE(sql);
