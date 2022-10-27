#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
#include <ha3/sql/data/Table.h>

BEGIN_HA3_NAMESPACE(sql);

class UnPackMultiValueTvfFunc : public TvfFunc
{
public:
    UnPackMultiValueTvfFunc();
    ~UnPackMultiValueTvfFunc();
private:
    UnPackMultiValueTvfFunc(const UnPackMultiValueTvfFunc &) = delete;
    UnPackMultiValueTvfFunc& operator=(const UnPackMultiValueTvfFunc &) = delete;
public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const TablePtr &input, bool eof, TablePtr &output);

private:
    bool unpackMultiValue(const ColumnPtr &column, TablePtr table);

    bool unpackTable(const std::vector<std::pair<int32_t, int32_t> > &unpackVec,
                     const std::string &unpackColName,
                     TablePtr &unpackTable);

    bool genColumnUnpackOffsetVec(const ColumnPtr &column,
                                  std::vector<std::pair<int32_t, int32_t> > &toMergeVec);

    bool copyNormalCol(size_t rawRowCount,
                       const std::vector<Row> &allRow,
                       const std:: vector<std::pair<int32_t, int32_t> > &unpackVec,
                       ColumnPtr &column);
    bool copyUnpackCol(size_t rawRowCount,
                       const std::vector<Row> &allRow,
                       const std:: vector<std::pair<int32_t, int32_t> > &unpackVec,
                       ColumnPtr &column);

private:
    std::vector<std::string> _unpackFields;
    autil::mem_pool::Pool *_queryPool;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(UnPackMultiValueTvfFunc);


class UnPackMultiValueTvfFuncCreator : public TvfFuncCreator
{
public:
    UnPackMultiValueTvfFuncCreator();
    ~UnPackMultiValueTvfFuncCreator();
private:
    UnPackMultiValueTvfFuncCreator(const UnPackMultiValueTvfFuncCreator &) = delete;
    UnPackMultiValueTvfFuncCreator& operator=(const UnPackMultiValueTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(UnPackMultiValueTvfFuncCreator);




END_HA3_NAMESPACE(sql);
