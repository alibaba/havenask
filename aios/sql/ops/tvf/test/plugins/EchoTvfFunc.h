#pragma once
#include <memory>
#include <stdint.h>
#include <string>

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace sql {

class EchoTvfFunc : public TvfFunc {
public:
    EchoTvfFunc();
    ~EchoTvfFunc();
    EchoTvfFunc(const EchoTvfFunc &) = delete;
    EchoTvfFunc &operator=(const EchoTvfFunc &) = delete;

private:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

private:
    uint32_t _count;
};

typedef std::shared_ptr<EchoTvfFunc> EchoTvfFuncPtr;

class EchoTvfFuncCreator : public TvfFuncCreatorR {
public:
    EchoTvfFuncCreator();
    ~EchoTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "echoTvf";
    }

private:
    EchoTvfFuncCreator(const EchoTvfFuncCreator &) = delete;
    EchoTvfFuncCreator &operator=(const EchoTvfFuncCreator &) = delete;

public:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};
typedef std::shared_ptr<EchoTvfFuncCreator> EchoTvfFuncCreatorPtr;
} // namespace sql
