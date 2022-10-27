#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/ops/tvf/TvfResourceBase.h>
#include <ha3/config/QueryInfo.h>

BEGIN_HA3_NAMESPACE(sql);
class SqlQueryResource;
struct TvfFuncInitContext {
    TvfFuncInitContext()
        : queryPool(nullptr)
        , poolResource(nullptr)
        , tvfResourceContainer(nullptr)
    {}

    std::vector<std::string> params;
    autil::mem_pool::Pool *queryPool;
    navi::MemoryPoolResource *poolResource;
    TvfResourceContainer *tvfResourceContainer;
    SqlQueryResource *queryResource;
    config::QueryInfo *queryInfo;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
};

class TvfFunc
{
public:
    TvfFunc() {}
    virtual ~TvfFunc() {}
private:
    TvfFunc(const TvfFunc &);
    TvfFunc& operator=(const TvfFunc &);
public:
    virtual bool init(TvfFuncInitContext &context) = 0;
    virtual bool compute(const TablePtr &input, bool eof, TablePtr &output) = 0;
public:
    std::string getName() const {
        return _name;
    }
    void setName(const std::string &name) {
        _name = name;
    }
    KernelScope getScope() const {
        return _scope;
    }
    void setScope(KernelScope scope) {
        _scope = scope;
    }
protected:
    HA3_LOG_DECLARE();
private:
    std::string _name;
    KernelScope _scope;
};
HA3_TYPEDEF_PTR(TvfFunc);


class OnePassTvfFunc : public TvfFunc
{
public:
    OnePassTvfFunc() {}
    virtual ~OnePassTvfFunc() {}
public:
    bool compute(const TablePtr &input, bool eof, TablePtr &output) override;
protected:
    virtual bool doCompute(const TablePtr &input, TablePtr &output) = 0;
protected:
    TablePtr _table;
};

HA3_TYPEDEF_PTR(TvfFunc);

END_HA3_NAMESPACE(sql);
