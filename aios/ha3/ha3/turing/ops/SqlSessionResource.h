#ifndef ISEARCH_SQLSESSIONRESOURCE_H
#define ISEARCH_SQLSESSIONRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/SharedObjectMap.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <ha3/config/QueryInfo.h>
#include <navi/engine/Navi.h>
#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <ha3/sql/ops/tvf/TvfFuncManager.h>
#include <ha3/proto/BasicDefs.pb.h>

BEGIN_HA3_NAMESPACE(turing);

class SqlSessionResource : public suez::turing::SharedObjectBase {
public:
    SqlSessionResource();
    ~SqlSessionResource();
private:
    SqlSessionResource(const SqlSessionResource &);
    SqlSessionResource& operator=(const SqlSessionResource &);
public:
    static std::string name() {
        return "SqlSessionResource";
    }
public:
    build_service::analyzer::AnalyzerFactoryPtr analyzerFactory;
    config::QueryInfo queryInfo;
    navi::NaviPtr naviPtr;
    sql::AggFuncManagerPtr aggFuncManager;
    sql::TvfFuncManagerPtr tvfFuncManager;
    proto::Range range;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlSessionResource);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SQLSESSIONRESOURCE_H
