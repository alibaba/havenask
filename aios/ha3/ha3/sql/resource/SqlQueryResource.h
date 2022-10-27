#ifndef ISEARCH_SQL_QUERYRESOURCE_H
#define ISEARCH_SQL_QUERYRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <navi/engine/Resource.h>
#include <suez/turing/common/QueryResource.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <multi_call/interface/QuerySession.h>

BEGIN_HA3_NAMESPACE(sql);

class SqlQueryResource : public navi::Resource {
public:
    SqlQueryResource(const tensorflow::QueryResourcePtr &queryResource); // to hold qrs query resource
    ~SqlQueryResource();
public:
    std::string getResourceName() const override {
        return "SqlQueryResource";
    }
    autil::mem_pool::Pool* getPool() const;
    suez::turing::Tracer* getTracer() const ;
    suez::turing::SuezCavaAllocator* getSuezCavaAllocator() const;
    suez::turing::TimeoutTerminator* getTimeoutTerminator() const;
    IE_NAMESPACE(partition)::PartitionReaderSnapshot* getPartitionReaderSnapshot() const;
    int64_t getStartTime() const;

    kmonitor::MetricsReporter* getQueryMetricsReporter() const;
    multi_call::QuerySession* getGigQuerySession() const;
    void setGigQuerySession(const multi_call::QuerySessionPtr &querySession);
    SqlSearchInfoCollector* getSqlSearchInfoCollector() const;
    void setTimeoutMs(int64_t timeout) {
        _timeout = timeout;
    }
    int64_t getTimeoutMs() const {
        return _timeout;
    }

private:
    tensorflow::QueryResourcePtr _queryResource;
    SqlSearchInfoCollectorPtr _sqlSearchInfoCollector;
    int64_t _timeout;
};

HA3_TYPEDEF_PTR(SqlQueryResource);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_QUERYRESOURCE_H
