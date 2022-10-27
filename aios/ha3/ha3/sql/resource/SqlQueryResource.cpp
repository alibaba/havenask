#include <ha3/sql/resource/SqlQueryResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace kmonitor;
BEGIN_HA3_NAMESPACE(sql);

SqlQueryResource::SqlQueryResource(const QueryResourcePtr &queryResource)
    : _queryResource(queryResource)
    , _timeout(-1)
{
    _sqlSearchInfoCollector.reset(new SqlSearchInfoCollector());
}

SqlQueryResource::~SqlQueryResource() {}

autil::mem_pool::Pool* SqlQueryResource::getPool() const {
    return _queryResource->getPool();
}

Tracer* SqlQueryResource::getTracer() const {
    return _queryResource->getTracer();
}

SuezCavaAllocator* SqlQueryResource::getSuezCavaAllocator() const {
    return _queryResource->getCavaAllocator();
}

TimeoutTerminator* SqlQueryResource::getTimeoutTerminator() const {
    return _queryResource->getTimeoutTerminator();
}

IE_NAMESPACE(partition)::PartitionReaderSnapshot* SqlQueryResource::getPartitionReaderSnapshot() const {
    return _queryResource->getIndexSnapshot();
}

MetricsReporter* SqlQueryResource::getQueryMetricsReporter() const {
    return _queryResource->getQueryMetricsReporter();
}

multi_call::QuerySession* SqlQueryResource::getGigQuerySession() const {
    return _queryResource->getGigQuerySession();
}
void SqlQueryResource::setGigQuerySession(const multi_call::QuerySessionPtr &querySession) {
    _queryResource->setGigQuerySession(querySession);
}

SqlSearchInfoCollector* SqlQueryResource::getSqlSearchInfoCollector() const {
    return _sqlSearchInfoCollector.get();
}

int64_t SqlQueryResource::getStartTime() const {
    return _queryResource->startTime;
}


END_HA3_NAMESPACE(sql);
