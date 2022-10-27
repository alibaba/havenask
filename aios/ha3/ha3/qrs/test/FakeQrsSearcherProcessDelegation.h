#ifndef ISEARCH_FAKEQRSSEARCHERPROCESSDELEGATION_H
#define ISEARCH_FAKEQRSSEARCHERPROCESSDELEGATION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>

BEGIN_HA3_NAMESPACE(qrs);

class FakeQrsSearcherProcessDelegation : public service::QrsSearcherProcessDelegation
{
public:
    FakeQrsSearcherProcessDelegation(
            const service::HitSummarySchemaCachePtr &hitSummarySchemaCache);
    ~FakeQrsSearcherProcessDelegation();
public:
    virtual common::ResultPtr search(common::RequestPtr &requestPtr);
    int32_t getConnectionTimeout() const {
        return _connectionTimeout;
    }
    void setConnectionTimeout(int32_t connectionTimeout) {
        _connectionTimeout = connectionTimeout;
    }
private:
    int32_t _connectionTimeout;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeQrsSearcherProcessDelegation);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKEQRSSEARCHERPROCESSDELEGATION_H
