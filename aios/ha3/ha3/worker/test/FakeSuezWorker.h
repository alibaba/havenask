#ifndef ISEARCH_FAKESUEZWORKER_H
#define ISEARCH_FAKESUEZWORKER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/worker/SuezWorker.h>

BEGIN_HA3_NAMESPACE(worker);

class FakeSuezWorker : public SuezWorker
{
public:
    FakeSuezWorker();
    ~FakeSuezWorker();
private:
    FakeSuezWorker(const FakeSuezWorker &);
    FakeSuezWorker& operator=(const FakeSuezWorker &);
public:
    virtual BizInfoManager *doCreateBizInfoManager() { return NULL; }
    virtual service::SessionPool* doCreateSessionPool() { return NULL; }
    virtual bool registerService(suez::RpcServer *rpcServer) { return false; }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeSuezWorker);

END_HA3_NAMESPACE(worker);

#endif //ISEARCH_FAKESUEZWORKER_H
