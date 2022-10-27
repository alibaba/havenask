#ifndef ISEARCH_FAKEBIZINFOMANAGER_H
#define ISEARCH_FAKEBIZINFOMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/worker/BizInfoManager.h>

BEGIN_HA3_NAMESPACE(worker);

class FakeBizInfoManager : public BizInfoManager
{
public:
    FakeBizInfoManager();
    ~FakeBizInfoManager();
private:
    FakeBizInfoManager(const FakeBizInfoManager &);
    FakeBizInfoManager& operator=(const FakeBizInfoManager &);
public:
    /* override */ bool init(const worker::ZoneResourcePtr &zoneResource,
                             const WorkerResourcePtr &workerResource);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeBizInfoManager);

END_HA3_NAMESPACE(worker);

#endif //ISEARCH_FAKEBIZINFOMANAGER_H
