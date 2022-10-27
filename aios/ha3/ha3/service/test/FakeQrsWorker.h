#ifndef ISEARCH_FAKEQRSWORKER_H
#define ISEARCH_FAKEQRSWORKER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsWorker.h>
#include <ha3/service/test/FakeQrsHaPartition.h>

BEGIN_HA3_NAMESPACE(service);

class FakeQrsWorker : public QrsWorker
{
public:
    FakeQrsWorker() {}
    ~FakeQrsWorker() {}
private:
    FakeQrsWorker(const FakeQrsWorker &);
    FakeQrsWorker& operator = (const FakeQrsWorker &);

protected:
    virtual HaPartitionPtr createHaPartition(const proto::PartitionID &/* no use */) {
        return HaPartitionPtr(new FakeQrsHaPartition);
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeQrsWorker);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_FAKEQRSWORKER_H
