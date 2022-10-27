#ifndef ISEARCH_FAKETIMEOUTTERMINATOR_H
#define ISEARCH_FAKETIMEOUTTERMINATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/TimeoutTerminator.h>

BEGIN_HA3_NAMESPACE(search);
class FakeTimeoutTerminator : public common::TimeoutTerminator
{
public:
    FakeTimeoutTerminator(int timeOutCount);
    ~FakeTimeoutTerminator();
private:
    FakeTimeoutTerminator(const FakeTimeoutTerminator &);
    FakeTimeoutTerminator& operator=(const FakeTimeoutTerminator &);
protected:
    /* override */ int64_t getCurrentTime();
private:
    int _timeOutCount;
    int _curCount;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeTimeoutTerminator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKETIMEOUTTERMINATOR_H
