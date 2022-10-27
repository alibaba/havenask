#ifndef ISEARCH_FAKECLOSURE_H
#define ISEARCH_FAKECLOSURE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <arpc/CommonMacros.h>
#include <autil/Lock.h>
#include <multi_call/rpc/GigClosure.h>

BEGIN_HA3_NAMESPACE(service);

class FakeClosure : public multi_call::GigClosure
{
public:
    FakeClosure();
    ~FakeClosure();
private:
    FakeClosure(const FakeClosure &);
    FakeClosure& operator = (const FakeClosure &);
public:
    void Run() override{
        autil::ScopedLock lock(_cond);
        _count++;
        _cond.signal();
    }
    multi_call::ProtocolType getProtocolType() override{
	return multi_call::MC_PROTOCOL_UNKNOWN;
    }

    int getCount() {
        return _count;
    }
    void clearCount(){
        _count = 0;
    }
    void wait() {
        while(true) {
            autil::ScopedLock lock(_cond);
            if (_count != 0) {
                return;
            }
            _cond.wait();
        }
    }
    void lock() {
        _cond.lock();
    }
    void unlock() {
        _cond.unlock();
    }
private:
    int _count;
    autil::ThreadCond _cond;
    
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeClosure);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_FAKECLOSURE_H
