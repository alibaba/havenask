#include <ha3/service/test/FakeClosure.h>

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, FakeClosure);

FakeClosure::FakeClosure() { 
    _count = 0;
}

FakeClosure::~FakeClosure() { 
}

END_HA3_NAMESPACE(service);

