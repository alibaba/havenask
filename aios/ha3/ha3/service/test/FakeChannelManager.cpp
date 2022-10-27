#include <ha3/service/test/FakeChannelManager.h>

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, FakeChannelManager);

FakeChannelManager::FakeChannelManager() {
    _threadPool.start();
}

FakeChannelManager::~FakeChannelManager() { 
}

END_HA3_NAMESPACE(service);

