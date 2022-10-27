#include <ha3/service/test/FakeRpcChannel.h>
#include <ha3/admin/DeployWorkItem.h>

using namespace autil;
BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, FakeRpcChannel);

FakeRpcChannel::FakeRpcChannel(autil::ThreadPool *threadPool) { 
    _done = NULL;
    _threadPool = threadPool;
}

FakeRpcChannel::~FakeRpcChannel() { 
}

void FakeRpcChannel::CallMethod(const RPCMethodDescriptor *method,
                            RPCController *controller,
                            const RPCMessage *request,
                            RPCMessage *response,
                            RPCClosure *done)
{
    _done = done;
    _threadPool->pushWorkItem(new admin::DeployWorkItem(
                    std::tr1::bind(&FakeRpcChannel::asyncCall,this)));
}

void FakeRpcChannel::asyncCall()
{
    usleep(100000);
    if (_done) {
        _done->Run();
    }
}
END_HA3_NAMESPACE(service);

