#ifndef ISEARCH_FAKERPCCHANNEL_H
#define ISEARCH_FAKERPCCHANNEL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <arpc/CommonMacros.h>
#include <autil/Thread.h>
#include <autil/ThreadPool.h>

BEGIN_HA3_NAMESPACE(service);

class FakeRpcChannel : public RPCChannel
{
public:
    FakeRpcChannel(autil::ThreadPool *threadPool);
    ~FakeRpcChannel();
private:
    FakeRpcChannel(const FakeRpcChannel &);
    FakeRpcChannel& operator=(const FakeRpcChannel &);
public:
    virtual void CallMethod(const RPCMethodDescriptor *method,
                            RPCController *controller,
                            const RPCMessage *request,
                            RPCMessage *response,
                            RPCClosure *done);
private:
    void asyncCall();
private:
    RPCClosure *_done;
    autil::ThreadPool *_threadPool;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeRpcChannel);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_FAKERPCCHANNEL_H
