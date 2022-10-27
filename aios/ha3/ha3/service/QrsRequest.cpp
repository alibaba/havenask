#include <ha3/service/QrsRequest.h>
#include <ha3/service/QrsResponse.h>
#include <autil/DataBuffer.h>
#include <suez/turing/proto/Search.pb.h>

using namespace std;
using namespace multi_call;
using namespace autil;

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsRequest);

QrsRequest::QrsRequest(const std::string &methodName,
                       google::protobuf::Message *message,
                       uint32_t timeout,
                       const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcRequest<suez::turing::GraphService_Stub>(methodName, arena)
{
    setMessage(message);
    setTimeout(timeout);
}

QrsRequest::~QrsRequest() {
}

ResponsePtr QrsRequest::newResponse() {
    return ResponsePtr(new QrsResponse(getProtobufArena()));
}

google::protobuf::Message *QrsRequest::serializeMessage() {
    auto pbMessage = static_cast<suez::turing::GraphRequest *>(_message);
    if (!pbMessage) {
        return NULL;
    }
    return _message;
}

END_HA3_NAMESPACE(service);
