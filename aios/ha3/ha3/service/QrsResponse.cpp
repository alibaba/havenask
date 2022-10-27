#include <ha3/service/QrsResponse.h>
#include <suez/turing/proto/Search.pb.h>

using namespace std;

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsResponse);

QrsResponse::QrsResponse(const std::shared_ptr<google::protobuf::Arena> &arena)
    : ArpcResponse(arena)
{
}

QrsResponse::~QrsResponse() {
}

bool QrsResponse::deserializeApp() {
    auto message = dynamic_cast<suez::turing::GraphResponse *>(getMessage());
    if (!message) {
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(service);
