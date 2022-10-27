#include <ha3/worker/HaProtoJsonizer.h>
#include <ha3/proto/QrsService.pb.h>
#include <suez/turing/proto/Search.pb.h>

using namespace std;
using namespace ::google::protobuf;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace isearch::proto;

BEGIN_HA3_NAMESPACE(worker);
HA3_LOG_SETUP(worker, HaProtoJsonizer);

HaProtoJsonizer::HaProtoJsonizer() {
}

HaProtoJsonizer::~HaProtoJsonizer() {
}

bool HaProtoJsonizer::fromJson(const string &jsonStr, Message *message) {
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, jsonStr);
        fromJsonMap(jsonMap, message);
    } catch (const ExceptionBase &e) {
        string msgName = string(typeid(*message).name());
        if (msgName == string(typeid(QrsRequest).name())) {
            auto request = dynamic_cast<QrsRequest*>(message);
            if(!request) {
                return false;
            }
            request->set_assemblyquery(jsonStr);
            return true;
        } else if (msgName == string(typeid(suez::turing::HealthCheckRequest).name())) {
            auto request = dynamic_cast<suez::turing::HealthCheckRequest*>(message);
            if(!request) {
                return false;
            }
            request->set_request(jsonStr);
        } else {
            HA3_LOG(WARN, "%s", e.what());
            return false;
        }
    }
    return true;
}

string HaProtoJsonizer::toJson(const Message &message) {
    const QrsResponse *qrsResponse = dynamic_cast<const QrsResponse *>(&message);
    const SqlClientResponse *sqlClientResponse =
        dynamic_cast<const SqlClientResponse *>(&message);
    if (qrsResponse) {
        return qrsResponse->assemblyresult();
    } else if (sqlClientResponse) {
        return sqlClientResponse->assemblyresult();
    }

    return http_arpc::ProtoJsonizer::toJson(message);
}

END_HA3_NAMESPACE(worker);
