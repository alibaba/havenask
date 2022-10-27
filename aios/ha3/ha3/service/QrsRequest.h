#ifndef ISEARCH_QRSREQUEST_H
#define ISEARCH_QRSREQUEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <multi_call/interface/ArpcRequest.h>
#include <suez/turing/proto/Search.pb.h>


BEGIN_HA3_NAMESPACE(service);

class QrsRequest : public multi_call::ArpcRequest<suez::turing::GraphService_Stub>
{
public:
    QrsRequest(const std::string &bizName,
               google::protobuf::Message *message,
               uint32_t timeout,
               const std::shared_ptr<google::protobuf::Arena> &arena);
    ~QrsRequest();
private:
    QrsRequest(const QrsRequest &);
    QrsRequest& operator=(const QrsRequest &);
public:
    multi_call::ResponsePtr newResponse() override;
    google::protobuf::Message *serializeMessage() override;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsRequest);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSREQUEST_H
