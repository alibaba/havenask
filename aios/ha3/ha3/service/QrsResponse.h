#ifndef ISEARCH_QRSRESPONSE_H
#define ISEARCH_QRSRESPONSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <multi_call/interface/ArpcResponse.h>

BEGIN_HA3_NAMESPACE(service);

class QrsResponse : public multi_call::ArpcResponse
{
public:
    QrsResponse(const std::shared_ptr<google::protobuf::Arena> &arena);
    ~QrsResponse();
private:
    QrsResponse(const QrsResponse &);
    QrsResponse& operator=(const QrsResponse &);
public:
    bool deserializeApp() override;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsResponse);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSRESPONSE_H
