#ifndef ISEARCH_QRSQUERYRESOURCE_H
#define ISEARCH_QRSQUERYRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Tracer.h>
#include <suez/turing/common/QueryResource.h>
#include <ha3/service/QrsSearchConfig.h>

BEGIN_HA3_NAMESPACE(turing);

class QrsQueryResource: public tensorflow::QueryResource
{
public:
    QrsQueryResource()
        :_srcCount(1u)
    {};
    ~QrsQueryResource() {};
public:
    uint32_t _srcCount;
    service::QrsSearchConfig *qrsSearchConfig = nullptr;
private:
    QrsQueryResource(const QrsQueryResource &);
    QrsQueryResource& operator=(const QrsQueryResource &);
private:
    HA3_LOG_DECLARE();
};
typedef std::shared_ptr<QrsQueryResource> QrsQueryResourcePtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_QRSQUERYRESOURCE_H
