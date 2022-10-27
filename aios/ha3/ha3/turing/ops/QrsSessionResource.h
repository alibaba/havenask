#ifndef ISEARCH_QRSSESSIONRESOURCE_H
#define ISEARCH_QRSSESSIONRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <suez/turing/common/SessionResource.h>

BEGIN_HA3_NAMESPACE(turing);

class QrsSessionResource: public tensorflow::SessionResource
{
public:
    QrsSessionResource(uint32_t count)
        : SessionResource(count){};
    ~QrsSessionResource() {};
private:
    QrsSessionResource(const QrsSessionResource &);
    QrsSessionResource& operator=(const QrsSessionResource &);
public:
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagers;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsSessionResource);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_QRSSESSIONRESOURCE_H
