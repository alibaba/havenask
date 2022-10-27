#ifndef ISEARCH_NAVIMOCK_H
#define ISEARCH_NAVIMOCK_H

#include <navi/engine/Navi.h>
#include <navi/engine/Resource.h>
#include <navi/engine/RunGraphParams.h>

BEGIN_HA3_NAMESPACE(service);

class NaviMock : public navi::Navi
{
public:
    NaviMock() {
    }
    ~NaviMock() {
    }
public:
    MOCK_METHOD5(runGraph, void(navi::GraphDef *, const navi::GraphInputPortsPtr &,
                                const navi::GraphOutputPortsPtr &,
                                const navi::RunGraphParams &,
                                const std::map<std::string, navi::Resource *> &));
};

HA3_TYPEDEF_PTR(NaviMock);

END_HA3_NAMESPACE(service);

#endif // ISEARCH_NAVIMOCK_H
