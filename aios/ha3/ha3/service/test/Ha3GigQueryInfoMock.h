#ifndef ISEARCH_HA3GIGQUERYSESSIONMOCK_H
#define ISEARCH_HA3GIGQUERYSESSIONMOCK_H

#include <multi_call/agent/QueryInfoForMock.h>
BEGIN_HA3_NAMESPACE(service);

class Ha3GigQueryInfoMock : public multi_call::QueryInfoForMock
{
public:
    Ha3GigQueryInfoMock() {
    }
    ~Ha3GigQueryInfoMock() {
    }
public:
    MOCK_CONST_METHOD1(degradeLevel, float(float));
    MOCK_METHOD3(finish, std::string(float, multi_call::MultiCallErrorCode, multi_call::WeightTy));
};

HA3_TYPEDEF_PTR(Ha3GigQueryInfoMock);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_HA3GIGQUERYSESSIONMOCK_H
