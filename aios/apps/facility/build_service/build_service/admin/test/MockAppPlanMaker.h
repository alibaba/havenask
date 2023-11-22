#pragma once

#include "build_service/admin/AppPlanMaker.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MockAppPlanMaker : public AppPlanMaker
{
public:
    MockAppPlanMaker(const std::string& appName, const std::string& heartbeatType)
        : AppPlanMaker(appName, heartbeatType)
    {
    }
    ~MockAppPlanMaker() {}

protected:
    MOCK_METHOD1(loadConfig, bool(const std::string&));
    MOCK_METHOD(RolePlan, makeRoleForAgent, (const std::string& roleName, const std::string& groupName));
};

typedef ::testing::StrictMock<MockAppPlanMaker> StrictMockAppPlanMaker;
typedef ::testing::NiceMock<MockAppPlanMaker> NiceMockAppPlanMaker;

}} // namespace build_service::admin
