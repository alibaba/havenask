#ifndef ISEARCH_BS_MOCKSIMPLEMASTERSCHEDULER_H
#define ISEARCH_BS_MOCKSIMPLEMASTERSCHEDULER_H

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "master_framework/SimpleMasterScheduler.h"

namespace build_service { namespace admin {

class MockSimpleMasterScheduler : public MASTER_FRAMEWORK_NS(simple_master)::SimpleMasterScheduler
{
public:
    MockSimpleMasterScheduler()
    {
        ON_CALL(*this, start()).WillByDefault(Return(true));
        ON_CALL(*this, stop()).WillByDefault(Return(true));
    }
    ~MockSimpleMasterScheduler() {}

public:
    MOCK_METHOD3(init, bool(const std::string&, worker_framework::LeaderChecker*, const std::string&));
    MOCK_METHOD0(start, bool());
    MOCK_METHOD0(stop, bool());
    // MOCK_METHOD1(setAppPlan, void(const MASTER_FRAMEWORK_NS(simple_master)::AppPlan&));
    MOCK_METHOD0(isLeader, bool());
    MOCK_METHOD0(isConnected, bool());
    MOCK_METHOD1(getRoleSlots, std::vector<hippo::SlotInfo>(const std::string&));
    MOCK_METHOD1(getAllRoleSlots, void(std::map<std::string, MASTER_FRAMEWORK_NS(master_base)::SlotInfos>&));
    MOCK_METHOD1(reAllocRoleSlots, void(const std::string&));
    MOCK_METHOD3(createLeaderSerializer,
                 hippo::LeaderSerializer*(const std::string&, const std::string&, const std::string&));
    void setAppPlan(const MASTER_FRAMEWORK_NS(simple_master)::AppPlan& inPlan) { _appPlan = inPlan; }

public:
    MASTER_FRAMEWORK_NS(simple_master)::AppPlan _appPlan;
};

typedef ::testing::StrictMock<MockSimpleMasterScheduler> StrictMockSimpleMasterScheduler;
typedef ::testing::NiceMock<MockSimpleMasterScheduler> NiceMockSimpleMasterScheduler;

}} // namespace build_service::admin

#endif // ISEARCH_BS_MOCKSIMPLEMASTERSCHEDULER_H
