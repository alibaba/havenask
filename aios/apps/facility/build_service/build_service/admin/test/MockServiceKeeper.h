#ifndef ISEARCH_BS_MOCKSERVICEKEEPER_H
#define ISEARCH_BS_MOCKSERVICEKEEPER_H

#include "build_service/admin/ServiceKeeper.h"
#include "build_service/admin/test/MockAppPlanMaker.h"
#include "build_service/admin/test/MockGenerationKeeper.h"
#include "build_service/admin/test/MockSimpleMasterScheduler.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MockServiceKeeper : public ServiceKeeper
{
public:
    MockServiceKeeper(bool useFakeGenerationKeeper = true)
    {
        ON_CALL(*this, createSimpleMasterScheduler(_, _, _))
            .WillByDefault(Invoke(std::bind(&MockServiceKeeper::createMockScheduler)));
        _needRecover = false;
        _needAppPlan = false;
        _healthChecker.reset(new AdminHealthChecker);
        _healthChecker->init(this);
        _useFakeGenerationKeeper = useFakeGenerationKeeper;
    }
    ~MockServiceKeeper()
    {
        // call ServiceKeeper::stop, join loop thread
        // avoid it call ServiceKeeper::generateAppPlan not MockServiceKeeper::generateAppPlan
        stop();
    }
    bool recover(const std::vector<proto::BuildId>& buildIds) override
    {
        if (_needRecover) {
            return ServiceKeeper::recover(buildIds);
        }
        return true;
    }
    AppPlan generateAppPlan(const GenerationKeepers& keepers, const std::vector<std::string>& ips) override
    {
        if (!_needAppPlan) {
            return ServiceKeeper::generateAppPlan(keepers, ips);
        }
        return AppPlan();
    }
    GenerationKeeperPtr createGenerationKeeper(const proto::BuildId& buildId) override
    {
        std::string amonPortStr = autil::StringUtil::toString(_amonitorPort);
        GenerationKeeperPtr generationKeeper(new MockGenerationKeeper(buildId, _zkRoot, _adminServiceName, amonPortStr,
                                                                      _zkWrapper, ProhibitedIpCollectorPtr(),
                                                                      _useFakeGenerationKeeper));
        return generationKeeper;
    }

private:
    MOCK_CONST_METHOD3(createSimpleMasterScheduler,
                       SimpleMasterScheduler*(const std::string&, worker_framework::LeaderChecker*,
                                              const std::string&));
    //    MOCK_METHOD2(recover, bool(const std::string&, cm_basic::ZkWrapper*));
    // AppPlan generateAppPlan(const GenerationKeepers &generationKeepers,
    //                       const std::vector<std::string> &prohibitedIps) {
    //    return AppPlan();
    //}

public:
    static SimpleMasterScheduler* createMockScheduler() { return new NiceMockSimpleMasterScheduler(); }
    void setMockAppPlan(bool needMock) { _needAppPlan = needMock; }
    void setRecover(bool needRecover) { _needRecover = needRecover; }

private:
    bool _needRecover;
    bool _needAppPlan;
    bool _useFakeGenerationKeeper;
};

typedef ::testing::StrictMock<MockServiceKeeper> StrictMockServiceKeeper;
typedef ::testing::NiceMock<MockServiceKeeper> NiceMockServiceKeeper;

}} // namespace build_service::admin

#endif // ISEARCH_BS_MOCKSERVICEKEEPER_H
