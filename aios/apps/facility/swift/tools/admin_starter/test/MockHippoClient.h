#pragma once

#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-nice-strict.h>
#include <gtest/gtest-matchers.h>
#include <string>

#include "hippo/HippoClient.h"
#include "hippo/proto/Client.pb.h"
#include "unittest/unittest.h"

namespace hippo {

class MockHippoClient : public HippoClient {
public:
    MockHippoClient() { ON_CALL(*this, init(_)).WillByDefault(Return(true)); }
    ~MockHippoClient() {}

public:
    MOCK_METHOD1(init, bool(const std::string &));
    MOCK_METHOD2(submitApplication, bool(const proto::SubmitApplicationRequest &, proto::SubmitApplicationResponse *));
    MOCK_METHOD2(stopApplication, bool(const proto::StopApplicationRequest &, proto::StopApplicationResponse *));
    MOCK_METHOD2(forceKillApplication,
                 bool(const proto::ForceKillApplicationRequest &, proto::ForceKillApplicationResponse *));
    MOCK_METHOD2(getServiceStatus, bool(const proto::ServiceStatusRequest &, proto::ServiceStatusResponse *));
    MOCK_METHOD2(getSlaveStatus, bool(const proto::SlaveStatusRequest &, proto::SlaveStatusResponse *));
    MOCK_METHOD2(getAppSummary, bool(const proto::AppSummaryRequest, proto::AppSummaryResponse *));
    MOCK_METHOD2(getAppStatus, bool(const proto::AppStatusRequest &, proto::AppStatusResponse *));
    MOCK_METHOD2(getAppResourcePreference,
                 bool(const proto::ResourcePreferenceRequest &, proto::ResourcePreferenceResponse *));
    MOCK_METHOD2(updateAppResourcePreference,
                 bool(const proto::UpdateResourcePreferenceRequest &, proto::UpdateResourcePreferenceResponse *));
    MOCK_METHOD2(updateApplication, bool(const proto::UpdateApplicationRequest &, proto::UpdateApplicationResponse *));
    MOCK_METHOD2(addQueue, bool(const proto::AddQueueRequest &, proto::AddQueueResponse *));
    MOCK_METHOD2(delQueue, bool(const proto::DelQueueRequest &, proto::DelQueueResponse *));
    MOCK_METHOD2(updateQueue, bool(const proto::UpdateQueueRequest &, proto::UpdateQueueResponse *));
    MOCK_METHOD2(getQueueStatus, bool(const proto::QueueStatusRequest &, proto::QueueStatusResponse *));
    MOCK_METHOD2(freezeApplication, bool(const proto::FreezeApplicationRequest &, proto::FreezeApplicationResponse *));
    MOCK_METHOD2(unfreezeApplication,
                 bool(const proto::UnFreezeApplicationRequest &, proto::UnFreezeApplicationResponse *));
    MOCK_METHOD2(freezeAllApplication,
                 bool(const proto::FreezeAllApplicationRequest &, proto::FreezeAllApplicationResponse *));
    MOCK_METHOD2(unfreezeAllApplication,
                 bool(const proto::UnFreezeAllApplicationRequest &, proto::UnFreezeAllApplicationResponse *));
    MOCK_METHOD2(updateAppMaster, bool(const proto::UpdateAppMasterRequest &, proto::UpdateAppMasterResponse *));
    MOCK_METHOD2(updateMasterScheduleSwitch,
                 bool(const proto::UpdateMasterOptionsRequest &, proto::UpdateMasterOptionsResponse *));
};

typedef ::testing::StrictMock<MockHippoClient> StrictMockHippoClient;
typedef ::testing::NiceMock<MockHippoClient> NiceMockHippoClient;

}; // namespace hippo
