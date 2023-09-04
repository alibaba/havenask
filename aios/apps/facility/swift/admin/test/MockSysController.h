#pragma once

#include "gmock/gmock.h"

#include "swift/admin/SysController.h"

namespace swift {
namespace admin {

class MockSysController : public SysController {
public:
    MockSysController(config::AdminConfig *config, monitor::AdminMetricsReporter *reporter)
        : SysController(config, reporter) {}
    ~MockSysController() {}

private:
    MOCK_METHOD1(doUpdateLeaderInfo, bool(const std::vector<protocol::AdminInfo> &));
    MOCK_METHOD2(checkLeaderInfoDiff, bool(const std::string &path, bool isJson));
    MOCK_METHOD2(changeSlot, void(const protocol::ChangeSlotRequest *, protocol::ChangeSlotResponse *));
    MOCK_METHOD1(updateBrokerWorkerStatusForEmptyTarget, void(WorkerMap &));
};

typedef ::testing::StrictMock<MockSysController> StrictMockSysController;
typedef ::testing::NiceMock<MockSysController> NiceMockSysController;

} // namespace admin
} // namespace swift
