#pragma once

#include <gmock/gmock.h>

#include "worker_framework/LeaderedWorkerBase.h"

namespace worker_framework {

class MockLeaderedWorkerBase : public LeaderedWorkerBase {
public:
    MOCK_METHOD0(becomeLeader, void());
    MOCK_METHOD0(noLongerLeader, void());
    MOCK_METHOD(bool, needSlotIdInLeaderInfo, (), (const, override));
    void preempted() override { getLeaderElector()->_leaderLockThreadPtr->_run = false; }
    bool initLeaderElector(const std::string &zkRoot) override {
        return LeaderedWorkerBase::initLeaderElector(zkRoot, 5, LOOP_INTERVAL, "");
    }
};

}; // namespace worker_framework
