#pragma once

#include "gmock/gmock.h"

#include "worker_framework/LeaderLocator.h"

namespace worker_framework {

class MockLeaderLocator : public LeaderLocator {
public:
    MOCK_METHOD3(init, bool(cm_basic::ZkWrapper *zk, const std::string &path, const std::string &baseName));
    MOCK_METHOD0(doGetLeaderAddr, std::string());
    MOCK_METHOD0(getLeaderAddr, std::string());

    bool init_P(cm_basic::ZkWrapper *zk, const std::string &path, const std::string &baseName) {
        return LeaderLocator::init(zk, path, baseName);
    }
};

typedef std::shared_ptr<MockLeaderLocator> MockLeaderLocatorPtr;

}; // namespace worker_framework
