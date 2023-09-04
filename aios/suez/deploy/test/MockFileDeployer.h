#pragma once

#include "suez/deploy/FileDeployer.h"
#include "unittest/unittest.h"

namespace suez {

class MockFileDeployer : public FileDeployer {
public:
    MockFileDeployer() : FileDeployer(nullptr) {}

public:
    MOCK_METHOD1(doDeploy, DeployStatus(const DeployFilesVec &));
};

using NiceMockFileDeployer = ::testing::NiceMock<MockFileDeployer>;

class DeployMockFileDeployer : public FileDeployer {
public:
    DeployMockFileDeployer() : FileDeployer(nullptr) {}

public:
    MOCK_METHOD3(deploy,
                 DeployStatus(const DeployFilesVec &, const std::function<bool()> &, const std::function<bool()> &));
    MOCK_METHOD0(cancel, void());
};

using NiceMockDeployFileDeployer = ::testing::NiceMock<DeployMockFileDeployer>;

} // namespace suez
