#include "suez/deploy/DeployManager.h"

#include "suez/deploy/LocalDeployItem.h"
#include "suez/deploy/NormalDeployItem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class DeployManagerTest : public TESTBASE {};

TEST_F(DeployManagerTest, testDedup) {
    DeployFilesVec deployFilesVec1;
    deployFilesVec1.push_back(DeployFiles{"path1", "path2"});
    DeployFilesVec deployFilesVec2;
    deployFilesVec2.push_back(DeployFiles{"path3", "path4"});
    DeployManager manager(nullptr, nullptr);
    auto item = manager.deploy(deployFilesVec1);
    EXPECT_EQ(item, manager.deploy(deployFilesVec1));
    EXPECT_EQ(1u, manager._deployFilesSet.size());
    manager.deploy(deployFilesVec2);
    EXPECT_EQ(2u, manager._deployFilesSet.size());

    manager.erase(deployFilesVec1);
    EXPECT_NE(item, manager.deploy(deployFilesVec1));
}

TEST_F(DeployManagerTest, testLocalDeploy) {
    DeployManager manager(nullptr, nullptr, false);
    DeployManager managerLocal(nullptr, nullptr, true);

    DeployFilesVec deployFilesVec;
    deployFilesVec.push_back(DeployFiles{"path1", "path2"});

    auto item = manager.deploy(deployFilesVec);
    auto itemLocal = managerLocal.deploy(deployFilesVec);

    ASSERT_NE(nullptr, dynamic_pointer_cast<NormalDeployItem>(item));
    ASSERT_EQ(nullptr, dynamic_pointer_cast<LocalDeployItem>(item));

    ASSERT_EQ(nullptr, dynamic_pointer_cast<NormalDeployItem>(itemLocal));
    ASSERT_NE(nullptr, dynamic_pointer_cast<LocalDeployItem>(itemLocal));
}

} // namespace suez
