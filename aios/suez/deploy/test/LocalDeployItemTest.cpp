#include "suez/deploy/LocalDeployItem.h"

#include <limits>

#include "fslib/fs/FileSystem.h"
#include "suez/common/DiskQuotaController.h"
#include "unittest/unittest.h"

using namespace std;
using namespace fslib::fs;
using namespace testing;

namespace suez {

class LocalDeployItemTest : public TESTBASE {
public:
    void setUp() {
        remotePath = GET_TEST_DATA_PATH() + "table_test/table_deployer/index/";
        localPath = GET_TEMP_DATA_PATH() + "local/";
    }

    void checkDeployDone(LocalDeployItem &item) {
        for (auto &deployFiles : item._deployFilesVec) {
            std::vector<std::string> remoteFiles, localFiles;
            int64_t remoteFilesSize, localFilesSize;

            DeployItem::getFilesAndSize(deployFiles.sourceRootPath, remoteFiles, remoteFilesSize);
            DeployItem::getFilesAndSize(deployFiles.targetRootPath, localFiles, localFilesSize);

            ASSERT_NE(0, localFiles.size());
            ASSERT_EQ(remoteFiles, localFiles);
            ASSERT_EQ(remoteFilesSize, localFilesSize);
        }
    }

private:
    string localPath;
    string remotePath;
};

TEST_F(LocalDeployItemTest, testDeploy) {
    string remotePath1 = remotePath + "item/";
    string localPath1 = localPath + "item/";
    string remotePath2 = remotePath + "model_table/";
    string localPath2 = localPath + "model_table/";

    DiskQuotaController diskQuotaController;
    DeployResource deployResource{nullptr, &diskQuotaController};
    DeployFilesVec deployFilesVec{{remotePath1, localPath1}, {remotePath2, localPath2}};

    LocalDeployItem item(deployResource, deployFilesVec);

    // diskQuota
    diskQuotaController.setQuota(1);
    auto ret = item.deployAndWaitDone();
    ASSERT_EQ(DS_DISKQUOTA, ret);

    // deploy done
    diskQuotaController.setQuota(numeric_limits<int64_t>::max());
    ret = item.deployAndWaitDone();
    ASSERT_EQ(DS_DEPLOYDONE, ret);
    checkDeployDone(item);

    // deploy cancel
    item._needCancel = true;
    ret = item.deployAndWaitDone();
    ASSERT_EQ(DS_CANCELLED, ret);

    // deploy again
    ret = item.deployAndWaitDone();
    ASSERT_EQ(DS_DEPLOYDONE, ret);
    checkDeployDone(item);
}

TEST_F(LocalDeployItemTest, testDeployFail) {
    string fakeRemotePath = GET_TEST_DATA_PATH() + "fakePath/";

    DiskQuotaController diskQuotaController;
    DeployResource deployResource{nullptr, &diskQuotaController};
    DeployFilesVec deployFilesVec{{fakeRemotePath, localPath}};

    LocalDeployItem item(deployResource, deployFilesVec);
    auto ret = item.deployAndWaitDone();
    ASSERT_EQ(DS_FAILED, ret);
}

} // namespace suez
