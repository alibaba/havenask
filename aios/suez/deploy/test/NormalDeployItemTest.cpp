#include "suez/deploy/NormalDeployItem.h"

#include <thread>

#include "MockDataClient.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace fslib::fs;
using namespace testing;

namespace suez {

class NormalDeployItemTest : public TESTBASE {};

TEST_F(NormalDeployItemTest, testDeploy) {
    string remoteConfigPath = GET_TEMPLATE_DATA_PATH() + "/remote/12345";
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(remoteConfigPath, true));
    string retLocalPath = GET_TEMPLATE_DATA_PATH() + "/12345/";
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(retLocalPath, true));

    DeployFilesVec deployFilesVec;
    deployFilesVec.push_back(DeployFiles{remoteConfigPath, retLocalPath});
    shared_ptr<StrictMockDataClient> mockDataClient(new StrictMockDataClient);
    NormalDeployItem deployItem(
        DeployResource{shared_ptr<DataClient>(mockDataClient), nullptr}, DataOption(), deployFilesVec);
    DataItemPtr dataItem(new DataItem("", "", DataOption()));

    // deploy failed
    EXPECT_CALL(*mockDataClient, getData(remoteConfigPath, vector<string>(), retLocalPath, _))
        .WillOnce(Return(dataItem));
    dataItem->setStatus(worker_framework::DS_FAILED);
    vector<DataItemPtr> dataItems;
    EXPECT_EQ(DS_UNKNOWN, deployItem.deploy(dataItems));
    EXPECT_FALSE(deployItem._dataItems.empty());
    EXPECT_EQ(DS_FAILED, deployItem.waitDone(dataItems));
    deployItem._dataItems.clear();

    // deploy failed
    EXPECT_CALL(*mockDataClient, getData(remoteConfigPath, vector<string>(), retLocalPath, _))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(nullptr));
    dataItems.clear();
    EXPECT_EQ(DS_FAILED, deployItem.deploy(dataItems));
    EXPECT_TRUE(deployItem._dataItems.empty());
    EXPECT_EQ(DS_FAILED, deployItem.deployAndWaitDone());
    EXPECT_TRUE(deployItem._dataItems.empty());

    // other deploy status
    EXPECT_CALL(*mockDataClient, getData(remoteConfigPath, vector<string>(), retLocalPath, _))
        .WillOnce(Return(dataItem));
    dataItem->setStatus(worker_framework::DS_CANCELED);
    EXPECT_EQ(DS_CANCELLED, deployItem.deployAndWaitDone());
    EXPECT_TRUE(deployItem._dataItems.empty());

    // deploy success, mark success
    dataItem->setStatus(worker_framework::DS_FINISHED);
    EXPECT_CALL(*mockDataClient, getData(remoteConfigPath, vector<string>(), retLocalPath, _))
        .WillOnce(Return(dataItem));
    EXPECT_EQ(DS_DEPLOYDONE, deployItem.deployAndWaitDone());
}

TEST_F(NormalDeployItemTest, testDeployTwice) {
    string remoteConfigPath = GET_TEMPLATE_DATA_PATH() + "/remote/12345";
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(remoteConfigPath, true));
    string retLocalPath = GET_TEMPLATE_DATA_PATH() + "/12345/";
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(retLocalPath, true));

    DeployFilesVec deployFilesVec;
    deployFilesVec.push_back(DeployFiles{remoteConfigPath, retLocalPath});
    shared_ptr<StrictMockDataClient> mockDataClient(new StrictMockDataClient);
    NormalDeployItem deployItem(
        DeployResource{shared_ptr<DataClient>(mockDataClient), nullptr}, DataOption(), deployFilesVec);
    DataItemPtr dataItem(new DataItem("", "", DataOption()));

    std::atomic<bool> deploying = false;
    // deploy unknown
    dataItem->setStatus(worker_framework::DS_UNKNOWN);
    auto returnDataItem = [dataItem, &deploying]() {
        deploying = true;
        return dataItem;
    };
    EXPECT_CALL(*mockDataClient, getData(remoteConfigPath, vector<string>(), retLocalPath, _))
        .WillOnce(Invoke(returnDataItem));
    auto dp1 = std::thread([&deployItem]() { EXPECT_EQ(DS_DEPLOYDONE, deployItem.deployAndWaitDone()); });
    auto dp2 = std::thread([&deployItem]() { EXPECT_EQ(DS_DEPLOYDONE, deployItem.deployAndWaitDone()); });
    while (!deploying) {
        usleep(1000);
    }
    dataItem->setStatus(worker_framework::DS_FINISHED);
    dp2.join();
    dp1.join();
}

TEST_F(NormalDeployItemTest, testDeployCancel) {
    string remoteConfigPath = GET_TEMPLATE_DATA_PATH() + "/remote/12345";
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(remoteConfigPath, true));
    string retLocalPath = GET_TEMPLATE_DATA_PATH() + "/12345/";
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(retLocalPath, true));

    DeployFilesVec deployFilesVec;
    deployFilesVec.push_back(DeployFiles{remoteConfigPath, retLocalPath});
    shared_ptr<StrictMockDataClient> mockDataClient(new StrictMockDataClient);
    NormalDeployItem deployItem(
        DeployResource{shared_ptr<DataClient>(mockDataClient), nullptr}, DataOption(), deployFilesVec);
    shared_ptr<MockDataItem> dataItem(new MockDataItem);
    auto setCancelStatus = [dataItem = dataItem.get()]() { dataItem->setStatus(worker_framework::DS_CANCELED); };
    EXPECT_CALL(*dataItem, doCancel()).WillOnce(Invoke(setCancelStatus));

    std::atomic<bool> deploying = false;
    // deploy unknown
    dataItem->setStatus(worker_framework::DS_UNKNOWN);
    auto returnDataItem = [dataItem, &deploying]() {
        deploying = true;
        return dataItem;
    };
    EXPECT_CALL(*mockDataClient, getData(remoteConfigPath, vector<string>(), retLocalPath, _))
        .WillOnce(Invoke(returnDataItem));
    auto dp1 = std::thread([&deployItem]() { EXPECT_EQ(DS_CANCELLED, deployItem.deployAndWaitDone()); });
    auto dp2 = std::thread([&deployItem, &deploying]() {
        while (!deploying) {
            usleep(1000);
        }
        deployItem.cancel();
    });
    while (!deploying) {
        usleep(1000);
    }
    dp2.join();
    dp1.join();
    EXPECT_TRUE(deployItem._dataItems.empty());
}

TEST_F(NormalDeployItemTest, testCheck) {
    NormalDeployItem deployItem(DeployResource{nullptr, nullptr}, DataOption(), DeployFilesVec());
    EXPECT_EQ(DS_DEPLOYDONE, deployItem.deployAndWaitDone());
}

TEST_F(NormalDeployItemTest, testDoDeployFiles) {
    StrictMockDataClient *mockDataClient = new StrictMockDataClient;
    DeployFilesVec deployFilesVec;
    NormalDeployItem deployItem(
        DeployResource{shared_ptr<DataClient>(mockDataClient), nullptr}, DataOption(), deployFilesVec);
    string remoteIndexPath = GET_TEST_DATA_PATH() + "table_test/table_deployer/index/";
    string retLocalPath;

    DataItemPtr dataItem;
    // localPath empty
    EXPECT_EQ(DS_FAILED, deployItem.doDeployFilesAsync(remoteIndexPath, "", DeployFiles(), DataOption(), dataItem));

    // DataItem NULL
    string localPath = "fakePath";
    EXPECT_CALL(*mockDataClient, getData(remoteIndexPath, vector<string>(), localPath, _))
        .WillOnce(Return(DataItemPtr()));
    EXPECT_EQ(DS_FAILED,
              deployItem.doDeployFilesAsync(remoteIndexPath, localPath, DeployFiles(), DataOption(), dataItem));
}

} // namespace suez
