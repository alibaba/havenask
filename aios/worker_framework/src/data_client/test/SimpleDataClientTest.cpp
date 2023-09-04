#ifndef WORKER_FRAMEWORK_SIMPLEDATACLIENTTEST_H
#define WORKER_FRAMEWORK_SIMPLEDATACLIENTTEST_H

#include "autil/Log.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"
#include "test/test.h"
#include "unittest/unittest.h"
#include "worker_framework/DataClient.h"

using namespace std;
using namespace testing;
using namespace fslib;
using namespace fslib::fs;

namespace worker_framework {

class SimpleDataClientTest : public testing::Test {
protected:
    SimpleDataClientTest() {}

    virtual ~SimpleDataClientTest() {}

    virtual void SetUp() { _testDir = getTestDataPath() + "/simple_data_client_test"; }
    virtual void TearDown() {}

protected:
    string _testDir;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(worker_framework.data_client, SimpleDataClientTest);

TEST_F(SimpleDataClientTest, testGetData) {
    string srcBaseUri = _testDir + "/src/";
    string dstDir = _testDir + "/dst/";
    FileSystem::remove(dstDir);

    vector<string> srcFilePathes;
    srcFilePathes.push_back("file1");
    srcFilePathes.push_back("file2");

    vector<string> expectedFilePathes;
    expectedFilePathes.push_back(dstDir + "file1");
    expectedFilePathes.push_back(dstDir + "file2");
    for (const auto &path : expectedFilePathes) {
        EXPECT_TRUE(FileSystem::isExist(path) == EC_FALSE);
    }

    DataOption dataOption;
    DataClient dataClient(1234);
    DataItemPtr dataItemPtr = dataClient.getData(srcBaseUri, srcFilePathes, dstDir, dataOption);
    EXPECT_EQ(DS_FINISHED, dataItemPtr->getStatus());

    for (const auto &path : expectedFilePathes) {
        EXPECT_TRUE(FileSystem::isExist(path) == EC_TRUE);
    }
}

TEST_F(SimpleDataClientTest, testGetDataWithoutFilePathes) {
    string srcBaseUri = _testDir + "/src/";
    string dstDir = _testDir + "/dst/";
    FileSystem::remove(dstDir);

    vector<string> expectedFilePathes;
    expectedFilePathes.push_back(dstDir + "file1");
    expectedFilePathes.push_back(dstDir + "file2");
    for (const auto &path : expectedFilePathes) {
        EXPECT_TRUE(FileSystem::isExist(path) == EC_FALSE);
    }

    DataOption dataOption;
    DataClient dataClient(1234);
    DataItemPtr dataItemPtr = dataClient.getData(srcBaseUri, dstDir, dataOption);
    EXPECT_EQ(DS_FINISHED, dataItemPtr->getStatus());

    for (const auto &path : expectedFilePathes) {
        EXPECT_TRUE(FileSystem::isExist(path) == EC_TRUE);
    }
}

TEST_F(SimpleDataClientTest, testGetDataWithFileMetas) {
    string srcBaseUri = _testDir + "/src/";
    string dstDir = _testDir + "/dst/";
    FileSystem::remove(dstDir);

    vector<DataFileMeta> srcFileMetas;
    DataFileMeta fileMeta;
    fileMeta.path = "file1";
    fileMeta.modifyTime = 1;
    fileMeta.length = 100;
    srcFileMetas.push_back(fileMeta);
    fileMeta.path = "file2";
    fileMeta.modifyTime = 2;
    fileMeta.length = 200;
    srcFileMetas.push_back(fileMeta);

    vector<string> expectedFilePathes;
    expectedFilePathes.push_back(dstDir + "file1");
    expectedFilePathes.push_back(dstDir + "file2");
    for (const auto &path : expectedFilePathes) {
        EXPECT_TRUE(FileSystem::isExist(path) == EC_FALSE);
    }

    DataOption dataOption;
    DataClient dataClient(1234);
    DataItemPtr dataItemPtr = dataClient.getData(srcBaseUri, srcFileMetas, dstDir, dataOption);
    EXPECT_EQ(DS_FINISHED, dataItemPtr->getStatus());

    for (const auto &path : expectedFilePathes) {
        EXPECT_TRUE(FileSystem::isExist(path) == EC_TRUE);
    }
}

TEST_F(SimpleDataClientTest, testRemoveData) {
    DataClient dataClient(1234);
    string dataPath = _testDir + "/a";
    EXPECT_TRUE(FileSystem::mkDir(dataPath) == EC_OK);
    EXPECT_TRUE(FileSystem::isExist(dataPath) == EC_TRUE);

    dataClient.removeData(dataPath, true);
    EXPECT_TRUE(FileSystem::isExist(dataPath) == EC_FALSE);
}

}; // namespace worker_framework

#endif // WORKER_FRAMEWORK_SIMPLEDATACLIENTTEST_H
