#include "autil/legacy/jsonizable.h"
#include "build_service/admin/BuildTaskValidator.h"
#include "build_service/admin/CounterFileCollector.h"
#include "build_service/admin/CounterRedisCollector.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/ProcessorCheckpointFormatter.h"
#include "build_service/admin/ProhibitedIpsTable.h"
#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/admin/test/FakeGenerationTask.h"
#include "build_service/admin/test/FileUtilForTest.h"
#include "build_service/admin/test/GenerationTaskStateMachine.h"
#include "build_service/admin/test/MockGenerationKeeper.h"
#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/common/CounterRedisSynchronizer.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/TaskIdentifier.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/test/unittest.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RedisClient.h"
#include "fslib/util/FileUtil.h"
#include "hippo/proto/Common.pb.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::index_base;
using namespace worker_framework;

using namespace build_service::util;
using namespace build_service::common;
using namespace build_service::config;
using namespace build_service::proto;

namespace build_service { namespace admin {

class ImportGenerationInteTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp()
    {
        string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/lua.conf";
        setenv("control_flow_config_path", filePath.c_str(), true);

        _zkRoot = GET_TEMP_DATA_PATH();
        _indexRoot = GET_TEMP_DATA_PATH();
        setenv("BUILD_SERVICE_TEST_INDEX_PATH_PREFIX", _indexRoot.c_str(), true);

        _configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
        _buildId.set_appname("app");
        _buildId.set_generationid(1);
        _buildId.set_datatable("simple");
        _adminServiceName = "bsImportGenerationTestApp";
        _zkWrapper = new cm_basic::ZkWrapper();
        _importedVersionId = 54321;
        // _generationKeeper.reset(new MockGenerationKeeper(_buildId, _zkRoot, "", "10086",
        //                 _zkWrapper, _table.createCollector(), true));
    }
    void tearDown()
    {
        DELETE_AND_SET_NULL(_zkWrapper);
        unsetenv("control_flow_config_path");
        unsetenv("BUILD_SERVICE_TEST_INDEX_PATH_PREFIX");
    }

protected:
    MockGenerationKeeperPtr PrepareGenerationKeeper(const string& zkRoot, const BuildId& buildId,
                                                    const string& configPath, const string& fakeIndexRoot,
                                                    const string& buildMode,
                                                    indexlib::versionid_t versionId = indexlibv2::INVALID_VERSIONID)
    {
        MockGenerationKeeperPtr keeper(
            new MockGenerationKeeper(buildId, zkRoot, "", "10086", _zkWrapper, _table.createCollector(), true));

        keeper->setFakeIndexRoot(fakeIndexRoot);
        StartBuildRequest request;
        request.set_configpath(configPath);
        request.set_datadescriptionkvs(
            "[{\"src\":\"full_swift\", \"type\":\"swift\", \"swift_root\": \"swift_root\", \"topic_name\":\"simple\", "
            "\"swift_start_timestamp\":\"1689649740\", \"swift_stop_timestamp\": \"1689649750\"},{\"src\":\"swift\", "
            "\"type\":\"swift\", \"swift_root\": \"swift_root\", \"topic_name\":\"simple\", "
            "\"swift_start_timestamp\":\"1689649750\"}]");
        request.set_buildmode(buildMode);
        request.set_debugmode("step");
        *request.mutable_buildid() = buildId;
        if (versionId != indexlibv2::INVALID_VERSIONID) {
            KeyValueMap kvMap;
            kvMap["importedVersionId"] = std::to_string(versionId);
            request.set_extendparameters(ToJsonString(kvMap));
        }

        StartBuildResponse response;
        keeper->startBuild(&request, &response);
        if (response.errormessage_size() == 0) {
            keeper->run(1);
            return keeper;
        }
        return nullptr;
    }

    void CheckLatestPublishedVersion(const MockGenerationKeeperPtr& keeper, size_t expectNodeSize,
                                     indexlib::versionid_t expectPublishedVersionId)
    {
        ASSERT_TRUE(keeper);
        auto task = dynamic_cast<GenerationTask*>(keeper->_generationTask);
        ASSERT_TRUE(task);
        GenerationTaskStateMachine machine("simple", keeper->_workerTable, task);
        machine.makeDecision(2);
        auto nodes = machine.getWorkerNodes("cluster1", ROLE_TASK);
        ASSERT_EQ(expectNodeSize, nodes.size());

        config::TaskTarget target;
        FromJsonString(target, ((proto::TaskNode*)nodes[0].get())->getTargetStatus().targetdescription());
        proto::BuildTaskTargetInfo targetInfo;
        std::string targetInfoStr;
        target.getTargetDescription(BS_BUILD_TASK_TARGET, targetInfoStr);
        FromJsonString(targetInfo, targetInfoStr);
        ASSERT_EQ(expectPublishedVersionId, targetInfo.latestPublishedVersion);
    }

    void checkVersion(const std::string& partIndexRoot, versionid_t sourceVersionId, versionid_t targetVersionId,
                      const segmentid_t lastSegmentId)
    {
        Version sourceVersion, targetVersion;
        VersionLoader::GetVersionS(partIndexRoot, sourceVersion, sourceVersionId);
        VersionLoader::GetVersionS(partIndexRoot, targetVersion, targetVersionId);

        ASSERT_EQ(sourceVersion.GetFormatVersion(), targetVersion.GetFormatVersion());
        ASSERT_EQ(lastSegmentId, targetVersion.GetLastSegment());
        ASSERT_EQ(sourceVersion.GetLevelInfo(), targetVersion.GetLevelInfo());
        ASSERT_EQ(sourceVersion.GetLocator(), targetVersion.GetLocator());
        ASSERT_EQ(sourceVersion.GetSegmentVector(), targetVersion.GetSegmentVector());
        ASSERT_EQ(sourceVersion.GetTimestamp(), targetVersion.GetTimestamp());
        ASSERT_EQ(targetVersionId, targetVersion.GetVersionId());
    }
    bool getZkStatus(const string& content, const string& nodePath, string& result);
    void checkZkStatus(const string& content, const string& nodePath, const string& result);
    void checkCheckpointFile(const string& generationDir, const string& clusterName, const versionid_t versionId,
                             bool expectedExist)
    {
        string filePath = PathDefine::getReservedCheckpointFileName(generationDir, clusterName);
        bool isExist;
        ASSERT_TRUE(fslib::util::FileUtil::isExist(filePath, isExist));
        string content;
        fslib::util::FileUtil::readFile(filePath, content);
        CheckpointList ccpList;
        FromJsonString(ccpList, content);
        EXPECT_EQ(ccpList.check(versionId), expectedExist) << versionId;
    };

    // filePaths : file1;file2;file3
    void CreateFiles(const indexlib::file_system::DirectoryPtr& dir, const string& filePaths)
    {
        vector<string> fileNames;
        fileNames = StringUtil::split(filePaths, ";");
        for (size_t i = 0; i < fileNames.size(); ++i) {
            static std::string content("");
            auto fileWriter = dir->CreateFileWriter(fileNames[i]);
            fileWriter->Write(content.c_str(), content.size()).GetOrThrow();
            ;
            fileWriter->Close().GetOrThrow();
        }
    }

    void createVersion(const std::string& partitionRoot, versionid_t versionId,
                       const std::vector<segmentid_t>& segmentIds, int64_t timestamp)
    {
        indexlib::index_base::Version version(versionId, timestamp);
        auto fs = indexlib::file_system::FileSystemCreator::Create(
                      "bs_ut", partitionRoot /*, indexlib::file_system::FileSystemOptions::Offline()*/)
                      .GetOrThrow();
        auto partDir = indexlib::file_system::Directory::Get(fs);
        for (auto& segId : segmentIds) {
            string segmentDir = partitionRoot + "/segment_" + StringUtil::toString(segId) + "_level_0";
            fslib::util::FileUtil::mkDir(segmentDir, true);
            auto segDir = partDir->MakeDirectory(segmentDir);
            CreateFiles(segDir, "file1;file2;file3");
            SegmentFileListWrapper::Dump(segDir, "");
            version.AddSegment(segId);
        }
        version.Store(partDir, true);
    }

    void finishFullProcessor(GenerationKeeperPtr& gkeeper)
    {
        auto& processorNodes = gkeeper->_workerTable->getWorkerNodes("fd_full_processor.fullProcessor");
        GenerationTask* generationTask = dynamic_cast<GenerationTask*>(gkeeper->_generationTask);
        bool ret = false;
        for (size_t i = 0; i < 100; i++) {
            for (auto& node : processorNodes) {
                node->setFinished(true);
            }
            InformResponse informResponse;
            gkeeper->stepBuild(&informResponse);
            auto flow = generationTask->getFlow("fd_full_processor");
            if (flow->getStatus() == TaskFlow::TF_STATUS::tf_finish) {
                ret = true;
                break;
            }
        }
        ASSERT_TRUE(ret);
    }

protected:
    template <typename CurrentType>
    void setSuspendedStatus(WorkerNodeBase* node)
    {
        CurrentType current;
        string currentStr;
        current.set_issuspended(true);
        current.set_protocolversion(0);
        current.SerializeToString(&currentStr);
        node->setCurrentStatusStr(currentStr, "");
    }
    template <typename CurrentType>
    void setVersion(WorkerNodeBase* node, int versionId, const string& configPath)
    {
        CurrentType current;
        string currentStr;
        current.set_protocolversion(versionId);
        current.set_configpath(configPath);
        current.SerializeToString(&currentStr);
        node->setCurrentStatusStr(currentStr, "");
    }

protected:
    string _zkRoot;
    string _indexRoot;
    string _configPath;
    BuildId _buildId;
    cm_basic::ZkWrapper* _zkWrapper;
    // MockGenerationKeeperPtr _generationKeeper;
    string _adminServiceName;
    ProhibitedIpsTable _table;
    indexlib::versionid_t _importedVersionId;
};

TEST_F(ImportGenerationInteTest, testImportBuild)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");

    // MockGenerationKeeperPtr keeper0(new MockGenerationKeeper(
    //                                     _buildId, _zkRoot, "", "10086",
    //                                     _zkWrapper, _table.createCollector(), true));
    // keeper0->setFakeIndexRoot(_zkRoot);
    // StartBuildRequest request;
    // request.set_configpath(_configPath);
    // request.set_datadescriptionkvs("[{\"src\":\"file\", \"type\":\"file\"},{\"src\":\"swift\",
    // \"type\":\"swift\"}]"); request.set_buildmode("full"); request.set_debugmode("step"); *request.mutable_buildid()
    // = _buildId; StartBuildResponse response; keeper0->startBuild(&request, &response);

    ASSERT_TRUE(keeper0);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask);

    string indexRoot = generationTask->_indexRoot;
    string clusterIndexRoot =
        IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
    string signatureFilePath =
        fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
    bool existFlag = false;
    ASSERT_TRUE(fslib::util::FileUtil::isExist(signatureFilePath, existFlag));
    ASSERT_TRUE(existFlag);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    {
        // test import failed if generation is not stopped
        string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
        ;
        auto keeper1 =
            PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_FALSE(keeper1);
    }
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    {
        string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
        ;
        auto keeper1 =
            PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_TRUE(keeper1);
        {
            ASSERT_TRUE(fslib::util::FileUtil::removeIfExist(signatureFilePath));
            MockGenerationKeeperPtr recoveredKeeper(new MockGenerationKeeper(
                _buildId, importedZkRoot, "", "10086", _zkWrapper, _table.createCollector(), true));
            recoveredKeeper->setFakeIndexRoot(_indexRoot);
            ASSERT_TRUE(recoveredKeeper->recover());
            auto task = dynamic_cast<GenerationTask*>(recoveredKeeper->_generationTask);
            ASSERT_TRUE(task->_isImportedTask);
            string content;
            ASSERT_TRUE(fslib::util::FileUtil::readFile(signatureFilePath, content));
            BuildTaskValidator::BuildTaskSignature signature;
            ASSERT_NO_THROW(FromJsonString(signature, content));
            string expectZkRoot = PathDefine::getGenerationZkRoot(importedZkRoot, _buildId);
            ASSERT_EQ(expectZkRoot, signature.zkRoot);
        }
        keeper1->stopBuild();
        keeper1->run(2);
        string generationStopFile = PathDefine::getGenerationStatusFile(importedZkRoot, _buildId) + ".stopped";
        ASSERT_TRUE(FileUtilForTest::checkPathExist(generationStopFile));
        existFlag = false;
        ASSERT_TRUE(fslib::util::FileUtil::isExist(signatureFilePath, existFlag));
        ASSERT_FALSE(existFlag);
    }
}

TEST_F(ImportGenerationInteTest, testStopAndImport)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    {
        auto keeper1 =
            PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_TRUE(keeper1);

        auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
        string indexRoot = task->_indexRoot;
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
        string signatureFilePath =
            fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));

        GenerationTaskStateMachine machine("simple", keeper1->_workerTable, task);
        machine.makeDecision(2);
        auto nodes = machine.getWorkerNodes("cluster1", ROLE_TASK);
        ASSERT_EQ(2u, nodes.size());
        string targetStr, host;
        nodes[0]->getTargetStatusStr(targetStr, host);
        BuilderTarget target;
        target.ParseFromString(targetStr);
        // EXPECT_TRUE(target.has_workerpathversion());
        // EXPECT_TRUE(target.workerpathversion() >= 0x100000);
    }
}

TEST_F(ImportGenerationInteTest, testValidateSignatureFile)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    string indexRoot = generationTask->_indexRoot;
    string clusterIndexRoot =
        IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
    string signatureFilePath =
        fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);

    string realtimeInfoPath = fslib::util::FileUtil::joinFilePath(clusterIndexRoot, "realtime_info.json");
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";

    EXPECT_TRUE(FileUtilForTest::checkPathExist(realtimeInfoPath))
        << "realtime_info path not existed:" << realtimeInfoPath;

    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    ASSERT_FALSE(FileUtilForTest::checkPathExist(signatureFilePath));
    {
        // import fail if signature.zkRoot not match
        BuildTaskValidator::BuildTaskSignature badSignature;
        badSignature.zkRoot = "zfs://some-one-else";
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(signatureFilePath, ToJsonString(badSignature)));
        auto keeper1 =
            PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_FALSE(keeper1);
    }
    {
        ASSERT_TRUE(fslib::util::FileUtil::removeIfExist(signatureFilePath));
        string realtimeInfoPath = fslib::util::FileUtil::joinFilePath(clusterIndexRoot, "realtime_info.json");
        EXPECT_TRUE(FileUtilForTest::checkPathExist(realtimeInfoPath))
            << "realtime_info path not existed:" << realtimeInfoPath;
        auto keeper1 =
            PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_TRUE(keeper1);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
        string content;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(signatureFilePath, content));
        BuildTaskValidator::BuildTaskSignature signature;
        ASSERT_NO_THROW(FromJsonString(signature, content));
        string expectZkRoot = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
        ASSERT_EQ(expectZkRoot, signature.zkRoot);
    }
}

TEST_F(ImportGenerationInteTest, testImportFailIfSourceIndexIsEmpty)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);

    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    {
        // empty index: import failed
        auto keeper1 =
            PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_FALSE(keeper1);
    }
    // restart and make fake index
    keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    keeper0->stopBuild();
    keeper0->run(2);

    {
        auto keeper1 =
            PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_TRUE(keeper1);

        auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
        string indexRoot = task->_indexRoot;
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
        string signatureFilePath =
            fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
    }
}

// npc mode do not support multiple cluster
//  TEST_F(ImportGenerationInteTest, testImportWithOneCluster)
//  {
//      // simple2 has two clusters: "cluster1" and "cluster2"
//      BuildId buildId;
//      buildId.set_appname("app");
//      buildId.set_generationid(1);
//      buildId.set_datatable("simple2");

//     string configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
//     auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "full");
//     ASSERT_TRUE(keeper0);
//     auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
//     ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

//     keeper0->stopBuild();
//     keeper0->run(2);

//     {
//         // simple has only one cluster: "cluster1"
//         BuildId buildId1;
//         buildId1.set_appname("app");
//         buildId1.set_generationid(1);
//         buildId1.set_datatable("simple");
//         string singleClusterConfigPath = GET_TEST_DATA_PATH() +
//         "admin_test/config_with_control_config_partition_count_2"; string importedZkRoot = GET_TEMP_DATA_PATH() +
//         "/importedZk";
//         ;
//         auto keeper1 = PrepareGenerationKeeper(importedZkRoot, buildId1, singleClusterConfigPath, _indexRoot,
//         "import"); ASSERT_TRUE(keeper1); auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask); string
//         indexRoot = task->_indexRoot;
//         {
//             string clusterIndexRoot =
//                 IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
//             string signatureFilePath =
//                 fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
//             ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
//         }
//         {
//             string clusterIndexRoot =
//                 IndexPathConstructor::constructGenerationPath(indexRoot, "cluster2", _buildId.generationid());
//             string signatureFilePath =
//                 fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
//             ASSERT_FALSE(FileUtilForTest::checkPathExist(signatureFilePath));
//         }
//     }
// }

TEST_F(ImportGenerationInteTest, testImportFailIfIncProcessorExists)
{
    // simple3 has one clusters: "cluster1", isIncProcessorExist == true, but ControlConfig has override
    // isIncProcessorExist to false
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple3");

    string configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    // since isIncProcessorExist is overided to false, signature exists and import can be succeed
    string indexRoot = generationTask->_indexRoot;
    string clusterIndexRoot =
        IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", buildId.generationid());
    string signatureFilePath =
        fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
    ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));

    keeper0->stopBuild();
    keeper0->run(2);
    {
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "import", _importedVersionId);
        ASSERT_TRUE(keeper1);
    }
}

TEST_F(ImportGenerationInteTest, testImportFailIfPartitionCountMismatch)
{
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple");

    string configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    keeper0->stopBuild();
    keeper0->run(2);

    {
        // test import fail if partition_count not match
        BuildId buildId1;
        buildId1.set_appname("app");
        buildId1.set_generationid(1);
        buildId1.set_datatable("simple");
        string newConfig = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_4";
        string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
        ;
        auto keeper1 =
            PrepareGenerationKeeper(importedZkRoot, buildId1, newConfig, _indexRoot, "import", _importedVersionId);
        ASSERT_FALSE(keeper1);
    }
}

TEST_F(ImportGenerationInteTest, testImportFailIfRealtimeInfoMismatch)
{
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple");

    string configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    keeper0->stopBuild();
    keeper0->run(2);

    {
        // test import fail if realtime info not match
        MockGenerationKeeperPtr keeper(
            new MockGenerationKeeper(buildId, _zkRoot, "", "10086", _zkWrapper, _table.createCollector(), true));

        keeper->setFakeIndexRoot(_indexRoot);
        StartBuildRequest request;
        request.set_configpath(configPath);
        request.set_datadescriptionkvs("[{\"src\":\"file\", \"type\":\"file\"},{\"src\":\"swift\", \"type\":\"swift\", "
                                       "\"some-new-key\":\"some-new-value\"}]");
        request.set_buildmode("import");
        request.set_debugmode("step");
        *request.mutable_buildid() = buildId;

        StartBuildResponse response;
        keeper->startBuild(&request, &response);
        ASSERT_NE(0, response.errormessage_size());
    }
}

TEST_F(ImportGenerationInteTest, testGenerationKeeperConstructor)
{
    std::shared_ptr<GenerationKeeper> keeper(
        new GenerationKeeper({_buildId, _zkRoot, "", "10086", _zkWrapper, _table.createCollector(), NULL}));
    ASSERT_FALSE(keeper->_needSyncStatus);

    MockGenerationKeeperPtr keeper2(
        new MockGenerationKeeper(_buildId, _zkRoot, "", "10086", _zkWrapper, _table.createCollector(), true));
    ASSERT_FALSE(keeper2->_needSyncStatus);
}

TEST_F(ImportGenerationInteTest, testImportFromCheckpointVersion)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, 8, indexlibv2::INVALID_VERSIONID);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    auto keeper1 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
    ASSERT_TRUE(keeper1);

    auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
    string indexRoot = task->_indexRoot;
    string clusterIndexRoot =
        IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
    string signatureFilePath =
        fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
    ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));

    CheckLatestPublishedVersion(keeper1, 2, _importedVersionId);
}

TEST_F(ImportGenerationInteTest, testImportWithInvalidVersionId)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, 8, indexlibv2::INVALID_VERSIONID);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    auto keeper1 =
        PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", indexlibv2::INVALID_VERSIONID);
    ASSERT_FALSE(keeper1);

    auto keeper2 =
        PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import", /*invalid version*/ -2);
    ASSERT_FALSE(keeper2);
}

TEST_F(ImportGenerationInteTest, testImportEncounterRecover)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, 8, indexlibv2::INVALID_VERSIONID);

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));

    string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
    auto keeper1 =
        PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import", _importedVersionId);
    ASSERT_TRUE(keeper1);
    CheckLatestPublishedVersion(keeper1, 2, _importedVersionId);
    keeper1.reset();

    // keeper1 has published checkpoint, recoveredKeeper can continue with checkpoint
    MockGenerationKeeperPtr recoveredKeeper(
        new MockGenerationKeeper(_buildId, importedZkRoot, "", "10086", _zkWrapper, _table.createCollector(), true));
    recoveredKeeper->setFakeIndexRoot(_indexRoot);
    ASSERT_TRUE(recoveredKeeper->recover());
    auto task = dynamic_cast<GenerationTask*>(recoveredKeeper->_generationTask);
    ASSERT_TRUE(task->_isImportedTask);
    CheckLatestPublishedVersion(recoveredKeeper, 2, _importedVersionId);
}
}} // namespace build_service::admin
