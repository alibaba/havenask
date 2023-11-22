#include <cstdint>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <stdlib.h>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/BuildTaskValidator.h"
#include "build_service/admin/ClusterCheckpointSynchronizerCreator.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/GenerationTaskBase.h"
#include "build_service/admin/GraphGenerationTask.h"
#include "build_service/admin/ProhibitedIpsTable.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/test/FileUtilForTest.h"
#include "build_service/admin/test/GenerationTaskStateMachine.h"
#include "build_service/admin/test/MockGenerationKeeper.h"
#include "build_service/common/PathDefine.h"
#include "build_service/common_define.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CheckpointList.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "hippo/DriverCommon.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/indexlib.h"
#include "unittest/unittest.h"
#include "worker_framework/LeaderElector.h"

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
        _indexRoot = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "/index/root/");
        setenv("BUILD_SERVICE_TEST_INDEX_PATH_PREFIX", GET_TEMP_DATA_PATH().c_str(), true);

        _configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
        _buildId.set_appname("app");
        _buildId.set_generationid(1);
        _buildId.set_datatable("simple");
        _adminServiceName = "bsImportGenerationTestApp";
        _zkWrapper = new cm_basic::ZkWrapper();
        _clusterName = "cluster1";
        _extendParamStr = "{\"importedVersions\":{\"cluster1\":20}}";
        _importedVersionId = 20;
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
                                                    const string& buildMode, bool useFakeGenerationTask = true,
                                                    const std::string& extendParamStr = "")
    {
        MockGenerationKeeperPtr keeper(new MockGenerationKeeper(buildId, zkRoot, "", "10086", _zkWrapper,
                                                                _table.createCollector(), useFakeGenerationTask));

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
        if (!extendParamStr.empty()) {
            request.set_extendparameters(extendParamStr);
        }
        StartBuildResponse response;
        keeper->startBuild(&request, &response);
        if (response.errormessage_size() == 0) {
            keeper->run(1);
            return keeper;
        }
        return nullptr;
    }

    void CheckLatestPublishedVersion(const MockGenerationKeeperPtr& keeper,
                                     const std::map<std::string, vector<int32_t>> expectTargetInfos)
    {
        ASSERT_TRUE(keeper);
        auto task = dynamic_cast<GenerationTask*>(keeper->_generationTask);
        ASSERT_TRUE(task);
        GenerationTaskStateMachine machine("simple", keeper->_workerTable, task);
        machine.makeDecision(2);
        for (const auto& [clusterName, info] : expectTargetInfos) {
            ASSERT_EQ(2, info.size());
            auto nodes = machine.getWorkerNodes(clusterName, ROLE_TASK);
            auto expectNodeSize = info[0];
            auto importedVersionId = info[1];
            ASSERT_EQ(expectNodeSize, nodes.size());
            config::TaskTarget target;
            FromJsonString(target, ((proto::TaskNode*)nodes[0].get())->getTargetStatus().targetdescription());
            proto::BuildTaskTargetInfo targetInfo;
            std::string targetInfoStr;
            target.getTargetDescription(BS_BUILD_TASK_TARGET, targetInfoStr);
            FromJsonString(targetInfo, targetInfoStr);
            ASSERT_EQ(importedVersionId, targetInfo.latestPublishedVersion);
        }
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
    std::string _extendParamStr;
    std::string _clusterName;
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
        IndexPathConstructor::constructGenerationPath(indexRoot, _clusterName, _buildId.generationid());
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
        auto keeper1 = PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
        ASSERT_FALSE(keeper1);
    }
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    {
        string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
        ;
        auto keeper1 = PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
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
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
        ASSERT_TRUE(keeper1);

        auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
        string indexRoot = task->_indexRoot;
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, _clusterName, _buildId.generationid());
        string signatureFilePath =
            fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));

        GenerationTaskStateMachine machine("simple", keeper1->_workerTable, task);
        machine.makeDecision(2);
        auto nodes = machine.getWorkerNodes(_clusterName, ROLE_TASK);
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
        IndexPathConstructor::constructGenerationPath(indexRoot, _clusterName, _buildId.generationid());
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
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
        ASSERT_FALSE(keeper1);
    }
    {
        ASSERT_TRUE(fslib::util::FileUtil::removeIfExist(signatureFilePath));
        string realtimeInfoPath = fslib::util::FileUtil::joinFilePath(clusterIndexRoot, "realtime_info.json");
        EXPECT_TRUE(FileUtilForTest::checkPathExist(realtimeInfoPath))
            << "realtime_info path not existed:" << realtimeInfoPath;
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
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
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
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
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
        ASSERT_TRUE(keeper1);

        auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
        string indexRoot = task->_indexRoot;
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, _clusterName, _buildId.generationid());
        string signatureFilePath =
            fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
    }
}

TEST_F(ImportGenerationInteTest, testImportWithOneCluster)
{
    // simple2 has two clusters: "cluster1" and "cluster2"
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple2");

    string configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());

    keeper0->stopBuild();
    keeper0->run(2);
    {
        // simple has only one cluster: "cluster1"
        BuildId buildId1;
        buildId1.set_appname("app");
        buildId1.set_generationid(1);
        buildId1.set_datatable("simple");
        string singleClusterConfigPath =
            GET_TEST_DATA_PATH() + "admin_test/config_with_control_config_partition_count_2";
        string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
        auto keeper1 = PrepareGenerationKeeper(importedZkRoot, buildId1, singleClusterConfigPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
        ASSERT_TRUE(keeper1);
        auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
        string indexRoot = task->_indexRoot;
        {
            string clusterIndexRoot =
                IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", _buildId.generationid());
            string signatureFilePath =
                fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
            ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
        }
        {
            string clusterIndexRoot =
                IndexPathConstructor::constructGenerationPath(indexRoot, "cluster2", _buildId.generationid());
            string signatureFilePath =
                fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
            ASSERT_FALSE(FileUtilForTest::checkPathExist(signatureFilePath));
        }
    }
}

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
        auto keeper1 = PrepareGenerationKeeper(_zkRoot, buildId, configPath, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
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
        auto keeper1 = PrepareGenerationKeeper(importedZkRoot, buildId1, newConfig, _indexRoot, "import",
                                               /*useFakeGenerationTask*/ true, _extendParamStr);
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
    CheckLatestPublishedVersion(keeper0, {{_clusterName, {/*nodeCount*/ 8, indexlibv2::INVALID_VERSIONID}}});

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));

    string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
    auto keeper1 = PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true, _extendParamStr);
    ASSERT_TRUE(keeper1);

    auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
    string indexRoot = task->_indexRoot;
    string clusterIndexRoot =
        IndexPathConstructor::constructGenerationPath(indexRoot, _clusterName, _buildId.generationid());
    string signatureFilePath =
        fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
    ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));

    CheckLatestPublishedVersion(keeper1, {{_clusterName, {/*nodeCount*/ 2, _importedVersionId}}});
}

TEST_F(ImportGenerationInteTest, testImportMultiClusterFromCheckpointVersion)
{
    // simple2 has two clusters: "cluster1" and "cluster2"
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple2");

    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, {{"cluster1", {/*nodeCount*/ 8, indexlibv2::INVALID_VERSIONID}},
                                          {"cluster2", {/*nodeCount*/ 6, indexlibv2::INVALID_VERSIONID}}});

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));

    string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
    auto keeper1 = PrepareGenerationKeeper(importedZkRoot, buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true,
                                           "{\"importedVersions\":{\"cluster1\":10,\"cluster2\":11}}");
    ASSERT_TRUE(keeper1);

    auto task = dynamic_cast<GenerationTask*>(keeper1->_generationTask);
    string indexRoot = task->_indexRoot;
    {
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, "cluster1", buildId.generationid());
        string signatureFilePath =
            fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
    }
    {
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, "cluster2", buildId.generationid());
        string signatureFilePath =
            fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE);
        ASSERT_TRUE(FileUtilForTest::checkPathExist(signatureFilePath));
    }
    CheckLatestPublishedVersion(keeper1,
                                {{"cluster1", {/*nodeCount*/ 2, /*importedVersionId*/ 10}}, {"cluster2", {10, 11}}});
}

TEST_F(ImportGenerationInteTest, testImportWithInvalidVersionId)
{
    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, _buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, _buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, _buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, {{_clusterName, {/*nodeCount*/ 8, indexlibv2::INVALID_VERSIONID}}});

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));

    string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
    auto keeper1 = PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true, "");
    ASSERT_FALSE(keeper1);

    auto keeper2 = PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true, "{\"importedVersions\":{\"cluster1\":-2}}");
    ASSERT_FALSE(keeper2);

    auto keeper3 = PrepareGenerationKeeper(importedZkRoot, _buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true,
                                           "{\"importedVersions\":{\"cluster1\":10,\"cluster2\":11}}");
    ASSERT_FALSE(keeper3);
}

TEST_F(ImportGenerationInteTest, testGetImportedVersionFromRequest)
{
    {
        proto::StartBuildRequest request;
        request.set_extendparameters("{\"importedVersions\":{\"cluster1\":10,\"cluster2\":11}}");
        GenerationTaskBase::ImportedVersionIdMap importedVersionIdMap;
        ASSERT_TRUE(GenerationKeeper::getImportedVersionFromRequest(&request, &importedVersionIdMap));
        ASSERT_EQ(2, importedVersionIdMap.size());
        ASSERT_EQ(10, importedVersionIdMap["cluster1"]);
        ASSERT_EQ(11, importedVersionIdMap["cluster2"]);
    }

    {
        proto::StartBuildRequest request;
        request.set_extendparameters("{\"importedVersions\":{\"cluster1\":10}}");
        GenerationTaskBase::ImportedVersionIdMap importedVersionIdMap;
        ASSERT_TRUE(GenerationKeeper::getImportedVersionFromRequest(&request, &importedVersionIdMap));
        ASSERT_EQ(1, importedVersionIdMap.size());
        ASSERT_EQ(10, importedVersionIdMap["cluster1"]);
    }

    {
        proto::StartBuildRequest request;
        GenerationTaskBase::ImportedVersionIdMap importedVersionIdMap;
        ASSERT_FALSE(GenerationKeeper::getImportedVersionFromRequest(&request, &importedVersionIdMap));
    }

    {
        proto::StartBuildRequest request;
        request.set_extendparameters("{\"wrongKey\":{\"cluster1\":10,\"cluster2\":11}}");
        GenerationTaskBase::ImportedVersionIdMap importedVersionIdMap;
        ASSERT_FALSE(GenerationKeeper::getImportedVersionFromRequest(&request, &importedVersionIdMap));
    }

    {
        proto::StartBuildRequest request;
        request.set_extendparameters("{\"importedVersions\":{\"cluster1\":10,\"cluster2\":-2}}");
        GenerationTaskBase::ImportedVersionIdMap importedVersionIdMap;
        ASSERT_FALSE(GenerationKeeper::getImportedVersionFromRequest(&request, &importedVersionIdMap));
    }
}

TEST_F(ImportGenerationInteTest, testImportEncounterRecover)
{
    // simple2 has two clusters: "cluster1" and "cluster2"
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple2");

    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, {{"cluster1", {/*nodeCount*/ 8, indexlibv2::INVALID_VERSIONID}},
                                          {"cluster2", {/*nodeCount*/ 6, indexlibv2::INVALID_VERSIONID}}});

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));

    string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
    auto keeper1 = PrepareGenerationKeeper(importedZkRoot, buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true,
                                           "{\"importedVersions\":{\"cluster1\":10,\"cluster2\":11}}");
    ASSERT_TRUE(keeper1);
    CheckLatestPublishedVersion(keeper1,
                                {{"cluster1", {/*nodeCount*/ 2, /*importedVersionId*/ 10}}, {"cluster2", {10, 11}}});
    keeper1.reset();

    // keeper1 has published checkpoint, recoveredKeeper can continue with checkpoint
    MockGenerationKeeperPtr recoveredKeeper(
        new MockGenerationKeeper(buildId, importedZkRoot, "", "10086", _zkWrapper, _table.createCollector(), true));
    recoveredKeeper->setFakeIndexRoot(_indexRoot);
    ASSERT_TRUE(recoveredKeeper->recover());
    auto task = dynamic_cast<GenerationTask*>(recoveredKeeper->_generationTask);
    ASSERT_TRUE(task->_isImportedTask);
    CheckLatestPublishedVersion(recoveredKeeper,
                                {{"cluster1", {/*nodeCount*/ 2, /*importedVersionId*/ 10}}, {"cluster2", {10, 11}}});
}

TEST_F(ImportGenerationInteTest, testRepeatedImport)
{
    BuildId buildId;
    buildId.set_appname("app");
    buildId.set_generationid(1);
    buildId.set_datatable("simple2");

    string generationDir = PathDefine::getGenerationZkRoot(_zkRoot, buildId);
    fslib::util::FileUtil::mkDir(generationDir, true);
    string generationStatusFile = PathDefine::getGenerationStatusFile(_zkRoot, buildId);

    auto keeper0 = PrepareGenerationKeeper(_zkRoot, buildId, _configPath, _indexRoot, "full");
    ASSERT_TRUE(keeper0);
    CheckLatestPublishedVersion(keeper0, {{"cluster1", {/*nodeCount*/ 8, indexlibv2::INVALID_VERSIONID}}});

    auto generationTask = dynamic_cast<GenerationTask*>(keeper0->_generationTask);
    ASSERT_TRUE(generationTask->TEST_prepareFakeIndex());
    keeper0->stopBuild();
    keeper0->run(2);
    string stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    keeper0.reset();

    // import on importedZkRoot for the first time
    string importedZkRoot = GET_TEMP_DATA_PATH() + "/importedZk";
    auto keeper1 = PrepareGenerationKeeper(importedZkRoot, buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true,
                                           "{\"importedVersions\":{\"cluster1\":10,\"cluster2\":11}}");
    ASSERT_TRUE(keeper1);
    CheckLatestPublishedVersion(keeper1, {{"cluster1", {/*nodeCount*/ 2, /*importedVersion*/ 10}}});
    keeper1->stopBuild();
    keeper1->run(2);
    generationStatusFile = PathDefine::getGenerationStatusFile(importedZkRoot, buildId);
    stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));
    keeper1.reset();

    // reset version with a new buildId
    auto CheckResetVersionTargetAndFinish = [](const MockGenerationKeeperPtr& keeper, const std::string clusterName,
                                               const KeyValueMap& expectKvMap) {
        ASSERT_TRUE(keeper);
        auto task = dynamic_cast<GraphGenerationTask*>(keeper->_generationTask);
        ASSERT_TRUE(task);
        keeper->run(5);
        auto nodes = GenerationTaskStateMachine::getWorkerNodes(clusterName, proto::ROLE_TASK, keeper->_workerTable);
        ASSERT_EQ(1, nodes.size());
        ASSERT_EQ(1, nodes.size());
        config::TaskTarget target;
        FromJsonString(target, ((proto::TaskNode*)nodes[0].get())->getTargetStatus().targetdescription());
        for (const auto& [key, value] : expectKvMap) {
            std::string result;
            target.getTargetDescription(key, result);
            ASSERT_EQ(value, result);
        }
        auto workerNodes = keeper->_workerTable->getActiveNodes();
        ASSERT_EQ(2, workerNodes.size());
        GenerationTaskStateMachine::finishTasks(workerNodes);
    };

    std::string resetVersionConfig = GET_TEST_DATA_PATH() + "/graph_reset_version_config";
    auto resetVersionBuildId = proto::ProtoCreator::createBuildId("simple2", 1, "app-reset-version");
    auto resetVersionKeeper = PrepareGenerationKeeper(importedZkRoot, resetVersionBuildId, resetVersionConfig,
                                                      _indexRoot, "customized", /*useFakeGenerationTask*/ false);
    CheckResetVersionTargetAndFinish(resetVersionKeeper, "cluster1",
                                     {{"versionId", "10"},
                                      {"clusterName", "cluster1"},
                                      {"indexRoot", _indexRoot},
                                      {"buildId", proto::ProtoUtil::buildIdToStr(resetVersionBuildId)},
                                      {"partitionCount", "2"}});
    resetVersionKeeper->run(5);
    generationStatusFile = PathDefine::getGenerationStatusFile(importedZkRoot, resetVersionBuildId);
    stopFile = generationStatusFile + ".stopped";
    ASSERT_TRUE(FileUtilForTest::checkPathExist(stopFile));

    // import on importedZkRoot for the second time
    auto keeper2 = PrepareGenerationKeeper(importedZkRoot, buildId, _configPath, _indexRoot, "import",
                                           /*useFakeGenerationTask*/ true,
                                           "{\"importedVersions\":{\"cluster1\":40,\"cluster2\":41}}");
    ASSERT_TRUE(keeper2);
    CheckLatestPublishedVersion(keeper2, {{"cluster1", {/*nodeCount*/ 2, /*importedVersion*/ 40}}});
}
}} // namespace build_service::admin
