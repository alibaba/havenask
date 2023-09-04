
#include "build_service_tasks/rollback/RollbackTask.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace testing;
using namespace build_service::io;
using namespace build_service::util;
using namespace build_service::config;
using namespace autil::legacy;
using namespace autil;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace build_service { namespace task_base {

class RollbackTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();
    void prepareIndexAndConfig(uint32_t partitionCount);
    void checkRollbackVersion(const std::string& indexRoot, uint32_t partitionCount, versionid_t sourceVersionId,
                              versionid_t targetVersionId);
    void checkTaskStatus(const std::string& paramStr, const std::string& key, const std::string& value)
    {
        KeyValueMap kvMap;
        FromJsonString(kvMap, paramStr);
        ASSERT_EQ(value, getValueFromKeyValueMap(kvMap, key, ""));
    }

protected:
    config::BuildServiceConfig _serviceConfig;
    config::BuildRuleConfig _buildRuleConfig;
    proto::PartitionId _pid;
};

class FakeRollbackTask : public RollbackTask
{
public:
    void setServiceConfig(const config::BuildServiceConfig& serviceConfig) { _serviceConfig = serviceConfig; }

private:
    bool getServiceConfig(config::ResourceReader& resourceReader, config::BuildServiceConfig& serviceConfig)
    {
        serviceConfig = _serviceConfig;
        return true;
    }
    config::BuildServiceConfig _serviceConfig;
};

void RollbackTaskTest::setUp()
{
    string dsStr = "{"
                   "\"src\" : \"swift\","
                   "\"type\" : \"swift\","
                   "\"swift_root\" : \"zfs://xxx-swift/xxx-swift-service\""
                   "}";
    KeyValueMap kvMap;
    kvMap[config::DATA_DESCRIPTION_KEY] = dsStr;
}

void RollbackTaskTest::tearDown() {}

void RollbackTaskTest::prepareIndexAndConfig(uint32_t partitionCount)
{
    vector<proto::Range> ranges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount);

    for (const auto& partRange : ranges) {
        proto::PartitionId pid;
        pid = _pid;
        *pid.mutable_range() = partRange;
        _pid = proto::PartitionId();
        _pid.set_role(proto::ROLE_TASK);
        _pid.set_step(proto::BUILD_STEP_INC);
        _pid.set_taskid("123_cluster1_rollback");
        *_pid.mutable_range() = partRange;
        *_pid.add_clusternames() = "cluster1";
        string indexRoot = GET_TEMP_DATA_PATH();
        string indexPath = util::IndexPathConstructor::constructIndexPath(indexRoot, _pid);
        string versionPre = indexPath + "/version.";
        _serviceConfig._indexRoot = indexRoot;
        Version version0 = VersionMaker::Make(0, "0,1", "", "");
        Version version1 = VersionMaker::Make(1, "2,3", "", "");
        Version version2 = VersionMaker::Make(2, "4,5", "", "");
        fslib::util::FileUtil::writeFile(versionPre + "0", version0.ToString());
        fslib::util::FileUtil::writeFile(versionPre + "1", version1.ToString());
        fslib::util::FileUtil::writeFile(versionPre + "2", version2.ToString());
        string segmentPre = indexPath + "/segment_";
        for (uint32_t i = 0; i < 6; ++i) {
            string segmentDir = segmentPre + StringUtil::toString(i) + "_level_0/";
            fslib::util::FileUtil::mkDir(segmentDir);
            indexlib::file_system::IndexFileList fileList;
            fslib::util::FileUtil::writeFile(segmentDir + "segment_file_list", fileList.ToString());
        }
    }
}

void RollbackTaskTest::checkRollbackVersion(const std::string& indexRoot, uint32_t partitionCount,
                                            versionid_t sourceVersionId, versionid_t targetVersionId)
{
    vector<proto::Range> ranges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount);

    for (const auto& partRange : ranges) {
        proto::PartitionId pid;
        pid = _pid;
        *pid.mutable_range() = partRange;

        string partIndexRoot = IndexPathConstructor::constructIndexPath(indexRoot, pid);
        Version sourceVersion, targetVersion;
        VersionLoader::GetVersionS(partIndexRoot, sourceVersion, sourceVersionId);
        VersionLoader::GetVersionS(partIndexRoot, targetVersion, targetVersionId);
        ASSERT_EQ(sourceVersion.GetFormatVersion(), targetVersion.GetFormatVersion());
        ASSERT_EQ(sourceVersion.GetLevelInfo(), targetVersion.GetLevelInfo());
        ASSERT_EQ(sourceVersion.GetLocator(), targetVersion.GetLocator());
        ASSERT_EQ(sourceVersion.GetSegmentVector(), targetVersion.GetSegmentVector());
        ASSERT_EQ(sourceVersion.GetTimestamp(), targetVersion.GetTimestamp());
        ASSERT_EQ(targetVersionId, targetVersion.GetVersionId());
        ASSERT_EQ(sourceVersionId, sourceVersion.GetVersionId());
    }
}

TEST_F(RollbackTaskTest, testSimple)
{
    prepareIndexAndConfig(2);
    FakeRollbackTask task;
    task.setServiceConfig(_serviceConfig);
    string jsonStr;
    // always write data to temp path
    fslib::util::FileUtil::atomicCopy(GET_TEST_DATA_PATH() + "/rollback_test/config",
                                      GET_TEMP_DATA_PATH() + "/rollback_test/config");
    fslib::util::FileUtil::readFile(GET_TEMP_DATA_PATH() + "/rollback_test/config/build_app.json.template", jsonStr);
    BuildServiceConfig BSConfig;
    FromJsonString(BSConfig, jsonStr);
    BSConfig._indexRoot = GET_TEMP_DATA_PATH();
    jsonStr = ToJsonString(BSConfig);
    fslib::util::FileUtil::writeFile(GET_TEMP_DATA_PATH() + "/rollback_test/config/build_app.json", jsonStr);
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/rollback_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";

    initParam.buildId.appName = _pid.buildid().appname();
    initParam.buildId.dataTable = _pid.buildid().datatable();
    initParam.buildId.generationId = _pid.buildid().generationid();
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 1;

    ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath(initParam.clusterName,
                                                             "cluster_config.builder_rule_config", _buildRuleConfig));

    config::TaskTarget target("do_task");
    target.addTargetDescription(BS_ROLLBACK_SOURCE_VERSION, "1");
    // target.addTargetDescription(BS_ROLLBACK_TARGET_VERSION, "6");
    ASSERT_TRUE(task.init(initParam));

    ASSERT_FALSE(task.isDone(target));
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));
    // Will not redo work
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));
    checkRollbackVersion(_serviceConfig._indexRoot, _buildRuleConfig.partitionCount, 1, 3);
    checkTaskStatus(task.getTaskStatus(), BS_ROLLBACK_TARGET_VERSION, "3");
}

}} // namespace build_service::task_base
