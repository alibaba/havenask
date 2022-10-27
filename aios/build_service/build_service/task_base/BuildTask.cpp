#include "build_service/task_base/BuildTask.h"
#include <autil/legacy/jsonizable.h>
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/builder/LineDataBuilder.h"
#include <indexlib/config/index_partition_options.h>
#include <indexlib/index_base/schema_adapter.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::proto;
using namespace build_service::workflow;
using namespace build_service::builder;
using namespace build_service::config;
using namespace build_service::util;
using namespace heavenask::indexlib;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, BuildTask);

BuildTask::BuildTask() {
}

BuildTask::~BuildTask() {
}

bool BuildTask::run(const string &jobParam,
                    BrokerFactory *brokerFactory,
                    int32_t instanceId, Mode mode)
{
    if (!TaskBase::init(jobParam)) {
        return false;
    }
    PartitionId partitionId = createPartitionId(instanceId, mode, "builder");
    if (!resetResourceReader(partitionId)) {
        return false;
    }
    if (!startMonitor(partitionId.range(), mode)) {
        string errorMsg = "start monitor failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string clusterName = getValueFromKeyValueMap(_kvMap, CLUSTER_NAMES);
    TableType tableType;
    if (!_resourceReader->getTableType(clusterName, tableType)) {
        return false;
    }
    if (tt_rawfile == tableType) {
        // READER_AND_PROCESSOR
        if (BUILD_MAP == mode) {
            return true;
        } else {
            return buildRawFileIndex(_resourceReader, _kvMap, partitionId);
        }
    } else {
        return startBuildFlow(partitionId, brokerFactory, mode);
    }
}

bool BuildTask::startBuildFlow(const PartitionId &partitionId,
                               BrokerFactory *brokerFactory,
                               Mode mode)
{
    unique_ptr<BuildFlow> buildFlow(createBuildFlow());
    BuildFlow::BuildFlowMode buildFlowMode = getBuildFlowMode(
            _jobConfig.needPartition(), mode);
    if (buildFlowMode == BuildFlow::NONE) {
        return true;
    }
    if (!buildFlow->startBuildFlow(_resourceReader, _kvMap, partitionId,
                                   brokerFactory, buildFlowMode,
                                   JOB, _metricProvider))
    {
        BS_LOG(ERROR, "start buildflow failed");
        return false;
    }

    // BuildFlow will exit itself when JOB mode
    if (!buildFlow->waitFinish()) {
        BS_LOG(ERROR, "fatal error happened!");
        return false;
    }
    BS_LOG(INFO, "build is finished");

    builder::Builder *builder = buildFlow->getBuilder();
    if (_jobConfig.getBuildMode() == BUILD_MODE_INCREMENTAL
        && builder && !builder->merge(_mergeIndexPartitionOptions))
    {
        string errorMsg = "merge failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

BuildFlow *BuildTask::createBuildFlow() const {
    return new BuildFlow();
}

BuildFlow::BuildFlowMode BuildTask::getBuildFlowMode(
        bool needPartition, Mode mode)
{
    if (!needPartition) {
        return mode == BUILD_MAP ? BuildFlow::ALL : BuildFlow::NONE;
    } else {
        return mode == BUILD_MAP ? BuildFlow::READER_AND_PROCESSOR : BuildFlow::BUILDER;
    }
}

bool BuildTask::resetResourceReader(const PartitionId &partitionId) {
    string indexRootPath = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    if (indexRootPath.empty()) {
        string errorMsg = "index root path can not be empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string indexPath = IndexPathConstructor::constructIndexPath(indexRootPath, partitionId);
    _resourceReader.reset(new ResourceReader(_resourceReader->getConfigPath()));
    _resourceReader->init();
    if (!_resourceReader->initGenerationMeta(indexPath)) {
        string errorMsg = "ResourceReader init failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

// TODO: ut
bool BuildTask::buildRawFileIndex(
        const ResourceReaderPtr &resourceReader,
        const KeyValueMap &kvMap,
        const PartitionId &partitionId)
{
    string src = getValueFromKeyValueMap(kvMap, READ_SRC);
    if (src != FILE_READ_SRC) {
        BS_LOG(ERROR, "expect src [file], actual [%s]", src.c_str());
        return false;
    }
    string file = getValueFromKeyValueMap(kvMap, DATA_PATH);
    string indexRoot = getValueFromKeyValueMap(kvMap, INDEX_ROOT_PATH);
    string indexPath = IndexPathConstructor::constructIndexPath(indexRoot, partitionId);
    string fileFolder, fileName;
    FileUtil::splitFileName(file, fileFolder, fileName); // TODO: FileSystem

    auto schema = resourceReader->getSchema(partitionId.clusternames(0));
    if (!schema) {
        return false;
    }
    auto &param = schema->GetUserDefinedParam();
    param[BS_FILE_NAME] = autil::legacy::Any(fileName);

    ErrorCode ec = FileSystem::mkDir(indexPath, true);
    if (ec != EC_OK && ec != EC_EXIST) {
        BS_LOG(ERROR, "Make directory : [%s] FAILED", indexPath.c_str());
        return false;
    }

    if (!LineDataBuilder::storeSchema(indexPath, schema)) {
        return false;
    }

    string targetFilePath = FileSystem::joinFilePath(indexPath, fileName);
    ec = FileSystem::isExist(targetFilePath);
    if (EC_TRUE == ec) {
        BS_LOG(INFO, "File[%s] exist.", targetFilePath.c_str());
        return true;
    } else if (EC_FALSE != ec) {
        return false;
    }

    if (!FileUtil::atomicCopy(file, targetFilePath)) {
        BS_LOG(ERROR, "Copy from path : [%s] to path : [%s] FAILED",
               file.c_str(), targetFilePath.c_str());
        return false;
    }
    return true;
}

}
}
