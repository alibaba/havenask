#include "build_service/config/TaskConfig.h"
#include "build_service/util/FileUtil.h"
#include "build_service/config/ResourceReader.h"
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace build_service::util;
using namespace build_service::plugin;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, TaskConfig);

TaskConfig::TaskConfig()
    : _partitionCount(1)
    , _parallelNum(1)
{
}

TaskConfig::~TaskConfig() {
}

bool TaskConfig::isBuildInTask(const std::string& taskName)
{
    if (taskName == BS_REPARTITION) {
        return true;
    }

    if (taskName == BS_EXTRACT_DOC) {
        return true;
    }

    if (taskName == BS_PREPARE_DATA_SOURCE) {
        return true;
    }

    if (taskName == BS_ROLLBACK_INDEX) {
        return true;
    }
    
    if (taskName == BS_DROP_BUILDING_INDEX) {
        return true;
    }
    
    if (taskName == BS_TASK_DOC_RECLAIM) {
        return true;
    }

    if (taskName == BS_TASK_RUN_SCRIPT) {
        return true;
    }

    if (taskName == BS_TASK_END_BUILD) {
        return true;
    }

    if (taskName == BS_TASK_CLONE_INDEX) {
        return true;
    }    
    
    return false;
}

void TaskConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("module_infos", _moduleInfos, _moduleInfos);
    json.Jsonize("task_module_name", _taskModuleName, _taskModuleName);    
    json.Jsonize("task_params", _parameters, _parameters);
    json.Jsonize("inputs", _inputConfigs, _inputConfigs);
    json.Jsonize("outputs", _outputConfigs, _outputConfigs);
    json.Jsonize("task_controller_config", _taskControllerConfig, _taskControllerConfig);
    json.Jsonize("partition_count", _partitionCount, _partitionCount);
    json.Jsonize("parallel_num", _parallelNum, _parallelNum);
}

bool TaskConfig::prepareBuildServiceTaskModuleInfo(
    const ResourceReaderPtr& resourceReader, std::string& pluginPath)
{
    char* env = getenv("LD_LIBRARY_PATH");
    string paths;
    if (NULL == env) {
        paths = resourceReader->getPluginPath() + ":./";
    } else {
        paths = resourceReader->getPluginPath() + ":./:" + string(env);
    }

    AUTIL_LOG(INFO, "scan path: [%s]", paths.c_str());
    StringUtil::trim(paths);
    StringTokenizer tokens(paths, ":", StringTokenizer::TOKEN_TRIM);
    StringTokenizer::Iterator it = tokens.begin();
    while (it != tokens.end()) {
        if (parseSoUnderOnePath((*it), pluginPath)) {
            AUTIL_LOG(INFO, "find libbuild_service_task.so under [%s]", pluginPath.c_str());
            return true;
        }
        it++;
    }
    return false;

}

bool TaskConfig::parseSoUnderOnePath(
    const std::string& path, std::string& pluginPath)
{
    vector<string> fileList;
    if (!FileUtil::listDir(path, fileList)) {
        return false;
    }
    for (auto& filePath : fileList) {
        if (filePath == "libbuild_service_tasks.so") {
            pluginPath = path;
            ModuleInfo moduleInfo;
            moduleInfo.moduleName = "build_service_task";
            moduleInfo.modulePath = "libbuild_service_tasks.so";
            _moduleInfos.push_back(moduleInfo);
            _taskModuleName = "build_service_task";
            return true;
        }
    }
    return false;
}

bool TaskConfig::getValidTaskConfigPath(
    const std::string& configPath,
    const std::string& taskConfigFilePath,
    const std::string& taskName,
    std::string& validTaskConfigPath) {
    if (!taskConfigFilePath.empty())
    {
        validTaskConfigPath = taskConfigFilePath;
        return true;
    }
    string taskFilePath = FileUtil::joinFilePath(
        configPath, ResourceReader::getTaskConfRelativePath(taskName));
    bool exist;
    if (!FileUtil::isFile(taskFilePath,exist))
    {
        BS_LOG(ERROR, "fail to judge task config file exist");
        return false;
    }
    if (exist)
    {
        validTaskConfigPath = taskFilePath;
        return true;
    }
    validTaskConfigPath.clear();
    return true;
}
    
bool TaskConfig::loadFromFile(const std::string& filePath,
                              TaskConfig& taskConfig) {
    string content;
    if (!FileUtil::readFile(filePath, content)) {
        BS_LOG(ERROR, "read task config file[%s] failed", filePath.c_str());
        return false;
    }
    try {
         FromJsonString(taskConfig, content);
    } catch (const ExceptionBase &e) {
        string errorMsg = "jsonize " + content 
            + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool TaskConfig::getInputConfig(const string& name, TaskInputConfig& input) const {
    auto iter = _inputConfigs.find(name);
    if (iter != _inputConfigs.end()) {
        input = iter->second;
        return true;
    }
    return false;
}
bool TaskConfig::getOutputConfig(const string& name, TaskOutputConfig& output) const {
    auto iter = _outputConfigs.find(name);
    if (iter != _outputConfigs.end()) {
        output = iter->second;
        return true;
    }
    return false;    
}
}
}
