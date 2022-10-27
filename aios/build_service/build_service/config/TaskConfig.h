#ifndef ISEARCH_BS_TASKCONFIG_H
#define ISEARCH_BS_TASKCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/config/TaskControllerConfig.h"

namespace build_service {
namespace config {
class ResourceReader;
typedef std::tr1::shared_ptr<ResourceReader> ResourceReaderPtr;

class TaskConfig : public autil::legacy::Jsonizable
{
public:
    TaskConfig();
    ~TaskConfig();
private:
    TaskConfig(const TaskConfig &);
    TaskConfig& operator=(const TaskConfig &);
public:
    static bool isBuildInTask(const std::string& taskName);
    static bool getValidTaskConfigPath(
        const std::string& configPath,
        const std::string& taskConfigFilePath,
        const std::string& taskName,
        std::string& validTaskConfigPath);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const plugin::ModuleInfos& getModuleInfos() const { return _moduleInfos; }
    const KeyValueMap& getTaskParameters() const { return _parameters; }
    const auto& getInputConfigs() const { return _inputConfigs; }
    const auto& getOutputConfigs() const { return _outputConfigs; }

    bool prepareBuildServiceTaskModuleInfo(
        const ResourceReaderPtr& resourceReader, std::string& pluginPath);

    bool getInputConfig(const std::string& name, TaskInputConfig& input) const;
    bool getOutputConfig(const std::string& name, TaskOutputConfig& output) const;
    const TaskControllerConfig& getTaskControllerConfig() const {
        return _taskControllerConfig;
    }
    int32_t getPartitionCount() const { return _partitionCount; }
    int32_t getParallelNum() const { return _parallelNum; }
    static bool loadFromFile(const std::string& fileName, TaskConfig& taskConfig);
    template<typename T>
    bool getTaskParam(const std::string& key, T& value) const {
        std::string valueStr;
        auto iter = _parameters.find(key);
        if (iter == _parameters.end()) {
            return false;
        }
        valueStr = iter->second;
        if (!autil::StringUtil::fromString(valueStr, value)) {
            return false;
        }
        return true;
    }

    const std::string& getTaskModuleName() const { return _taskModuleName; }

private:
    bool parseSoUnderOnePath(
        const std::string& path, std::string& pluginPath);
private:
    plugin::ModuleInfos _moduleInfos;
    std::string _taskModuleName;
    KeyValueMap _parameters;
    std::map<std::string, TaskInputConfig> _inputConfigs;
    std::map<std::string, TaskOutputConfig> _outputConfigs;
    TaskControllerConfig _taskControllerConfig;
    uint32_t _partitionCount;
    uint32_t _parallelNum;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskConfig);

}
}

#endif //ISEARCH_BS_TASKCONFIG_H
