#ifndef ISEARCH_BS_SCRIPTTASKCONFIG_H
#define ISEARCH_BS_SCRIPTTASKCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class ScriptTaskConfig : public autil::legacy::Jsonizable
{
public:
    struct Resource : public autil::legacy::Jsonizable
    {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override
        {
            json.Jsonize("type", type);
            json.Jsonize("path", path);
        }
        std::string type;
        std::string path;
    };
public:
    ScriptTaskConfig();
    ~ScriptTaskConfig();
    
private:
    ScriptTaskConfig(const ScriptTaskConfig &);
    ScriptTaskConfig& operator=(const ScriptTaskConfig &);
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    std::string scriptType;
    std::string scriptFile;
    std::vector<Resource> resources;
    std::string arguments;
    int32_t maxRetryTimes;
    int32_t minRetryInterval; //seconds
    bool forceRetry;

public:
    static std::string PYTHON_SCRIPT_TYPE;
    static std::string SHELL_SCRIPT_TYPE;
    
    static std::string BINARY_RESOURCE_TYPE;
    static std::string LIBRARY_RESOURCE_TYPE;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ScriptTaskConfig);

}
}

#endif //ISEARCH_BS_SCRIPTTASKCONFIG_H
