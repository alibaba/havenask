#ifndef ISEARCH_BS_TASKINPUTCONFIG_H
#define ISEARCH_BS_TASKINPUTCONFIG_H

#include <autil/legacy/jsonizable.h>
#include "build_service/common_define.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace config {

class TaskInputConfig : public autil::legacy::Jsonizable
{
public:
    TaskInputConfig();
    ~TaskInputConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void setType(const std::string& type) { _type = type; }
    const std::string& getType() const { return _type; }
    const KeyValueMap& getParameters() const { return _parameters; }
    void addParameters(const std::string& key, const std::string& value) {
        _parameters[key] = value;
    }
    const std::string& getModuleName() const { return _moduleName; }

    // private:
    //     std::string typeToStr();
    //     InputType strToType(const std::string& str);
private:
    std::string _type;
    std::string _moduleName;
    KeyValueMap _parameters;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskInputConfig);

}
}

#endif //ISEARCH_BS_TASKINPUTCONFIG_H
