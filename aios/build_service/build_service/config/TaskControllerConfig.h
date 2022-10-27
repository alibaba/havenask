#ifndef ISEARCH_BS_TASKCONTROLLERCONFIG_H
#define ISEARCH_BS_TASKCONTROLLERCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/TaskTarget.h"

namespace build_service {
namespace config {

class TaskControllerConfig : public autil::legacy::Jsonizable
{
public:
    TaskControllerConfig();
    ~TaskControllerConfig();
public:
    enum Type {
        CT_BUILDIN,
        CT_CUSTOM,
        CT_UNKOWN
    };

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    Type getControllerType() const { return _type; }
    const std::vector<TaskTarget>& getTargetConfigs() const { return _targets; }
private:
    std::string typeToStr();
    Type strToType(const std::string& str);
    
private:
    Type _type;
    std::vector<TaskTarget> _targets;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskControllerConfig);

}
}

#endif //ISEARCH_BS_TASKCONTROLLERCONFIG_H
