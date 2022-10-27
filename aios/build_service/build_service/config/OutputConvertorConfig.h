#ifndef ISEARCH_BS_OUTPUTCONVERTORCONFIG_H
#define ISEARCH_BS_OUTPUTCONVERTORCONFIG_H

#include <autil/legacy/jsonizable.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace config {

class OutputConvertorConfig : public autil::legacy::Jsonizable
{
public:
    OutputConvertorConfig();
    ~OutputConvertorConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    std::string moduleName;
    std::string name;
    KeyValueMap parameters;    

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OutputConvertorConfig);

}
}

#endif //ISEARCH_BS_OUTPUTCONVERTORCONFIG_H
