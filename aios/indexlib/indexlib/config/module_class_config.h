#ifndef __INDEXLIB_MODULE_CLASS_CONFIG_H
#define __INDEXLIB_MODULE_CLASS_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class ModuleClassConfig : public autil::legacy::Jsonizable
{
public:
    ModuleClassConfig();
    ~ModuleClassConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const ModuleClassConfig& other) const;
    ModuleClassConfig& operator=(const ModuleClassConfig& other);

public:
    std::string className;
    std::string moduleName;
    util::KeyValueMap  parameters;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ModuleClassConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MODULE_CLASS_CONFIG_H
