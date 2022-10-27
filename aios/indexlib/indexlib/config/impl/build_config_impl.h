#ifndef __INDEXLIB_BUILD_CONFIG_IMPL_H
#define __INDEXLIB_BUILD_CONFIG_IMPL_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/storage/raid_config.h"

IE_NAMESPACE_BEGIN(config);

class BuildConfigImpl : public autil::legacy::Jsonizable
{
public:
    BuildConfigImpl();
    BuildConfigImpl(const BuildConfigImpl& other);
    ~BuildConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator == (const BuildConfigImpl& other) const;
    void operator = (const BuildConfigImpl& other);
    bool SetCustomizedParams(const std::string& key, const std::string& value);
    const std::map<std::string, std::string>& GetCustomizedParams() const;
    
public:
    std::vector<ModuleClassConfig> segAttrUpdaterConfig;
    storage::RaidConfigPtr raidConfig;
    std::map<std::string, std::string> customizedParams;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_BUILD_CONFIG_IMPL_H
