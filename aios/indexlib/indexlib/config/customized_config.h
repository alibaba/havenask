#ifndef __INDEXLIB_CUSTOMIZED_CONFIG_H
#define __INDEXLIB_CUSTOMIZED_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, CustomizedConfigImpl);

IE_NAMESPACE_BEGIN(config);

class CustomizedConfig : public autil::legacy::Jsonizable
{
public:
    CustomizedConfig();
    CustomizedConfig(const CustomizedConfig& other);
    ~CustomizedConfig();
public:
    const std::string& GetPluginName() const;
    const bool GetParameters(const std::string& key,
                             std::string& value) const;
    const std::map<std::string, std::string>& GetParameters() const;
    const std::string& GetId() const;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const CustomizedConfig& other) const;

public:
    //for test
    void SetId(const std::string& id);
    void SetPluginName(const std::string& pluginName);
    
private:
    CustomizedConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedConfig);
typedef std::vector<CustomizedConfigPtr> CustomizedConfigVector;

class CustomizedConfigHelper
{
public:
    static CustomizedConfigPtr FindCustomizedConfig(
        const CustomizedConfigVector& configs, const std::string& id)
    {
        for (auto config : configs)
        {
            if (config->GetId() == id)
            {
                return config;
            }
        }
        return CustomizedConfigPtr();
    }
};

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CUSTOMIZED_CONFIG_H
