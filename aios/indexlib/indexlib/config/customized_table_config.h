#ifndef __INDEXLIB_CUSTOMIZED_TABLE_CONFIG_H
#define __INDEXLIB_CUSTOMIZED_TABLE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, CustomizedTableConfigImpl);

IE_NAMESPACE_BEGIN(config);

class CustomizedTableConfig : public autil::legacy::Jsonizable
{
public:
    CustomizedTableConfig();
    CustomizedTableConfig(const CustomizedTableConfig& other);
    ~CustomizedTableConfig();
public:
    const std::string& GetPluginName() const;
    const std::string& GetIdentifier() const;
    const bool GetParameters(const std::string& key,
                             std::string& value) const;
    const std::map<std::string, std::string>& GetParameters() const;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const CustomizedTableConfig& other) const;
    
private:
    CustomizedTableConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedTableConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CUSTOMIZED_TABLE_CONFIG_H
