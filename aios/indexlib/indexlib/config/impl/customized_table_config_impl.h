#ifndef __INDEXLIB_CUSTOMIZED_TABLE_CONFIG_IMPL_H
#define __INDEXLIB_CUSTOMIZED_TABLE_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class CustomizedTableConfigImpl : public autil::legacy::Jsonizable
{
public:
    CustomizedTableConfigImpl();
    CustomizedTableConfigImpl(const CustomizedTableConfigImpl& other);
    ~CustomizedTableConfigImpl();
public:
    const std::string& GetPluginName() const { return mPluginName; }
    const std::string& GetIdentifier() const { return mId; }
    const bool GetParameters(const std::string& key,
                             std::string& value) const;
    const std::map<std::string, std::string>& GetParameters() const
    { return mParameters; }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const CustomizedTableConfigImpl& other) const;
    
private:
    std::string mId;
    std::string mPluginName;
    std::map<std::string, std::string> mParameters;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedTableConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CUSTOMIZED_TABLE_CONFIG_IMPL_H
