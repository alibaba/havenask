#ifndef __INDEXLIB_CUSTOMIZED_CONFIG_IMPL_H
#define __INDEXLIB_CUSTOMIZED_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class CustomizedConfigImpl : public autil::legacy::Jsonizable
{
public:
    CustomizedConfigImpl();
    CustomizedConfigImpl(const CustomizedConfigImpl& other);
    ~CustomizedConfigImpl();
public:
    const std::string& GetPluginName() const { return mPluginName; }
    const std::map<std::string, std::string>& GetParameters() const
    { return mParameters; }
    const bool GetParameters(const std::string& key,
                             std::string& value) const;
    const std::string& GetId() const { return mId; }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const CustomizedConfigImpl& other) const;

public:
    //for test
    void SetId(const std::string& id);
    void SetPluginName(const std::string& pluginName);

private:
    std::string mId;
    std::string mPluginName;
    std::map<std::string, std::string> mParameters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedConfigImpl);
typedef std::vector<CustomizedConfigImplPtr> CustomizedConfigImplVector;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CUSTOMIZED_CONFIG_IMPL_H
