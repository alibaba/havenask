#ifndef __INDEXLIB_ADAPTIVE_DICTIONARY_CONFIG_H
#define __INDEXLIB_ADAPTIVE_DICTIONARY_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);
class AdaptiveDictionaryConfigImpl;
DEFINE_SHARED_PTR(AdaptiveDictionaryConfigImpl);

class AdaptiveDictionaryConfig : public autil::legacy::Jsonizable
{
public:
    enum DictType
    {
        DF_ADAPTIVE,
        PERCENT_ADAPTIVE,
        SIZE_ADAPTIVE,
        UNKOWN_TYPE
    };

public:
    AdaptiveDictionaryConfig();
    AdaptiveDictionaryConfig(const std::string& ruleName, 
                             const std::string& dictType,
                             int32_t threshold);
    ~AdaptiveDictionaryConfig();

public:
    const std::string& GetRuleName() const;

    const DictType& GetDictType() const;

    int32_t GetThreshold() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const AdaptiveDictionaryConfig& other) const;

private:
    AdaptiveDictionaryConfigImplPtr mImpl;
    
private:
    friend class AdaptiveDictionaryConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveDictionaryConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ADAPTIVE_DICTIONARY_CONFIG_H
