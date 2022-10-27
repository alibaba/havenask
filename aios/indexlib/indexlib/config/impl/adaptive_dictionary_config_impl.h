#ifndef __INDEXLIB_ADAPTIVE_DICTIONARY_CONFIG_IMPL_H
#define __INDEXLIB_ADAPTIVE_DICTIONARY_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/adaptive_dictionary_config.h"

IE_NAMESPACE_BEGIN(config);

class AdaptiveDictionaryConfigImpl : public autil::legacy::Jsonizable
{
public:
    AdaptiveDictionaryConfigImpl();
    AdaptiveDictionaryConfigImpl(const std::string& ruleName, 
                             const std::string& dictType,
                             int32_t threshold);
    ~AdaptiveDictionaryConfigImpl();

public:
    const std::string& GetRuleName() const
    { return mRuleName; }

    const AdaptiveDictionaryConfig::DictType& GetDictType() const
    { return mDictType; }

    int32_t GetThreshold() const
    { return mThreshold; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const AdaptiveDictionaryConfigImpl& other) const;

private:
    AdaptiveDictionaryConfig::DictType FromTypeString(const std::string& typeStr);
    std::string ToTypeString(AdaptiveDictionaryConfig::DictType type);
    void CheckThreshold() const;

private:
    std::string mRuleName;
    int32_t mThreshold;
    AdaptiveDictionaryConfig::DictType mDictType;
    
private:
    friend class AdaptiveDictionaryConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveDictionaryConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ADAPTIVE_DICTIONARY_CONFIG_IMPL_H
