#ifndef __INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_IMPL_H
#define __INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/adaptive_dictionary_config.h"

IE_NAMESPACE_BEGIN(config);

class AdaptiveDictionarySchemaImpl : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, size_t>::const_iterator Iterator;

public:
    AdaptiveDictionarySchemaImpl();
    ~AdaptiveDictionarySchemaImpl();

public:
    AdaptiveDictionaryConfigPtr GetAdaptiveDictionaryConfig(
            const std::string& ruleName) const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const AdaptiveDictionarySchemaImpl& other) const;
    void AssertCompatible(const AdaptiveDictionarySchemaImpl& other) const;

    // for test
    void AddAdaptiveDictionaryConfig(const AdaptiveDictionaryConfigPtr& config);

private:
    std::map<std::string, size_t> mDictRules;
    std::vector<AdaptiveDictionaryConfigPtr> mAdaptiveDictConfigs;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveDictionarySchemaImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_IMPL_H
