#ifndef __INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_H
#define __INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/adaptive_dictionary_config.h"

IE_NAMESPACE_BEGIN(config);
class AdaptiveDictionarySchemaImpl;
DEFINE_SHARED_PTR(AdaptiveDictionarySchemaImpl);

class AdaptiveDictionarySchema : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, size_t>::const_iterator Iterator;

public:
    AdaptiveDictionarySchema();
    ~AdaptiveDictionarySchema() {}

public:
    AdaptiveDictionaryConfigPtr GetAdaptiveDictionaryConfig(
        const std::string& ruleName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const AdaptiveDictionarySchema& other) const;
    void AssertCompatible(const AdaptiveDictionarySchema& other) const;
    // for test
    void AddAdaptiveDictionaryConfig(const AdaptiveDictionaryConfigPtr& config);

private:
    AdaptiveDictionarySchemaImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveDictionarySchema);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_H
