#ifndef __INDEXLIB_DICTIONARY_SCHEMA_IMPL_H
#define __INDEXLIB_DICTIONARY_SCHEMA_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/config/dictionary_config.h"
#include "indexlib/config/dictionary_schema.h"

IE_NAMESPACE_BEGIN(config);

class DictionarySchemaImpl : public autil::legacy::Jsonizable
{
public:
    DictionarySchemaImpl();
    ~DictionarySchemaImpl();

public:
    void AddDictionaryConfig(const DictionaryConfigPtr& dictConfig);
    DictionaryConfigPtr GetDictionaryConfig(const std::string& dictName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const DictionarySchemaImpl& other) const;
    void AssertCompatible(const DictionarySchemaImpl& other) const;

    inline DictionarySchema::Iterator Begin() const { return mDictConfigs.begin(); }
    inline DictionarySchema::Iterator End() const { return mDictConfigs.end(); } 

private:
    std::map<std::string, DictionaryConfigPtr> mDictConfigs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictionarySchemaImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DICTIONARY_SCHEMA_IMPL_H
