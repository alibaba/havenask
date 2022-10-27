#ifndef __INDEXLIB_DICTIONARY_SCHEMA_H
#define __INDEXLIB_DICTIONARY_SCHEMA_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/dictionary_config.h"

DECLARE_REFERENCE_CLASS(config, DictionarySchemaImpl);

IE_NAMESPACE_BEGIN(config);

class DictionarySchema : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, DictionaryConfigPtr>::const_iterator Iterator;

public:
    DictionarySchema();
    ~DictionarySchema();

public:
    void AddDictionaryConfig(const DictionaryConfigPtr& dictConfig);
    DictionaryConfigPtr GetDictionaryConfig(const std::string& dictName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const DictionarySchema& other) const;
    void AssertCompatible(const DictionarySchema& other) const;

    Iterator Begin() const;
    Iterator End() const;

private:
    DictionarySchemaImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictionarySchema);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DICTIONARY_SCHEMA_H
