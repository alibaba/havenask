#ifndef __INDEXLIB_DICTIONARY_CONFIG_H
#define __INDEXLIB_DICTIONARY_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, DictionaryConfigImpl);

IE_NAMESPACE_BEGIN(config);

class DictionaryConfig : public autil::legacy::Jsonizable
{
public:
    DictionaryConfig(const std::string& dictName, 
                     const std::string& content);
    DictionaryConfig(const DictionaryConfig& other);
    ~DictionaryConfig();

public:
    const std::string& GetDictName() const;

    const std::string& GetContent() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const DictionaryConfig& other) const;

private:
    DictionaryConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictionaryConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DICTIONARY_CONFIG_H
