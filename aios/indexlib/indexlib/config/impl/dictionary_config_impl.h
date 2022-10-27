#ifndef __INDEXLIB_DICTIONARY_CONFIG_IMPL_H
#define __INDEXLIB_DICTIONARY_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class DictionaryConfigImpl : public autil::legacy::Jsonizable
{
public:
    DictionaryConfigImpl(const std::string& dictName, 
                         const std::string& content);
DictionaryConfigImpl(const DictionaryConfigImpl& other)
    : mDictName(other.mDictName)
        , mContent(other.mContent)
    {
    }

    ~DictionaryConfigImpl();

public:
    const std::string& GetDictName() const
    { return mDictName; }

    const std::string& GetContent() const
    { return mContent; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const DictionaryConfigImpl& other) const;

private:
    std::string mDictName;
    std::string mContent;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictionaryConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DICTIONARY_CONFIG_IMPL_H
