#ifndef __INDEXLIB_DOCUMENT_INIT_PARAM_H
#define __INDEXLIB_DOCUMENT_INIT_PARAM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(document);

class DocumentInitParam
{
public:
    typedef std::map<std::string, std::string> KeyValueMap;
    DEFINE_SHARED_PTR(KeyValueMap);
    
public:
    DocumentInitParam() {}
    DocumentInitParam(const KeyValueMap& kvMap)
        : mKVMap(kvMap)
    {}
    virtual ~DocumentInitParam() {}

public:
    void AddValue(const std::string& key, const std::string& value);
    bool GetValue(const std::string& key, std::string& value);
    
protected:
    KeyValueMap mKVMap;
};
DEFINE_SHARED_PTR(DocumentInitParam);

/////////////////////////////////////////////////////////////////

template <typename UserDefineResource>
class DocumentInitParamTyped : public DocumentInitParam
{
public:
    DocumentInitParamTyped() {}
    DocumentInitParamTyped(const KeyValueMap& kvMap,
                           const UserDefineResource& res)
        : DocumentInitParam(kvMap)
        , mResource(res)
    {}
    
    ~DocumentInitParamTyped() {}

    void SetResource(const UserDefineResource& res) { mResource = res; }
    const UserDefineResource& GetResource() const { return mResource; }

    using DocumentInitParam::AddValue;
    using DocumentInitParam::GetValue;
private:
    UserDefineResource mResource;
};
    
IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_INIT_PARAM_H
