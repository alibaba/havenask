#include "indexlib/document/document_init_param.h"

using namespace std;

IE_NAMESPACE_BEGIN(document);

void DocumentInitParam::AddValue(const string& key, const string& value)
{
    mKVMap[key] = value;
}

bool DocumentInitParam::GetValue(const string& key, string& value)
{
    KeyValueMap::const_iterator iter = mKVMap.find(key);
    if (iter == mKVMap.end())
    {
        return false;
    }
    value = iter->second;
    return true;
}

IE_NAMESPACE_END(document);

