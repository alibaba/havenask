#include "indexlib/file_system/lifecycle_table.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LifecycleTable);

LifecycleTable::LifecycleTable() 
{
}

string LifecycleTable::GetLifecycle(const std::string& path) const
{
    static const string EMPTY_STRING;
    if (mLifecycleMap.empty())
    {
        return EMPTY_STRING;
    }
    auto iter = mLifecycleMap.upper_bound(path);
    if (iter == mLifecycleMap.begin())
    {
        return EMPTY_STRING;
    }
    --iter;
    if (StringUtil::startsWith(path, iter->first))
    {
        return iter->second;
    }
    return EMPTY_STRING;
}

bool LifecycleTable::InnerAdd(const std::string& path, const std::string& lifecycle)
{
    if (mLifecycleMap.empty()) {
        mLifecycleMap[path] = lifecycle;
        return true;
    }
    auto iter = mLifecycleMap.upper_bound(path);
    if (iter != mLifecycleMap.end())
    {
        if (StringUtil::startsWith(iter->first, path))
        {
            IE_LOG(ERROR, "add [%s] failed, already have path [%s] inside", path.c_str(),
                   iter->first.c_str());
            return false;
        }
    }
    if (iter != mLifecycleMap.begin())
    {
        --iter;
        if (path == iter->first && lifecycle == iter->second)
        {
            return true;
        }
        if (StringUtil::startsWith(path, iter->first))
        {
            IE_LOG(ERROR, "add [%s] failed, already have path [%s] inside", path.c_str(),
                   iter->first.c_str());
            return false;            
        }
    }

    mLifecycleMap[path] = lifecycle;
    return true;
}

IE_NAMESPACE_END(file_system);

