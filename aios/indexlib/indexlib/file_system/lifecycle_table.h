#ifndef __INDEXLIB_LIFECYCLE_TABLE_H
#define __INDEXLIB_LIFECYCLE_TABLE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/path_util.h"

IE_NAMESPACE_BEGIN(file_system);

class LifecycleTable
{
public:
    LifecycleTable();

public:
    bool AddDirectory(const std::string& path, const std::string& lifecycle)
    {
        return InnerAdd(Normalize(path, true), lifecycle);
    }
    bool AddFile(const std::string& path, const std::string& lifecycle)
    {
        return InnerAdd(Normalize(path, false), lifecycle);
    }
    void RemoveDirectory(const std::string& path)
    {
        auto normPath = Normalize(path, true);
        mLifecycleMap.erase(normPath);
    }
    void RemoveFile(const std::string& path)
    {
        auto normPath = Normalize(path, false);
        mLifecycleMap.erase(normPath);
    }
    std::string GetLifecycle(const std::string& path) const;

private:
    bool InnerAdd(const std::string& path, const std::string& lifecycle);
    std::string Normalize(const std::string& path, bool isDirectory) const
    {
        auto normPath = util::PathUtil::NormalizePath(path);
        if (isDirectory)
        {
            normPath += "/";
        }
        return normPath;
    }

private:
    std::map<std::string, std::string> mLifecycleMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LifecycleTable);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LIFECYCLE_TABLE_H
