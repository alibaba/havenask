#ifndef INDEXLIB_UTIL_FINDDL_H
#define INDEXLIB_UTIL_FINDDL_H

#include <link.h>
#include <dlfcn.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/path_util.h"
#include <autil/StringUtil.h>
#include <iostream>

IE_NAMESPACE_BEGIN(util);

namespace dlutil
{
struct DlInfo
{
    std::string name;
    std::string path;
};

inline int getDlHandle(struct dl_phdr_info* info, size_t size, void* data)
{
    auto targetDlInfo = reinterpret_cast<DlInfo*>(data);

    auto fileName = IE_NAMESPACE(util)::PathUtil::GetFileName(info->dlpi_name);
    std::vector<std::string> nameInfo;
    autil::StringUtil::split(nameInfo, fileName, '.');
    if (nameInfo.size() >= 2)
    {
        const auto& libName = nameInfo[0];
        if (libName == targetDlInfo->name)
        {
            targetDlInfo->path = info->dlpi_name;
            std::cout << info->dlpi_addr<<std::endl;
            return 1;
        }
    }

    return 0;
}

}

// eg: name: "libtcmalloc"
inline void* FindDl(const std::string& name)
{
    IE_DECLARE_AND_SETUP_LOGGER(util.FindDl);

    dlutil::DlInfo info;
    info.name = name;
    ::dl_iterate_phdr(&dlutil::getDlHandle, &info);
    if (!info.path.empty())
    {
        IE_LOG(INFO, "found [%s] loaded as [%s]", name.c_str(), info.path.c_str());
        return ::dlopen(info.path.c_str(), RTLD_NOLOAD | RTLD_NOW);
    }
    IE_LOG(INFO, "shared library [%s] not loaded", name.c_str());
    return nullptr;
}

IE_NAMESPACE_END(util);

#endif
