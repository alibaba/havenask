#ifndef __INDEXLIB_META_CACHE_PRELOADER_H
#define __INDEXLIB_META_CACHE_PRELOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index_base);

class MetaCachePreloader
{
public:
    MetaCachePreloader();
    ~MetaCachePreloader();
    
public:
    static void Load(const std::string& indexPath, versionid_t version);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MetaCachePreloader);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_META_CACHE_PRELOADER_H
