#ifndef ISEARCH_BS_RESOURCEREADERMANAGER_H
#define ISEARCH_BS_RESOURCEREADERMANAGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include <autil/LoopThread.h>
#include <autil/Lock.h>
#include <unordered_map>
#include <indexlib/misc/singleton.h>

namespace build_service {
namespace config {

class ResourceReaderManager : public IE_NAMESPACE(misc)::Singleton<ResourceReaderManager>
{
public:
    ResourceReaderManager();
    ~ResourceReaderManager();

private:
    ResourceReaderManager(const ResourceReaderManager &);
    ResourceReaderManager& operator=(const ResourceReaderManager &);
public:
    static ResourceReaderPtr getResourceReader(const std::string& configPath);
private:
    void clearLoop();
private:
    std::unordered_map<std::string, std::pair<ResourceReaderPtr, int64_t>> _readerCache;
    mutable autil::ThreadMutex _cacheMapLock;
    bool _enableCache;
    int64_t _clearLoopInterval;
    int64_t _cacheExpireTime;
    autil::LoopThreadPtr _loopThread;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResourceReaderManager);

}
}

#endif //ISEARCH_BS_RESOURCEREADERMANAGER_H
