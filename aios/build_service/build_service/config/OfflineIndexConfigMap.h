#ifndef ISEARCH_BS_OFFLINEINDEXCONFIGMAP_H
#define ISEARCH_BS_OFFLINEINDEXCONFIGMAP_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/OfflineMergeConfig.h"
#include <autil/legacy/jsonizable.h>
#include <indexlib/config/build_config.h>

namespace build_service {
namespace config {

struct OfflineIndexConfig 
{
    IE_NAMESPACE(config)::BuildConfig buildConfig;
    OfflineMergeConfig offlineMergeConfig;
};
    
class OfflineIndexConfigMap : public autil::legacy::Jsonizable
{
public:
    OfflineIndexConfigMap();
    ~OfflineIndexConfigMap();

public:
    typedef std::map<std::string, OfflineIndexConfig> InnerMap;
    typedef InnerMap::iterator Iterator;
    typedef InnerMap::const_iterator ConstIterator;

public:
    Iterator begin() { return _offlineIndexConfigMap.begin(); }
    
    Iterator end() { return _offlineIndexConfigMap.end(); }
    
    Iterator find(const std::string& configName) {
        return _offlineIndexConfigMap.find(configName);
    }

    ConstIterator begin() const { return _offlineIndexConfigMap.begin(); }
    
    ConstIterator end() const { return _offlineIndexConfigMap.end(); }
    
    ConstIterator find(const std::string& configName) const {
        return _offlineIndexConfigMap.find(configName);
    }

    OfflineIndexConfig& operator [] (const std::string& configName) {
        return _offlineIndexConfigMap[configName];
    }

    size_t size() const { return _offlineIndexConfigMap.size(); }

    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

private:
    InnerMap _offlineIndexConfigMap;
        
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OfflineIndexConfigMap);

}
}

#endif //ISEARCH_BS_OFFLINEINDEXCONFIGMAP_H
