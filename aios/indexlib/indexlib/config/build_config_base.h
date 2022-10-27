#ifndef __INDEXLIB_BUILD_CONFIG_BASE_H
#define __INDEXLIB_BUILD_CONFIG_BASE_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

class BuildConfigBase : public autil::legacy::Jsonizable
{
public:
    enum BuildPhase
    {
        BP_FULL,
        BP_INC,
        BP_RT,
    };

public:
    BuildConfigBase();
    virtual ~BuildConfigBase();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator == (const BuildConfigBase& other) const;
    static bool GetEnvBuildTotalMemUse(int64_t& mem);
    
public:
    // attention: do not add new member to this class
    // U can add new member to BuildConfigImpl
    int64_t hashMapInitSize;
    int64_t buildTotalMemory;       // unit, MB
    int64_t buildResourceMemoryLimit; // unit, MB. for CustomOfflinePartition
    uint32_t maxDocCount;
    uint32_t keepVersionCount;
    uint32_t dumpThreadCount;
    uint32_t shardingColumnNum;
    LevelTopology levelTopology;
    uint32_t levelNum;
    int64_t ttl; // in second
    bool enablePackageFile;
    bool speedUpPrimaryKeyReader;
    bool useUserTimestamp;
    bool usePathMetaCache;
    BuildPhase buildPhase;
    std::string speedUpPrimaryKeyReaderParam;
    
public:
    static const uint32_t DEFAULT_HASHMAP_INIT_SIZE = 0;
    static const uint64_t DEFAULT_BUILD_TOTAL_MEMORY = (uint64_t)6 * 1024;//6 GB
    static const uint32_t DEFAULT_MAX_DOC_COUNT = (uint32_t)-1;
    static const uint32_t DEFAULT_KEEP_VERSION_COUNT = 2;
    static const uint32_t DEFAULT_DUMP_THREADCOUNT = 1;
    static const uint32_t MAX_DUMP_THREAD_COUNT = 20;
    static const uint32_t DEFAULT_SHARDING_COLUMN_NUM = 1;
    static const bool DEFAULT_USE_PACKAGE_FILE;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildConfigBase);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_BUILD_CONFIG_BASE_H
