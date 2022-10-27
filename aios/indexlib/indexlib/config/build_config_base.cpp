#include <autil/StringUtil.h>
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/build_config_base.h"
#include "indexlib/config/primary_key_load_strategy_param.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, BuildConfigBase);

#ifdef DEFAULT_DISABLE_PACKAGE_FILE
const bool BuildConfigBase::DEFAULT_USE_PACKAGE_FILE = false;
#else
const bool BuildConfigBase::DEFAULT_USE_PACKAGE_FILE = true;
#endif

BuildConfigBase::BuildConfigBase()
    : hashMapInitSize(DEFAULT_HASHMAP_INIT_SIZE)
    , buildTotalMemory(DEFAULT_BUILD_TOTAL_MEMORY)
    , buildResourceMemoryLimit(-1)
    , maxDocCount(DEFAULT_MAX_DOC_COUNT)
    , keepVersionCount(DEFAULT_KEEP_VERSION_COUNT)
    , dumpThreadCount(DEFAULT_DUMP_THREADCOUNT)
    , shardingColumnNum(DEFAULT_SHARDING_COLUMN_NUM)
    , levelTopology(topo_default)
    , levelNum(1)
    , ttl(INVALID_TTL)
    , enablePackageFile(DEFAULT_USE_PACKAGE_FILE)
    , speedUpPrimaryKeyReader(false)
    , useUserTimestamp(false)
    , usePathMetaCache(true)
    , buildPhase(BP_FULL)
{
    int64_t envMem = 0;
    if (GetEnvBuildTotalMemUse(envMem))
    {
        buildTotalMemory = envMem;
    }
}

BuildConfigBase::~BuildConfigBase() 
{
}

bool BuildConfigBase::GetEnvBuildTotalMemUse(int64_t& mem)
{
    char * memStr = getenv("BUILD_TOTAL_MEM");
    if (!memStr)
    {
        return false;
    }
    if (!StringUtil::fromString(string(memStr), mem))
    {
        return false;
    }
    
    if (mem > 0)
    {
        return true;
    }

    return false;
}

void BuildConfigBase::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("hash_map_init_size", hashMapInitSize, hashMapInitSize);
    json.Jsonize("build_total_memory", buildTotalMemory, buildTotalMemory);
    json.Jsonize("build_resource_memory_limit", buildResourceMemoryLimit, buildResourceMemoryLimit);    
    json.Jsonize("max_doc_count", maxDocCount, maxDocCount);
    json.Jsonize("keep_version_count", keepVersionCount, keepVersionCount);
    json.Jsonize("dump_thread_count", dumpThreadCount, dumpThreadCount);
    json.Jsonize("level_num", levelNum, levelNum);
    json.Jsonize("time_to_live", ttl, ttl);
    json.Jsonize("enable_package_file", enablePackageFile, enablePackageFile);
    json.Jsonize("speedup_primary_key_reader", speedUpPrimaryKeyReader, 
                 speedUpPrimaryKeyReader);
    json.Jsonize("use_user_timestamp", useUserTimestamp, useUserTimestamp);
    json.Jsonize("use_path_meta_cache", usePathMetaCache, usePathMetaCache);
    json.Jsonize("speedup_primary_key_param", speedUpPrimaryKeyReaderParam, "");
    if (json.GetMode() == TO_JSON)
    {
        string topoStr = TopologyToStr(levelTopology);
        json.Jsonize("level_topology", topoStr);
        json.Jsonize("sharding_column_num", shardingColumnNum);

    }
    else
    {
        string topoStr;
        json.Jsonize("level_topology", topoStr, TOPOLOGY_DEFAULT_STR);
        levelTopology = StrToTopology(topoStr);

        uint32_t tempColumnNum = DEFAULT_SHARDING_COLUMN_NUM;
        json.Jsonize("sharding_column_num", tempColumnNum, tempColumnNum);
        shardingColumnNum = DEFAULT_SHARDING_COLUMN_NUM;
        // shardingColumnNum must be 2^n
        while (shardingColumnNum < tempColumnNum)
        {
            shardingColumnNum <<= 1;
        }
    }
}

void BuildConfigBase::Check() const
{
    if (buildTotalMemory < (int64_t)DEFAULT_CHUNK_SIZE*2)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter, 
                "build_total_memory at least twice as much as chunk_size(%luM), actual(%luM).", 
                DEFAULT_CHUNK_SIZE, buildTotalMemory);
    }
    if (maxDocCount == 0)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "max_doc_count should not be zero");
    }
    if (keepVersionCount == 0)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, 
                             "keep_version_count should be greater than 0.");
    }
    if (dumpThreadCount == 0 || dumpThreadCount > MAX_DUMP_THREAD_COUNT)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter, 
                "dump_thread_count should be greater than 0"
                " and no more than %d.", MAX_DUMP_THREAD_COUNT);
    }

    if (speedUpPrimaryKeyReader)
    {
        PrimaryKeyLoadStrategyParam::CheckParam(speedUpPrimaryKeyReaderParam);
    }
    if (levelTopology != topo_hash_mod && shardingColumnNum != 1)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter,
                "sharding_column_num should be 1 when level_topology is not hash_mod");
    }
}

bool BuildConfigBase::operator == (const BuildConfigBase& other) const
{
    return hashMapInitSize == other.hashMapInitSize
        && buildTotalMemory == other.buildTotalMemory
        && buildResourceMemoryLimit == other.buildResourceMemoryLimit
        && maxDocCount == other.maxDocCount
        && keepVersionCount == other.keepVersionCount
        && dumpThreadCount == other.dumpThreadCount
        && shardingColumnNum == other.shardingColumnNum
        && levelTopology == other.levelTopology
        && levelNum == other.levelNum
        && ttl == other.ttl
        && enablePackageFile == other.enablePackageFile
        && speedUpPrimaryKeyReader == other.speedUpPrimaryKeyReader
        && useUserTimestamp == other.useUserTimestamp
        && buildPhase == other.buildPhase
        && speedUpPrimaryKeyReaderParam == other.speedUpPrimaryKeyReaderParam
        && usePathMetaCache == other.usePathMetaCache;
}


IE_NAMESPACE_END(config);

