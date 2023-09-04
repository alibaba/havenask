#include "indexlib/index/normal/primarykey/test/primary_key_test_case_helper.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib { namespace index {

IFileSystemPtr PrimaryKeyTestCaseHelper::PrepareFileSystemForMMap(bool isLock, string outputRoot)
{
    FileSystemOptions fsOptions;
    LoadConfig loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(false, isLock);
    fsOptions.loadConfigList.PushBack(loadConfig);
    IFileSystemPtr fileSystem = FileSystemCreator::Create("pk_formatter_test", outputRoot, fsOptions).GetOrThrow();
    return fileSystem;
}

IFileSystemPtr PrimaryKeyTestCaseHelper::PrepareFileSystemForCache(const string& cacheType, size_t blockSize,
                                                                   string outputRoot)
{
    FileSystemOptions fsOptions;
    // dadi use at least 2GB
    size_t memorySize = cacheType == "dadi" ? 2UL * 1024 * 1024 * 1024 : 1024000;
    LoadConfig loadConfig = LoadConfigListCreator::MakeBlockLoadConfig(
        cacheType, memorySize, blockSize, CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE, false, false);
    fsOptions.loadConfigList.PushBack(loadConfig);
    IFileSystemPtr fileSystem = FileSystemCreator::Create("pk_formatter_test", outputRoot, fsOptions).GetOrThrow();
    return fileSystem;
}

PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode
PrimaryKeyTestCaseHelper::GetLoadModeByPkIndexType(PrimaryKeyIndexType pkIndexType)
{
    switch (pkIndexType) {
    case pk_hash_table:
        return PrimaryKeyLoadStrategyParam::HASH_TABLE;
    case pk_sort_array:
        return PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
    case pk_block_array:
        return PrimaryKeyLoadStrategyParam::BLOCK_VECTOR;
    default:
        assert(false);
        return PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
    }
}
}} // namespace indexlib::index
