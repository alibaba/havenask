#include "indexlib/index/primary_key/test/PrimaryKeyReaderTesthelper.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyReaderTesthelper);

PrimaryKeyReaderTesthelper::PrimaryKeyReaderTesthelper() {}

PrimaryKeyReaderTesthelper::~PrimaryKeyReaderTesthelper() {}

IFileSystemPtr PrimaryKeyReaderTesthelper::PrepareFileSystemForMMap(bool isLock, string outputRoot)
{
    FileSystemOptions fsOptions;
    LoadConfig loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(false, isLock);
    fsOptions.loadConfigList.PushBack(loadConfig);
    IFileSystemPtr fileSystem = FileSystemCreator::Create("pk_formatter_test", outputRoot, fsOptions).GetOrThrow();
    return fileSystem;
}

IFileSystemPtr PrimaryKeyReaderTesthelper::PrepareFileSystemForCache(const string& cacheType, size_t blockSize,
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

} // namespace indexlibv2::index
