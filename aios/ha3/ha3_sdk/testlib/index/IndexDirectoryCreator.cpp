#include "ha3_sdk/testlib/index/IndexDirectoryCreator.h"

#include <iosfwd>

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::test;

namespace indexlib {
namespace index {

IndexDirectoryCreator::IndexDirectoryCreator() {}

IndexDirectoryCreator::~IndexDirectoryCreator() {}

DirectoryPtr IndexDirectoryCreator::Create(const std::string &path) {
    FileSystemOptions options;
    LoadConfigList loadConfigList;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.needFlush = true;
    options.useCache = true;
    options.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fileSystem = FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                /*rootPath=*/path,
                                                options,
                                                util::MetricProviderPtr(),
                                                /*isOverride=*/false)
                          .GetOrThrow();

    return DirectoryCreator::Get(fileSystem, path, true);
}

} // namespace index
} // namespace indexlib
