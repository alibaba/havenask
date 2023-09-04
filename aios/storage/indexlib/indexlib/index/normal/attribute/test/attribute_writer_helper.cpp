#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeWriterHelper);

AttributeWriterHelper::AttributeWriterHelper() {}

AttributeWriterHelper::~AttributeWriterHelper() {}

void AttributeWriterHelper::DumpWriter(AttributeWriter& writer, const string& dirPath)
{
    FileSystemOptions fileSystemOptions;
    fileSystemOptions.needFlush = true;
    util::MemoryQuotaControllerPtr mqc(new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    util::PartitionMemoryQuotaControllerPtr quotaController(new util::PartitionMemoryQuotaController(mqc));
    fileSystemOptions.memoryQuotaController = quotaController;
    IFileSystemPtr fileSystem =
        FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                  /*rootPath=*/dirPath, fileSystemOptions, std::shared_ptr<util::MetricProvider>(),
                                  /*isOverride=*/false)
            .GetOrThrow();
    DirectoryPtr dir = IDirectory::ToLegacyDirectory(
        make_shared<MemDirectory>("", fileSystem)); // TODO: @qingran test memOutput & diskOutput
    util::SimplePool dumpPool;
    writer.Dump(dir, &dumpPool);
    fileSystem->Sync(true).GetOrThrow();
}
}} // namespace indexlib::index
