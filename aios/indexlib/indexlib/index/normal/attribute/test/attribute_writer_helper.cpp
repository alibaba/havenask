#include "indexlib/util/simple_pool.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/in_mem_directory.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeWriterHelper);

AttributeWriterHelper::AttributeWriterHelper() 
{
}

AttributeWriterHelper::~AttributeWriterHelper() 
{
}

void AttributeWriterHelper::DumpWriter(
        AttributeWriter& writer, const string& dirPath)
{
    FileSystemOptions fileSystemOptions;
    fileSystemOptions.needFlush = true;
    util::MemoryQuotaControllerPtr mqc(
            new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    util::PartitionMemoryQuotaControllerPtr quotaController(
            new util::PartitionMemoryQuotaController(mqc));
    IndexlibFileSystemPtr fileSystem(
            new IndexlibFileSystemImpl(dirPath));
    fileSystemOptions.memoryQuotaController = quotaController;
    fileSystem->Init(fileSystemOptions);
    DirectoryPtr dir(new InMemDirectory(dirPath, fileSystem));
    util::SimplePool dumpPool;
    writer.Dump(dir, &dumpPool);
    fileSystem->Sync(true);
}

IE_NAMESPACE_END(index);

