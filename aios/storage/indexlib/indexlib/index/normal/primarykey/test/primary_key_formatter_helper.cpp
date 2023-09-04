#include "indexlib/index/normal/primarykey/test/primary_key_formatter_helper.h"

#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/normal/primarykey/test/primary_key_test_case_helper.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PrimaryKeyFormatterHelper);

const string PrimaryKeyFormatterHelper::PK_FILE_NAME = "pk_test.data";
const string PrimaryKeyFormatterHelper::PK_DIRECTORY_PATH = "segment_0_level_0/index/pk_index";

DirectoryPtr PrimaryKeyFormatterHelper::PrepareMmapDirectory(bool isLock, string outputRoot)
{
    IFileSystemPtr fileSystem = PrimaryKeyTestCaseHelper::PrepareFileSystemForMMap(isLock, outputRoot);
    THROW_IF_FS_ERROR(fileSystem->RemoveDirectory(PK_DIRECTORY_PATH, RemoveOption::MayNonExist()), "remove [%s] failed",
                      PK_DIRECTORY_PATH.c_str());
    THROW_IF_FS_ERROR(fileSystem->MakeDirectory(PK_DIRECTORY_PATH, DirectoryOption()), "mkdir [%s] failed",
                      PK_DIRECTORY_PATH.c_str());
    return IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>(PK_DIRECTORY_PATH, fileSystem));
}

DirectoryPtr PrimaryKeyFormatterHelper::PrepareCacheDirectory(const string& cacheType, size_t blockSize,
                                                              string outputRoot)
{
    IFileSystemPtr fileSystem = PrimaryKeyTestCaseHelper::PrepareFileSystemForCache(cacheType, blockSize, outputRoot);
    THROW_IF_FS_ERROR(fileSystem->RemoveDirectory(PK_DIRECTORY_PATH, RemoveOption::MayNonExist()), "remove [%s] failed",
                      PK_DIRECTORY_PATH.c_str());
    THROW_IF_FS_ERROR(fileSystem->MakeDirectory(PK_DIRECTORY_PATH, DirectoryOption()), "mkdir [%s] failed",
                      PK_DIRECTORY_PATH.c_str());
    return IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>(PK_DIRECTORY_PATH, fileSystem));
}
}} // namespace indexlib::index
