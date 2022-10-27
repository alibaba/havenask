#include <autil/StringUtil.h>
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/file_system_options.h"
#include "indexlib/file_system/test/mount_table_unittest.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MountTableTest);

#define CHECK_MOUNT_TABLE(mountTable, paths)            \
    CheckMountTable((mountTable), (paths), __LINE__)

#define CHECK_UNMOUNT(mountTableStr, unMountPath, expectMountTableStr) \
    CheckUnMount((mountTableStr), (unMountPath), (expectMountTableStr), __LINE__)

#define CHECK_MATCHPATH(                                                \
        mountTable, path, expectMatch, expectMatchPath, expectType)     \
    CheckMatchPath((mountTable), (path), (expectMatch),                 \
                   (expectMatchPath), (expectType), __LINE__)


MountTableTest::MountTableTest()
{
}

MountTableTest::~MountTableTest()
{
}

void MountTableTest::CaseSetUp()
{
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MountTableTest::CaseTearDown()
{
}

void MountTableTest::TestSimpleProcess()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    fileSystem->MakeDirectory(GET_ABS_PATH("a"));
    fileSystem->MountInMemStorage(GET_ABS_PATH("/a/b"));
    ASSERT_TRUE(fileSystem->IsExist(GET_ABS_PATH("/a")));
    ASSERT_FALSE(fileSystem->IsExist(GET_ABS_PATH("/b")));
    ASSERT_TRUE(fileSystem->IsExist(GET_ABS_PATH("/a/b")));
    ASSERT_FALSE(fileSystem->IsExist(GET_ABS_PATH("/a/c")));
    ASSERT_FALSE(fileSystem->IsExist(GET_ABS_PATH("/a/b/c")));
    ASSERT_FALSE(fileSystem->IsExist(GET_ABS_PATH("/a/c/d")));
}

void MountTableTest::TestMount()
{
    IndexlibFileSystemPtr ifs = GET_FILE_SYSTEM();
    MountTablePtr mountTable = CreateMountTable(GET_ABS_PATH(""));
    mountTable->Mount(GET_ABS_PATH("inmem"), FSST_IN_MEM);
    CHECK_MOUNT_TABLE(mountTable, "inmem");
    ifs->MakeDirectory(GET_ABS_PATH("local"));
    mountTable->Mount(GET_ABS_PATH("local/inmem"), FSST_IN_MEM);
    CHECK_MOUNT_TABLE(mountTable, "inmem;local/inmem");
    mountTable->Mount(GET_ABS_PATH("local/inmem/subInmem"), FSST_IN_MEM);
    CHECK_MOUNT_TABLE(mountTable, "inmem;local/inmem");
    mountTable->Mount(GET_ABS_PATH("local/inmem1"), FSST_IN_MEM);
    CHECK_MOUNT_TABLE(mountTable, "inmem;local/inmem;local/inmem1");
}

void MountTableTest::TestUnMount()
{
    CHECK_UNMOUNT("inmem;local/inmem", "local", "inmem");
    CHECK_UNMOUNT("inmem;local/inmem", "inmem", "local/inmem");
    CHECK_UNMOUNT("inmem;local/inmem1;local/inmem2", "local", "inmem");
    CHECK_UNMOUNT("inmem;local/inmem", "local/inmem", "inmem");
    CHECK_UNMOUNT("inmem;local/local/inmem1;local/inmem2", "local", "inmem");
}

void MountTableTest::TestUnMountFail()
{
    ASSERT_THROW(CHECK_UNMOUNT("inmem;local/inmem", "local/inmem/abc", ""),
                 NonExistException);
    ASSERT_THROW(CHECK_UNMOUNT("inmem;local/inmem", "notexist", ""),
                 NonExistException);
}

void MountTableTest::TestGetStorage()
{
    string rootPath = GET_ABS_PATH("root");
    MountTablePtr mountTable = CreateMountTable(rootPath);
    mountTable->Mount(rootPath + "/inmem", FSST_IN_MEM);
    StoragePtr diskStorage = mountTable->GetDiskStorage();
    StoragePtr inMemStorage = mountTable->GetInMemStorage();
    StoragePtr multiStorage = mountTable->GetMultiStorage();

    ASSERT_EQ(multiStorage.get(), mountTable->GetStorage(rootPath));
    ASSERT_EQ(diskStorage.get(), mountTable->GetStorage(rootPath + "/local"));
    ASSERT_EQ(inMemStorage.get(), mountTable->GetStorage(rootPath + "/inmem"));
    ASSERT_EQ(inMemStorage.get(), mountTable->GetStorage(rootPath + "/inmem/abc"));
}

void MountTableTest::TestGetStorageWithType()
{
    string rootPath = GET_ABS_PATH("root");
    MountTablePtr mountTable = CreateMountTable(rootPath);
    mountTable->Mount(rootPath + "/inmem", FSST_IN_MEM);
    StoragePtr diskStorage = mountTable->GetDiskStorage();
    StoragePtr inMemStorage = mountTable->GetInMemStorage();

    ASSERT_EQ(diskStorage.get(), mountTable->GetStorage(FSST_LOCAL));
    ASSERT_EQ(inMemStorage.get(), mountTable->GetStorage(FSST_IN_MEM));
    ASSERT_THROW(mountTable->GetStorage(FSST_UNKNOWN), BadParameterException);
}

void MountTableTest::TestGetStorageType()
{
    string rootPath = GET_ABS_PATH("root");
    MountTablePtr mountTable = CreateMountTable(rootPath);
    mountTable->Mount(rootPath + "/inmem", FSST_IN_MEM);

    ASSERT_EQ(FSST_LOCAL, mountTable->GetStorageType(rootPath)); 
    ASSERT_EQ(FSST_LOCAL, mountTable->GetStorageType(rootPath + "/local"));
    ASSERT_EQ(FSST_IN_MEM, mountTable->GetStorageType(rootPath + "/inmem"));
    ASSERT_EQ(FSST_IN_MEM, mountTable->GetStorageType(rootPath + "/inmem/abc"));
}

void MountTableTest::TestGetFirstMatchPath()
{
    MountTable mountTable;
    string rootPath = GET_TEST_DATA_PATH();
    mountTable.Init(rootPath, "", FileSystemOptions(), mMemoryController);
    storage::FileSystemWrapper::MkDir(rootPath + "R/a/b", true);
    mountTable.Mount(rootPath + "R/a/b/c", FSST_IN_MEM);
    mountTable.Mount(rootPath + "R/d", FSST_IN_MEM);

    CHECK_MATCHPATH(mountTable, rootPath + "R", true, rootPath + "R/a/b/c", FSST_LOCAL);
    CHECK_MATCHPATH(mountTable, rootPath + "O", false, rootPath, FSST_LOCAL);
    CHECK_MATCHPATH(mountTable, rootPath + "R/h", false, rootPath, FSST_LOCAL);
    CHECK_MATCHPATH(mountTable, rootPath + "R/d", true, rootPath + "R/d", FSST_IN_MEM);
    CHECK_MATCHPATH(mountTable, rootPath + "R/d/f", true, rootPath + "R/d", FSST_IN_MEM);
    CHECK_MATCHPATH(mountTable, rootPath + "R/a", true, rootPath + "R/a/b/c", FSST_LOCAL);
}


void MountTableTest::TestMountException()
{
    IndexlibFileSystemPtr ifs = GET_FILE_SYSTEM();
    ifs->MakeDirectory(GET_ABS_PATH("root"));
    MountTablePtr mountTable = CreateMountTable(GET_ABS_PATH("root"));

    // mount non-exist directory
    ASSERT_THROW(mountTable->Mount(GET_ABS_PATH("root"), FSST_IN_MEM),
                 UnSupportedException);
    ASSERT_THROW(mountTable->Mount(GET_ABS_PATH("root/notExistParent/mount"), FSST_IN_MEM),
                 NonExistException);

    // mount parent of in mem directory
    ifs->MakeDirectory(GET_ABS_PATH("root/a"));
    ASSERT_NO_THROW(mountTable->Mount(GET_ABS_PATH("root/a/b"), FSST_IN_MEM));
    ASSERT_THROW(mountTable->Mount(GET_ABS_PATH("root/a"), FSST_IN_MEM),
                 UnSupportedException);

    // mount directory exist in local
    ifs->MakeDirectory(GET_ABS_PATH("root/existInLocal"));
    ASSERT_THROW(mountTable->Mount(GET_ABS_PATH("root/existInLocal"), FSST_IN_MEM),
                 ExistException);

    // repeat mount
    ASSERT_NO_THROW(mountTable->Mount(GET_ABS_PATH("root/inmem1"), FSST_IN_MEM));
    // ExistException
    ASSERT_NO_THROW(mountTable->Mount(GET_ABS_PATH("root/inmem1"), FSST_IN_MEM));
}

MountTablePtr MountTableTest::CreateMountTable(const std::string& rootPath)
{
    IndexlibFileSystemPtr ifs = GET_FILE_SYSTEM();
    if (!ifs->IsExist(rootPath))
    {
        ifs->MakeDirectory(rootPath, true);
    }

    MountTablePtr mountTable(new MountTable());
    FileSystemOptions fileSystemOptions;
    mountTable->Init(PathUtil::NormalizePath(rootPath), "", fileSystemOptions, mMemoryController);

    return mountTable;
}

void MountTableTest::CheckMountTable(const MountTablePtr& mountTable, 
                                     const string& expectPaths,
                                     int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    vector<string> expectTables;
    StringUtil::fromString(expectPaths, expectTables, ";");
    const InMemStoragePtr& inMemStorage = mountTable->GetInMemStorage();
    ASSERT_EQ(inMemStorage->mRootPathTable->GetFileCount(), expectTables.size());
    for (size_t i = 0; i < expectTables.size(); ++i)
    {
        string mountPath = GET_ABS_PATH(expectTables[i]);
        ASSERT_TRUE(inMemStorage->mRootPathTable->IsExist(mountPath)) << mountPath;
        ASSERT_EQ(FSST_IN_MEM, mountTable->GetStorageType(mountPath)) << mountPath;
    }
}

void MountTableTest::CheckUnMount(const string& mountTableStr,
                                  const string& unMountPath,
                                  const string& expectMountTableStr,
                                  int lineNo)
{
    TearDown();
    SetUp();

    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    IndexlibFileSystemPtr ifs = GET_FILE_SYSTEM();
    string rootPath = GET_ABS_PATH("");

    vector<string> mountEntrys;
    StringUtil::fromString(mountTableStr, mountEntrys, ";");
    for (size_t i = 0; i < mountEntrys.size(); ++i)
    {
        string mountPath = PathUtil::JoinPath(rootPath, mountEntrys[i]);
        string parentPath = PathUtil::GetParentDirPath(mountPath);
        if (!ifs->IsExist(parentPath))
        {
            ifs->MakeDirectory(parentPath, true);
        }
        ifs->MountInMemStorage(mountPath);
    }

    string unMountAbsPath = PathUtil::JoinPath(rootPath, unMountPath);
    ifs->RemoveDirectory(unMountAbsPath);

    IndexlibFileSystemImplPtr implIfs = DYNAMIC_POINTER_CAST(
            IndexlibFileSystemImpl, ifs);
    const MountTablePtr mountTable = implIfs->GetMountTable();
    CheckMountTable(mountTable, expectMountTableStr, lineNo);
}

void MountTableTest::CheckMatchPath(const MountTable& mountTable,
        string path, bool expectMatch, string expectMatchPath,
        FSStorageType expectType, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    string matchPath;
    FSStorageType type;
    bool isMatch = mountTable.GetFirstMatchPath(path, matchPath, type);
    ASSERT_EQ(expectMatch, isMatch);
    if (isMatch)
    {
        ASSERT_EQ(expectMatchPath, matchPath);        
    }
    ASSERT_EQ(expectType, type);
}

IE_NAMESPACE_END(file_system);
