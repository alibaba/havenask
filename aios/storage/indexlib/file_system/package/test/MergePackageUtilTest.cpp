#include "indexlib/file_system/package/MergePackageUtil.h"

#include "autil/LambdaWorkItem.h"
#include "autil/ThreadPool.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/MergePackageMeta.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/test/PackageTestUtil.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/ExceptionRunner.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::file_system {

class MergePackageUtilTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MergePackageUtilTest);

private:
    void CheckExistencePhysical(const std::string& path, bool expected);
    void CheckFileLength(const std::string& path, size_t expected);
    static std::string GenerateTestString(size_t length, char c);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MergePackageUtilTest);

void MergePackageUtilTest::CheckExistencePhysical(const std::string& path, bool expected)
{
    auto [status, isExist] = indexlib::file_system::FslibWrapper::IsExist(path).StatusWith();
    ASSERT_TRUE(status.IsOK());
    EXPECT_EQ(isExist, expected) << "path: " << path;
}

void MergePackageUtilTest::CheckFileLength(const std::string& path, size_t expected)
{
    auto [status, length] = indexlib::file_system::FslibWrapper::GetFileLength(path).StatusWith();
    ASSERT_TRUE(status.IsOK());
    EXPECT_EQ(length, expected) << "path: " << path;
}

std::string MergePackageUtilTest::GenerateTestString(size_t length, char c) { return std::string(length, c); }

TEST_F(MergePackageUtilTest, ConvertDirToPackage)
{
    size_t PAGE_SIZE = getpagesize();
    auto fileSystem = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto rootDir = Directory::Get(fileSystem);
    auto srcDirectory = rootDir->MakeDirectory("src");
    srcDirectory->MakeDirectory("inner_dir1");
    srcDirectory->MakeDirectory("inner_dir2");
    srcDirectory->MakeDirectory("inner_dir3");
    srcDirectory->Store("file1", GenerateTestString(5 * PAGE_SIZE, '0'));
    srcDirectory->Store("inner_dir1/file2", GenerateTestString(9 * PAGE_SIZE, '1'));
    srcDirectory->Store("inner_dir2/file3", GenerateTestString(7 * PAGE_SIZE + 1, '2'));
    srcDirectory->Store("file4", GenerateTestString(20 * PAGE_SIZE, '3'));
    srcDirectory->Store("inner_dir3/file5", GenerateTestString(2 * PAGE_SIZE, '4'));
    srcDirectory->Store("inner_dir3/file6", GenerateTestString(3 * PAGE_SIZE + 2, '5'));
    srcDirectory->Store("inner_dir3/file7", GenerateTestString(6 * PAGE_SIZE + 1, '6'));
    srcDirectory->Store("inner_dir3/file8", GenerateTestString(PAGE_SIZE + 5, '7'));
    auto outputDirectory = rootDir->MakeDirectory("output");
    auto workingDirectory = rootDir->MakeDirectory("working");

    MergePackageMeta mergePackageMeta;
    std::map<std::string, size_t>& file2SizeMap = mergePackageMeta.packageTag2File2SizeMap["unused-tag"];
    file2SizeMap[srcDirectory->GetPhysicalPath("file1")] = 5 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir1/file2")] = 9 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir2/file3")] = 7 * PAGE_SIZE + 1;
    file2SizeMap[srcDirectory->GetPhysicalPath("file4")] = 20 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file5")] = 2 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file6")] = 3 * PAGE_SIZE + 2;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file7")] = 6 * PAGE_SIZE + 1;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file8")] = PAGE_SIZE + 5;

    mergePackageMeta.dirSet = {"inner_dir1", "inner_dir2", "inner_dir3"};

    auto ec = MergePackageUtil::ConvertDirToPackage(workingDirectory->GetIDirectory(), outputDirectory->GetIDirectory(),
                                                    /*parentDirName=*/"src", mergePackageMeta,
                                                    /*packagingThresholdBytes=*/10 * PAGE_SIZE, /*threadCount=*/0);
    ASSERT_EQ(ec, FSEC_OK);
    PackageFileMeta packageFileMeta;
    ec = packageFileMeta.Load(util::PathUtil::JoinPath(outputDirectory->GetPhysicalPath(""), "package_file.__meta__"));
    ASSERT_EQ(ec, FSEC_OK);
    EXPECT_TRUE(PackageTestUtil::CheckInnerFileMeta(
        "inner_dir1:DIR:0:0:0#inner_dir2:DIR:0:0:0#inner_dir3:DIR:0:0:0#file4:FILE:81920:0:0#file1:FILE:20480:0:1#"
        "inner_dir1/file2:FILE:36864:0:2#inner_dir2/file3:FILE:28673:0:3#inner_dir3/file5:FILE:8192:32768:3#inner_dir3/"
        "file6:FILE:12290:0:4#inner_dir3/file7:FILE:24577:0:5#inner_dir3/file8:FILE:4101:28672:5#",
        packageFileMeta));

    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.0", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.1", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.2", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.3", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.4", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.5", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.6", false);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.0", 20 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.1", 5 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.2", 9 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.3", 10 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.4", 3 * PAGE_SIZE + 2);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.5", 8 * PAGE_SIZE + 5);
}

TEST_F(MergePackageUtilTest, CleanSrcIndexFiles)
{
    auto fileSystem = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto rootDir = Directory::Get(fileSystem);
    auto srcDirectory = rootDir->MakeDirectory("src");
    srcDirectory->MakeDirectory("inner_dir1");
    srcDirectory->MakeDirectory("inner_dir2");
    srcDirectory->MakeDirectory("inner_dir3");
    srcDirectory->MakeDirectory("inner_dir4");
    srcDirectory->Store("file1", "12345");
    srcDirectory->Store("file2", "123456789");
    srcDirectory->Store("file3", "1234567890123");
    srcDirectory->Store("inner_dir1/file", "1234567");
    srcDirectory->Store("inner_dir2/file", "12345678901234567890");
    srcDirectory->Store("inner_dir3/file", "123");

    auto directoryWithPackaingPlan = rootDir->MakeDirectory("directory_with_packaging_plan");
    PackagingPlan packagingPlan;
    FileListToPackage& fileListToPackage = packagingPlan.dstPath2SrcFilePathsMap["unused-dst-path"];
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("file1"));
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("file2"));
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("inner_dir1/file"));
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("inner_dir2/file"));
    packagingPlan.srcDirPaths = {"inner_dir1", "inner_dir2"};

    directoryWithPackaingPlan->Store("packaging_plan", autil::legacy::ToJsonString(packagingPlan));
    auto ec =
        MergePackageUtil::CleanSrcIndexFiles(srcDirectory->GetIDirectory(), directoryWithPackaingPlan->GetIDirectory());
    ASSERT_EQ(ec, FSEC_OK);

    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file1", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file2", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file3", true);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1/file", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2/file", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3", true);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3/file", true);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir4", true);
}

TEST_F(MergePackageUtilTest, CleanSrcIndexFilesNonExist)
{
    auto fileSystem = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto rootDir = Directory::Get(fileSystem);
    auto srcDirectory = rootDir->MakeDirectory("src");
    srcDirectory->MakeDirectory("inner_dir1");
    srcDirectory->MakeDirectory("inner_dir2");
    srcDirectory->Store("file1", "12345");
    srcDirectory->Store("file2", "123456789");
    srcDirectory->Store("inner_dir1/file", "1234567");
    srcDirectory->Store("inner_dir2/file", "12345678901234567890");

    auto directoryWithPackaingPlan = rootDir->MakeDirectory("directory_with_packaging_plan");
    PackagingPlan packagingPlan;
    FileListToPackage& fileListToPackage = packagingPlan.dstPath2SrcFilePathsMap["unused-dst-path"];
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("") + "/file1");
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("") + "/file2");
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("") + "/file3");
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("") + "/inner_dir1/file");
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("") + "/inner_dir2/file");
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("") + "/inner_dir3/file");
    packagingPlan.srcDirPaths = {"inner_dir1", "inner_dir2", "inner_dir3"};

    directoryWithPackaingPlan->Store("packaging_plan", autil::legacy::ToJsonString(packagingPlan));
    auto ec =
        MergePackageUtil::CleanSrcIndexFiles(srcDirectory->GetIDirectory(), directoryWithPackaingPlan->GetIDirectory());
    ASSERT_EQ(ec, FSEC_OK);

    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file1", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file2", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file3", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1/file", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2/file", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3/file", false);
}

TEST_F(MergePackageUtilTest, ConvertDirToPackageException)
{
    size_t PAGE_SIZE = getpagesize();
    indexlib::util::ExceptionRunner runner(/*depth=*/3);
    auto fileSystem = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto rootDir = Directory::Get(fileSystem);
    auto srcDirectory = rootDir->MakeDirectory("src");
    srcDirectory->MakeDirectory("inner_dir1");
    srcDirectory->MakeDirectory("inner_dir2");
    srcDirectory->MakeDirectory("inner_dir3");
    srcDirectory->Store("file1", GenerateTestString(5 * PAGE_SIZE, '0'));
    srcDirectory->Store("inner_dir1/file2", GenerateTestString(9 * PAGE_SIZE, '1'));
    srcDirectory->Store("inner_dir2/file3", GenerateTestString(7 * PAGE_SIZE + 1, '2'));
    srcDirectory->Store("file4", GenerateTestString(20 * PAGE_SIZE, '3'));
    srcDirectory->Store("inner_dir3/file5", GenerateTestString(2 * PAGE_SIZE, '3'));
    srcDirectory->Store("inner_dir3/file6", GenerateTestString(3 * PAGE_SIZE, '4'));
    auto outputDirectory = rootDir->MakeDirectory("output");
    auto workingDirectory = rootDir->MakeDirectory("working");

    MergePackageMeta mergePackageMeta;
    std::map<std::string, size_t>& file2SizeMap = mergePackageMeta.packageTag2File2SizeMap["unused-tag"];
    file2SizeMap[srcDirectory->GetPhysicalPath("file1")] = 5 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir1/file2")] = 9 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir2/file3")] = 7 * PAGE_SIZE + 1;
    file2SizeMap[srcDirectory->GetPhysicalPath("file4")] = 20 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file5")] = 2 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file6")] = 3 * PAGE_SIZE;
    mergePackageMeta.dirSet = {"inner_dir1", "inner_dir2", "inner_dir3"};
    runner.AddReset([&rootDir]() -> void {
        rootDir->RemoveDirectory("working");
        rootDir->MakeDirectory("working");
    });
    runner.AddAction(indexlib::util::Action(
        [&]() {
            autil::ThreadPool threadPool(/*threadNum=*/2, /*queueSize=*/100, true);
            for (int i = 0; i < 2; ++i) {
                auto workItem = new autil::LambdaWorkItem([&]() {
                    try {
                        auto ec = MergePackageUtil::ConvertDirToPackage(
                                      workingDirectory->GetIDirectory(), outputDirectory->GetIDirectory(),
                                      /*parentDirName=*/"src", mergePackageMeta,
                                      /*packagingThresholdBytes=*/10 * PAGE_SIZE, /*threadCount=*/0)
                                      .Code();
                        if (ec != FSEC_OK) {
                            AUTIL_LOG(ERROR, "ConvertDirToPackage failed, ec=%d", ec);
                        }
                    } catch (const std::exception& e) {
                        AUTIL_LOG(ERROR, "ConvertDirToPackage failed, e=%s", e.what());
                    }
                });
                if (autil::ThreadPool::ERROR_NONE != threadPool.pushWorkItem(workItem)) {
                    autil::ThreadPool::dropItemIgnoreException(workItem);
                }
            }
            threadPool.start("");
            threadPool.waitFinish();
        },
        /*failureTypeCap=*/3));
    runner.AddVerification([&outputDirectory]() -> bool {
        PackageFileMeta packageFileMeta;
        auto ec = packageFileMeta.Load(outputDirectory->GetPhysicalPath("") + "/package_file.__meta__");
        if (ec != FSEC_OK) {
            return false;
        }
        return PackageTestUtil::CheckInnerFileMeta(
            "inner_dir1:DIR:0:0:0#inner_dir2:DIR:0:0:0#inner_dir3:DIR:0:0:0#file4:FILE:81920:0:0#file1:FILE:20480:0:1#"
            "inner_dir1/file2:FILE:36864:0:2#inner_dir2/file3:FILE:28673:0:3#inner_dir3/"
            "file5:FILE:8192:32768:3#inner_dir3/"
            "file6:FILE:12288:0:4",
            packageFileMeta);
    });

    runner.Run();

    PackageFileMeta packageFileMeta;
    auto ec = packageFileMeta.Load(outputDirectory->GetPhysicalPath("") + "/package_file.__meta__");
    ASSERT_EQ(ec, FSEC_OK);
    EXPECT_TRUE(PackageTestUtil::CheckInnerFileMeta(
        "inner_dir1:DIR:0:0:0#inner_dir2:DIR:0:0:0#inner_dir3:DIR:0:0:0#file4:FILE:81920:0:0#file1:FILE:20480:0:1#"
        "inner_dir1/file2:FILE:36864:0:2#inner_dir2/file3:FILE:28673:0:3#inner_dir3/"
        "file5:FILE:8192:32768:3#inner_dir3/"
        "file6:FILE:12288:0:4",
        packageFileMeta));
}

TEST_F(MergePackageUtilTest, CleanSrcIndexFilesException)
{
    indexlib::util::ExceptionRunner runner(/*depth=*/3);
    auto fileSystem = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto rootDir = Directory::Get(fileSystem);
    auto srcDirectory = rootDir->MakeDirectory("src");
    srcDirectory->MakeDirectory("inner_dir1");
    srcDirectory->MakeDirectory("inner_dir2");
    srcDirectory->MakeDirectory("inner_dir3");
    srcDirectory->MakeDirectory("inner_dir4");
    srcDirectory->Store("file1", "12345");
    srcDirectory->Store("file2", "123456789");
    srcDirectory->Store("file3", "1234567890123");
    srcDirectory->Store("inner_dir1/file", "1234567");
    srcDirectory->Store("inner_dir2/file", "12345678901234567890");
    srcDirectory->Store("inner_dir3/file", "123");

    auto directoryWithPackaingPlan = rootDir->MakeDirectory("directory_with_packaging_plan");
    PackagingPlan packagingPlan;
    FileListToPackage& fileListToPackage = packagingPlan.dstPath2SrcFilePathsMap["unused-dst-path"];
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("file1"));
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("file2"));
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("inner_dir1/file"));
    fileListToPackage.filePathList.emplace_back(srcDirectory->GetPhysicalPath("inner_dir2/file"));
    packagingPlan.srcDirPaths = {"inner_dir1", "inner_dir2"};
    directoryWithPackaingPlan->Store("packaging_plan", autil::legacy::ToJsonString(packagingPlan));

    runner.AddReset([]() -> void {});
    runner.AddAction(indexlib::util::Action(
        [&srcDirectory, &directoryWithPackaingPlan]() {
            autil::ThreadPool threadPool(/*threadNum=*/2, /*queueSize=*/100, true);
            for (int i = 0; i < 2; ++i) {
                auto workItem = new autil::LambdaWorkItem([&]() {
                    try {
                        auto ec = MergePackageUtil::CleanSrcIndexFiles(srcDirectory->GetIDirectory(),
                                                                       directoryWithPackaingPlan->GetIDirectory())
                                      .Code();
                        if (ec != FSEC_OK) {
                            AUTIL_LOG(ERROR, "CleanSrcIndexFiles failed, ec=%d", ec);
                        }
                    } catch (const std::exception& e) {
                        AUTIL_LOG(ERROR, "CleanSrcIndexFiles failed, e=%s", e.what());
                    }
                });
                if (autil::ThreadPool::ERROR_NONE != threadPool.pushWorkItem(workItem)) {
                    autil::ThreadPool::dropItemIgnoreException(workItem);
                }
            }
            threadPool.start("");
            threadPool.waitFinish();
        },
        /*failureTypeCap=*/3));
    runner.AddVerification([&]() -> bool {
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file1", false);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file2", false);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file3", true);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1", false);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1/file", false);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2", false);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2/file", false);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3", true);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3/file", true);
        CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir4", true);
        return true;
    });
    runner.Run();

    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file1", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file2", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/file3", true);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir1/file", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir2/file", false);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3", true);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir3/file", true);
    CheckExistencePhysical(srcDirectory->GetPhysicalPath("") + "/inner_dir4", true);
}

TEST_F(MergePackageUtilTest, TestProdMergePlan)
{
    std::string content;
    auto ec = fslib::fs::FileSystem::readFile(GET_PRIVATE_TEST_DATA_PATH() + "/prod-packaging-plan.json", content);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "readFile failed");
    }
    PackagingPlan packagingPlan;
    indexlib::file_system::FileSystemOptions fsOptions;
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto dir = indexlib::file_system::Directory::Get(fs);
    auto tmpDir = dir->MakeDirectory("tmp");
    autil::legacy::FromJsonString(packagingPlan, content);
    auto ec2 = MergePackageUtil::GeneratePackageMeta(tmpDir->GetIDirectory(), dir->GetIDirectory(),
                                                     /*parentDirName=*/"segment_0_level_0", packagingPlan);
    if (ec2 != FSEC_OK) {
        AUTIL_LOG(ERROR, "GeneratePackageMeta failed");
    }
}

TEST_F(MergePackageUtilTest, ConvertDirToPackageMultiThread)
{
    size_t PAGE_SIZE = getpagesize();
    auto fileSystem = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto rootDir = Directory::Get(fileSystem);
    auto srcDirectory = rootDir->MakeDirectory("src");
    srcDirectory->MakeDirectory("inner_dir1");
    srcDirectory->MakeDirectory("inner_dir2");
    srcDirectory->MakeDirectory("inner_dir3");
    srcDirectory->Store("file1", GenerateTestString(5 * PAGE_SIZE, '0'));
    srcDirectory->Store("inner_dir1/file2", GenerateTestString(9 * PAGE_SIZE, '1'));
    srcDirectory->Store("inner_dir2/file3", GenerateTestString(7 * PAGE_SIZE + 1, '2'));
    srcDirectory->Store("file4", GenerateTestString(20 * PAGE_SIZE, '3'));
    srcDirectory->Store("inner_dir3/file5", GenerateTestString(2 * PAGE_SIZE, '4'));
    srcDirectory->Store("inner_dir3/file6", GenerateTestString(3 * PAGE_SIZE + 2, '5'));
    srcDirectory->Store("inner_dir3/file7", GenerateTestString(6 * PAGE_SIZE + 1, '6'));
    srcDirectory->Store("inner_dir3/file8", GenerateTestString(PAGE_SIZE + 5, '7'));
    auto outputDirectory = rootDir->MakeDirectory("output");
    auto workingDirectory = rootDir->MakeDirectory("working");

    MergePackageMeta mergePackageMeta;
    std::map<std::string, size_t>& file2SizeMap = mergePackageMeta.packageTag2File2SizeMap["unused-tag"];
    file2SizeMap[srcDirectory->GetPhysicalPath("file1")] = 5 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir1/file2")] = 9 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir2/file3")] = 7 * PAGE_SIZE + 1;
    file2SizeMap[srcDirectory->GetPhysicalPath("file4")] = 20 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file5")] = 2 * PAGE_SIZE;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file6")] = 3 * PAGE_SIZE + 2;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file7")] = 6 * PAGE_SIZE + 1;
    file2SizeMap[srcDirectory->GetPhysicalPath("inner_dir3/file8")] = PAGE_SIZE + 5;

    mergePackageMeta.dirSet = {"inner_dir1", "inner_dir2", "inner_dir3"};

    auto ec = MergePackageUtil::ConvertDirToPackage(workingDirectory->GetIDirectory(), outputDirectory->GetIDirectory(),
                                                    /*parentDirName=*/"src", mergePackageMeta,
                                                    /*packagingThresholdBytes=*/10 * PAGE_SIZE, /*threadCount=*/5);
    ASSERT_EQ(ec, FSEC_OK);
    PackageFileMeta packageFileMeta;
    ec = packageFileMeta.Load(util::PathUtil::JoinPath(outputDirectory->GetPhysicalPath(""), "package_file.__meta__"));
    ASSERT_EQ(ec, FSEC_OK);
    EXPECT_TRUE(PackageTestUtil::CheckInnerFileMeta(
        "inner_dir1:DIR:0:0:0#inner_dir2:DIR:0:0:0#inner_dir3:DIR:0:0:0#file4:FILE:81920:0:0#file1:FILE:20480:0:1#"
        "inner_dir1/file2:FILE:36864:0:2#inner_dir2/file3:FILE:28673:0:3#inner_dir3/file5:FILE:8192:32768:3#inner_dir3/"
        "file6:FILE:12290:0:4#inner_dir3/file7:FILE:24577:0:5#inner_dir3/file8:FILE:4101:28672:5#",
        packageFileMeta));

    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.0", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.1", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.2", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.3", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.4", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.5", true);
    CheckExistencePhysical(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.6", false);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.0", 20 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.1", 5 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.2", 9 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.3", 10 * PAGE_SIZE);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.4", 3 * PAGE_SIZE + 2);
    CheckFileLength(outputDirectory->GetPhysicalPath("") + "/package_file.__data__.unused-tag.5", 8 * PAGE_SIZE + 5);
}

} // namespace indexlib::file_system
