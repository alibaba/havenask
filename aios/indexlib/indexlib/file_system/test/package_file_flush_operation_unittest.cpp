#include "indexlib/file_system/test/package_file_flush_operation_unittest.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageFileFlushOperationTest);

PackageFileFlushOperationTest::PackageFileFlushOperationTest()
{
}

PackageFileFlushOperationTest::~PackageFileFlushOperationTest()
{
}

void PackageFileFlushOperationTest::CaseSetUp()
{
    mMemoryController = util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void PackageFileFlushOperationTest::CaseTearDown()
{
}

void PackageFileFlushOperationTest::TestExecute()
{
    SCOPED_TRACE("Failed");
    string rootDir = GET_TEST_DATA_PATH();
    string packageFilePath = rootDir + "packagefile";
    size_t pageSize = getpagesize();
    string shortContent = "Hello";
    string longContent = string(pageSize, '*')+shortContent; 
    string fileNodeInfoStr;
    string metaDespStr;
    
    // test valid file path
    fileNodeInfoStr.clear();
    fileNodeInfoStr += MakeInfoStr(rootDir+"a/file1", shortContent, false, false, "#");
    fileNodeInfoStr += MakeInfoStr(rootDir+"a/file2", longContent, false, false, "#");
    fileNodeInfoStr += MakeInfoStr(rootDir+"b/file3", shortContent, false, false);
    
    metaDespStr += MakeMetaStr("a/file1", 0, shortContent.size(), false, "#");
    metaDespStr += MakeMetaStr("a/file2", pageSize*1, longContent.size(), false, "#");
    metaDespStr += MakeMetaStr("b/file3", pageSize*3, shortContent.size(), false, "");
    InnerTestExecute(fileNodeInfoStr, packageFilePath, false,
                     pageSize*3+shortContent.size(), 0, metaDespStr);

    // test AtomicExecute & copyOnDump
    // throw exception if package meta is already existed
    fileNodeInfoStr += "#";
    fileNodeInfoStr += MakeInfoStr(rootDir+"b/file4", shortContent, true, true);
    metaDespStr += "#";
    metaDespStr += MakeMetaStr("b/file4", pageSize*4, shortContent.size(), false, "");
    InnerTestExecute(fileNodeInfoStr, packageFilePath, true, 0, 0);
    
    // success if package meta path is unique 
    InnerTestExecute(fileNodeInfoStr, packageFilePath+"_new", false,
                     pageSize*4+shortContent.size(), shortContent.size(), metaDespStr);
}

void PackageFileFlushOperationTest::TestExecuteWithFlushCache()
{
    string rootDir = GET_TEST_DATA_PATH();
    string packageFilePath = rootDir + "packagefile";
    size_t pageSize = getpagesize();
    string shortContent = "Hello";
    string longContent = string(pageSize, '*')+shortContent; 
    string fileNodeInfoStr;
    string metaDespStr;
    
    // test valid file path
    fileNodeInfoStr.clear();
    fileNodeInfoStr += MakeInfoStr(rootDir+"a/file1", shortContent, false, false, "#");
    fileNodeInfoStr += MakeInfoStr(rootDir+"a/file2", longContent, false, false, "#");
    fileNodeInfoStr += MakeInfoStr(rootDir+"b/file3", shortContent, false, false);
    
    metaDespStr += MakeMetaStr("a/file1", 0, shortContent.size(), false, "#");
    metaDespStr += MakeMetaStr("a/file2", pageSize*1, longContent.size(), false, "#");
    metaDespStr += MakeMetaStr("b/file3", pageSize*3, shortContent.size(), false, "");

    PathMetaContainerPtr flushCache(new PathMetaContainer);
    InnerTestExecute(fileNodeInfoStr, packageFilePath, false,
                     pageSize*3+shortContent.size(), 0, metaDespStr, flushCache);
    ASSERT_TRUE(flushCache->IsExist(rootDir));
}

// fileNodeInfoStr = path1:content1#path2:content2:...
// metaDespStr = path1:offset1:length1:true#path2:offset2:length2:false:...
void PackageFileFlushOperationTest::InnerTestExecute(
        const std::string& fileNodeInfoStr,
        const std::string& packageFilePath,
        bool hasException,
        size_t expectedPackageFileSize,
        int64_t expectedFlushMemoryUse,
        const string& metaDespStr,
        const PathMetaContainerPtr& flushCache)
{
    vector<vector<string> > fileNodeInfos;
    vector<FileNodePtr> fileNodeVec;
    StringUtil::fromString(fileNodeInfoStr, fileNodeInfos, ":", "#");
    vector<string> expectedContents;
    vector<FSDumpParam> dumpParamVec;
    
    for (size_t i = 0; i < fileNodeInfos.size(); i++)
    {
        assert(fileNodeInfos[i].size() == 4);
        FileNodePtr fileNode = CreateFileNode(fileNodeInfos[i][0], fileNodeInfos[i][1]);
        fileNodeVec.push_back(fileNode);
        expectedContents.push_back(fileNodeInfos[i][1]);
        FSDumpParam dumpParam;
        dumpParam.atomicDump = (fileNodeInfos[i][2] == string("true"));
        dumpParam.copyOnDump = (fileNodeInfos[i][3] == string("true"));
        dumpParamVec.push_back(dumpParam);
    }
    
    set<string> innerDirs;
    PackageFileMetaPtr fileMeta = 
        InMemPackageFileWriter::GeneratePackageFileMeta(
            packageFilePath, innerDirs, fileNodeVec);

    PackageFileFlushOperation pFileFlushOperation(
        packageFilePath, fileMeta, fileNodeVec, dumpParamVec, storage::RaidConfigPtr());

    
    if (hasException)
    {
        ASSERT_ANY_THROW(pFileFlushOperation.Execute(flushCache));
    }
    else
    {
        ASSERT_NO_THROW(pFileFlushOperation.Execute(flushCache));
        EXPECT_EQ(expectedFlushMemoryUse, pFileFlushOperation.GetFlushMemoryUse());
        CheckMetaFile(packageFilePath, metaDespStr);
        CheckDataFile(packageFilePath, 0, metaDespStr,
                      expectedPackageFileSize, expectedContents);
    }

}

void PackageFileFlushOperationTest::CheckMetaFile(const string& packageFilePath,
        const string& metaDespStr)
{
    string packageFileMetaPath = packageFilePath + PACKAGE_FILE_META_SUFFIX;
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageFileMetaPath));
    // TODO check content of metafile
    string metaStr;
    FileSystemWrapper::AtomicLoad(packageFileMetaPath, metaStr);

    PackageFileMeta packageFileMeta;
    packageFileMeta.InitFromString(metaStr);
    
    vector<vector<string> > metaDespVec; 
    StringUtil::fromString(metaDespStr, metaDespVec, ":", "#");

    ASSERT_EQ(metaDespVec.size(), packageFileMeta.GetInnerFileCount());

    PackageFileMeta::Iterator iter = packageFileMeta.Begin();
    for (int i = 0; iter != packageFileMeta.End(); ++iter, ++i)
    {
        assert(metaDespVec[i].size() == 4);
        ASSERT_EQ(metaDespVec[i][0], iter->GetFilePath());
        ASSERT_EQ(metaDespVec[i][1], StringUtil::toString(iter->GetOffset()));
        ASSERT_EQ(metaDespVec[i][2], StringUtil::toString(iter->GetLength()));
        if (iter->IsDir())
        {
            ASSERT_EQ(string("true"), metaDespVec[i][3]);
        }
        else
        {
            ASSERT_EQ(string("false"), metaDespVec[i][3]);
        }
    }
}

void PackageFileFlushOperationTest::CheckDataFile(const string& packageFilePath,
        int dataFileID,
        const string& metaDespStr,
        size_t expectedPackageFileSize,
        const vector<string>& expectedContents)
{
    string realPackageFilePath = packageFilePath
                                 + PACKAGE_FILE_DATA_SUFFIX
                                 + StringUtil::toString(dataFileID);

    ASSERT_TRUE(FileSystemWrapper::IsExist(realPackageFilePath));
    vector<vector<string> > metaDespVec; 
    StringUtil::fromString(metaDespStr, metaDespVec, ":", "#");
    size_t fileLength = FileSystemWrapper::GetFileLength(realPackageFilePath);
    ASSERT_EQ(expectedPackageFileSize, fileLength);
    size_t offset, length;

    string pkFileContent;
    FileSystemWrapper::AtomicLoad(realPackageFilePath, pkFileContent);
    
    for (size_t i = 0; i < metaDespVec.size(); i++)
    {
        offset = StringUtil::fromString<uint32_t>(metaDespVec[i][1]);
        length = StringUtil::fromString<uint32_t>(metaDespVec[i][2]);
        if (metaDespVec[i][3] == string("true"))
        {
            continue;
        }

        ASSERT_EQ(expectedContents[i], pkFileContent.substr(offset, length));
    }
}

FileNodePtr PackageFileFlushOperationTest::CreateFileNode(
        const std::string& path, const std::string& content)
{
    FileNodePtr fileNode(new InMemFileNode(content.size(), false, LoadConfig(), mMemoryController));
    fileNode->Open(path, FSOT_IN_MEM);
    fileNode->SetDirty(true);
    fileNode->Write(content.data(), content.size());
    return fileNode;
}

IE_NAMESPACE_END(file_system);

