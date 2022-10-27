#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, TestFileCreator);

TestFileCreator::TestFileCreator() 
{
}

TestFileCreator::~TestFileCreator() 
{
}

FileWriterPtr TestFileCreator::CreateWriter(
        const IndexlibFileSystemPtr& fileSystem,
        FSStorageType storageType, FSOpenType openType, 
        const string& content, const string& path, const FSWriterParam& writerParam)
{
    string dirPath = PathUtil::JoinPath(fileSystem->GetRootPath(),
            DEFAULT_DIRECTORY);
    string filePath = PathUtil::JoinPath(dirPath, DEFAULT_FILE_NAME);
    if (!path.empty())
    {
        dirPath = PathUtil::GetParentDirPath(path);
        filePath = path;
    }

    if (fileSystem->IsExist(dirPath))
    {
        FileStat fileStat;
        fileSystem->GetFileStat(dirPath, fileStat);
        assert(fileStat.storageType == storageType);
    }
    else
    {
        if (storageType == FSST_IN_MEM)
        {
            fileSystem->MountInMemStorage(dirPath);
        }
        else
        {
            fileSystem->MakeDirectory(dirPath);
        }
    }

    FileWriterPtr fileWriter;
    if (openType == FSOT_SLICE)
    {
        const SliceFilePtr& sliceFile = fileSystem->CreateSliceFile(
                filePath, DEFAULT_SLICE_LEN, DEFAULT_SLICE_NUM);
        fileWriter = sliceFile->CreateSliceFileWriter();
    }
    else
    {
        fileWriter = fileSystem->CreateFileWriter(filePath, writerParam);
    }
    if (!content.empty())
    {
        fileWriter->Write(content.data(), content.size());
    }
    return fileWriter;
}

FileReaderPtr TestFileCreator::CreateReader(
        const IndexlibFileSystemPtr& fileSystem, FSStorageType storageType, 
        FSOpenType openType, const string& content, const string& path)
{
    string filePath = path;
    if (path.empty())
    {
        filePath = GetDefaultFilePath(fileSystem->GetRootPath());
    }
    if (!fileSystem->IsExist(filePath))
    {
        FileWriterPtr writer = 
            CreateWriter(fileSystem, storageType, openType, content, filePath);
        writer->Close();
    }

    return fileSystem->CreateFileReader(filePath, openType);
}

vector<FileReaderPtr> TestFileCreator::CreateInPackageFileReader(
            const IndexlibFileSystemPtr& fileSystem,
            FSStorageType storageType, FSOpenType openType, 
            const vector<string>& content, const std::string& path)
{
    string filePath = path;
    if (path.empty())
    {
        filePath = PathUtil::JoinPath(fileSystem->GetRootPath(), 
                DEFAULT_DIRECTORY, DEFAULT_PACKAGE_FILE_NAME);
    }

    if (!fileSystem->MountPackageFile(filePath))
    {
        CreatePackageFile(fileSystem, storageType, content, filePath);
        fileSystem->MountPackageFile(filePath);
    }

    string parentDirPath = PathUtil::GetParentDirPath(filePath);
    vector<FileReaderPtr> fileReaders;
    for (size_t i = 0; i < content.size(); i++)
    {
        string innerFilePath = 
            PathUtil::JoinPath(parentDirPath, StringUtil::toString(i));
        FileReaderPtr fileReader = fileSystem->CreateFileReader(
                innerFilePath, openType);
        fileReaders.push_back(fileReader);
        assert(innerFilePath == fileReader->GetPath());
    }
    return fileReaders;
}

void TestFileCreator::CreatePackageFile(
            const IndexlibFileSystemPtr& fileSystem,
            FSStorageType storageType, const vector<string>& contents, 
            const string& path)
{
    string rootPath = PathUtil::NormalizePath(fileSystem->GetRootPath()) + "/";
    assert(path.find(rootPath) == 0);
    string relativePath = path.substr(rootPath.size());

    size_t pos = relativePath.find("/");
    assert(pos != string::npos);

    string dirName = relativePath.substr(0, pos);
    string packFileName = relativePath.substr(pos + 1);
    assert(!packFileName.empty());
     
    string packageFilePath;
    if (storageType == FSST_LOCAL)
    {
        fileSystem->MountInMemStorage(rootPath + dirName + "_tmp");
        packageFilePath = 
            rootPath + dirName + "_tmp/" + packFileName;
    }
    else
    {
        fileSystem->MountInMemStorage(rootPath + dirName);
        packageFilePath = 
            rootPath + dirName + "/" + packFileName;
    }
    
    PackageFileWriterPtr packFileWriter =
        fileSystem->CreatePackageFileWriter(packageFilePath);
    for (size_t i = 0; i < contents.size(); i++)
    {
        FileWriterPtr fileWriter = 
            packFileWriter->CreateInnerFileWriter(StringUtil::toString(i));
        fileWriter->Write(contents[i].data(), contents[i].size());
        fileWriter->Close();
    }
    packFileWriter->Close();

    if (storageType == FSST_LOCAL)
    {
        fileSystem->Sync(true);
        FileSystemWrapper::Rename(rootPath + dirName + "_tmp",
                rootPath + dirName);
    }
}

SliceFileReaderPtr TestFileCreator::CreateSliceFileReader(
        const IndexlibFileSystemPtr& fileSystem, FSStorageType storageType, 
        const string& content, const string& path)
{
    //TODO: not support no cache file system
    string filePath = GetDefaultFilePath(fileSystem->GetRootPath());
    if (!fileSystem->IsExist(filePath))
    {
        FileWriterPtr writer = 
            CreateWriter(fileSystem, storageType, FSOT_SLICE, content);
        writer->Close();
    }

    if (!path.empty())
    {
        filePath = path;
    }

    SliceFilePtr sliceFile = fileSystem->GetSliceFile(filePath);
    assert(sliceFile);
    return sliceFile->CreateSliceFileReader();
}

string TestFileCreator::GetDefaultFilePath(const string& path)
{
    return PathUtil::JoinPath(path, DEFAULT_DIRECTORY, DEFAULT_FILE_NAME);
}

void TestFileCreator::CreateFiles(const IndexlibFileSystemPtr& fileSystem,
                                  FSStorageType storageType, FSOpenType openType,
                                  const string& fileContext, const string& path, 
                                  int32_t fileCount)
{
    string filePath = path;
    for (int i = 0; i < fileCount; i++)
    {
        if (fileCount > 1)
        {
            filePath = path + StringUtil::toString(i);
        }
        FileWriterPtr writer = CreateWriter(fileSystem, storageType,
                openType, fileContext, filePath);
        writer->Close();
    }
}

// treeStr = #:local,inmem^I,file^F; #/local:local_0; #/inmem:inmem_0
void TestFileCreator::CreateFileTree(const IndexlibFileSystemPtr& fileSystem,
                                     const string& treeStr)
{
    string trimedTreeStr;
    for (size_t i = 0; i < treeStr.size(); ++i)
    {
        if (treeStr[i] != ' ')
        {
            trimedTreeStr += treeStr[i];
        }
    }
    vector<string> dirEntrys = StringUtil::split(trimedTreeStr, ";");
    for (size_t i = 0; i < dirEntrys.size(); ++i)
    {
        CreateOneDirectory(fileSystem, dirEntrys[i]);
    }
}

// dirEntry = #:local,inmem^I,file^F
void TestFileCreator::CreateOneDirectory(const IndexlibFileSystemPtr& fileSystem,
        const string& dirEntry)
{
    vector<string> pathEntrys = StringUtil::split(dirEntry, ":");
    if (pathEntrys.size() < 2)
    {
        return;
    }

    assert(pathEntrys.size() == 2);
    string commonPath = pathEntrys[0];
    if (commonPath[0] == '#')
    {
        commonPath = PathUtil::JoinPath(
                fileSystem->GetRootPath(), commonPath.substr(1));
    }

    vector<string> subPathEntrys;
    StringUtil::fromString(pathEntrys[1], subPathEntrys, ",");
    for (size_t i = 0; i < subPathEntrys.size(); ++i)
    {
        string pathEntry = PathUtil::JoinPath(commonPath, subPathEntrys[i]);
        CreateOnePath(fileSystem, pathEntry);
    }
}

// pathEntry = ROOT/inmem^I
void TestFileCreator::CreateOnePath(const IndexlibFileSystemPtr& fileSystem,
        const string& pathEntry)
{
    vector<string> paths = StringUtil::split(pathEntry, "^");
    assert(paths.size() > 0);
    string path = paths[0];

    if (paths.size() == 1)
    {
        fileSystem->MakeDirectory(path);
        return;
    }

    assert(paths.size() == 2);
    string type = paths[1];
    if (type == "I")
    {
        fileSystem->MountInMemStorage(path);
    }
    else if (type == "F")
    {
        FileWriterPtr writer = fileSystem->CreateFileWriter(path);
        writer->Close();
    }
    else if (type == "S")
    {
        fileSystem->CreateSliceFile(path, DEFAULT_SLICE_LEN, DEFAULT_SLICE_NUM);
    }
    else
    {
        assert(false);
    }
}

IE_NAMESPACE_END(file_system);

