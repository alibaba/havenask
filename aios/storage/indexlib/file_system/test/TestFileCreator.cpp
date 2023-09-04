#include "indexlib/file_system/test/TestFileCreator.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SliceFileWriter.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, TestFileCreator);

TestFileCreator::TestFileCreator() {}

TestFileCreator::~TestFileCreator() {}

std::shared_ptr<FileWriter> TestFileCreator::CreateWriter(const std::shared_ptr<IFileSystem>& fileSystem,
                                                          FSStorageType storageType, FSOpenType openType,
                                                          const string& content, const string& path,
                                                          const WriterOption& writerOption)
{
    string dirPath = DEFAULT_DIRECTORY;
    string filePath = PathUtil::JoinPath(dirPath, DEFAULT_FILE_NAME);
    if (!path.empty()) {
        dirPath = PathUtil::GetParentDirPath(path);
        filePath = path;
    }

    if (fileSystem->IsExist(dirPath).GetOrThrow()) {
        // FileStat fileStat;
        // fileSystem->TEST_GetFileStat(dirPath, fileStat);
        // assert(fileStat.storageType == storageType);
    } else {
        THROW_IF_FS_ERROR(fileSystem->MakeDirectory(dirPath, DirectoryOption()), "");
    }

    std::shared_ptr<FileWriter> fileWriter;
    if (openType == FSOT_SLICE) {
        fileWriter = fileSystem->CreateFileWriter(filePath, WriterOption::Slice(DEFAULT_SLICE_LEN, DEFAULT_SLICE_NUM))
                         .GetOrThrow();
    } else {
        fileWriter = fileSystem->CreateFileWriter(filePath, writerOption).GetOrThrow();
    }
    if (!content.empty()) {
        fileWriter->Write(content.data(), content.size()).GetOrThrow();
    }
    return fileWriter;
}

std::shared_ptr<FileReader> TestFileCreator::CreateReader(const std::shared_ptr<IFileSystem>& fileSystem,
                                                          FSStorageType storageType, FSOpenType openType,
                                                          const string& content, const string& path)
{
    string filePath = path;
    if (path.empty()) {
        filePath = GetDefaultFilePath("");
    }
    if (!fileSystem->IsExist(filePath).GetOrThrow()) {
        std::shared_ptr<FileWriter> writer = CreateWriter(fileSystem, storageType, openType, content, filePath);
        writer->Close().GetOrThrow();
    }

    return fileSystem->CreateFileReader(filePath, openType).GetOrThrow();
}

std::shared_ptr<SliceFileReader> TestFileCreator::CreateSliceFileReader(const std::shared_ptr<IFileSystem>& fileSystem,
                                                                        FSStorageType storageType,
                                                                        const string& content, const string& path)
{
    // TODO: not support no cache file system
    string filePath = GetDefaultFilePath("");
    if (!fileSystem->IsExist(filePath).GetOrThrow()) {
        std::shared_ptr<FileWriter> writer = CreateWriter(fileSystem, storageType, FSOT_SLICE, content);
        writer->Close().GetOrThrow();
    }

    if (!path.empty()) {
        filePath = path;
    }

    std::shared_ptr<SliceFileReader> sliceFileReader =
        std::dynamic_pointer_cast<SliceFileReader>(fileSystem->CreateFileReader(filePath, FSOT_SLICE).GetOrThrow());
    assert(sliceFileReader);
    return sliceFileReader;
}

string TestFileCreator::GetDefaultFilePath(const string& path)
{
    return PathUtil::JoinPath(path, DEFAULT_DIRECTORY, DEFAULT_FILE_NAME);
}

void TestFileCreator::CreateFiles(const std::shared_ptr<IFileSystem>& fileSystem, FSStorageType storageType,
                                  FSOpenType openType, const string& fileContext, const string& path, int32_t fileCount)
{
    string filePath = path;
    for (int i = 0; i < fileCount; i++) {
        if (fileCount > 1) {
            filePath = path + StringUtil::toString(i);
        }
        std::shared_ptr<FileWriter> writer = CreateWriter(fileSystem, storageType, openType, fileContext, filePath);
        writer->Close().GetOrThrow();
    }
}

void TestFileCreator::CreateFile(const std::shared_ptr<IFileSystem>& fileSystem, FSStorageType storageType,
                                 FSOpenType openType, const string& fileContext, const string& path)
{
    string filePath = path;
    std::shared_ptr<FileWriter> writer = CreateWriter(fileSystem, storageType, openType, fileContext, filePath);
    writer->Close().GetOrThrow();
}

// treeStr = #:local,inmem^I,file^F; #/local:local_0; #/inmem:inmem_0
void TestFileCreator::CreateFileTree(const std::shared_ptr<IFileSystem>& fileSystem, const string& treeStr)
{
    string trimedTreeStr;
    for (size_t i = 0; i < treeStr.size(); ++i) {
        if (treeStr[i] != ' ') {
            trimedTreeStr += treeStr[i];
        }
    }
    vector<string> dirEntrys = StringUtil::split(trimedTreeStr, ";");
    for (size_t i = 0; i < dirEntrys.size(); ++i) {
        CreateOneDirectory(fileSystem, dirEntrys[i]);
    }
}

// dirEntry = #:local,inmem^I,file^F
void TestFileCreator::CreateOneDirectory(const std::shared_ptr<IFileSystem>& fileSystem, const string& dirEntry)
{
    vector<string> pathEntrys = StringUtil::split(dirEntry, ":");
    if (pathEntrys.size() < 2) {
        return;
    }

    assert(pathEntrys.size() == 2);
    string commonPath = pathEntrys[0];
    if (commonPath[0] == '#') {
        commonPath = commonPath.substr(1);
    }

    if (commonPath[0] == '/') {
        commonPath = commonPath.substr(1);
    }

    vector<string> subPathEntrys;
    StringUtil::fromString(pathEntrys[1], subPathEntrys, ",");
    for (size_t i = 0; i < subPathEntrys.size(); ++i) {
        string pathEntry = PathUtil::JoinPath(commonPath, subPathEntrys[i]);
        CreateOnePath(fileSystem, pathEntry);
    }
}

// pathEntry = ROOT/inmem^I
void TestFileCreator::CreateOnePath(const std::shared_ptr<IFileSystem>& fileSystem, const string& pathEntry)
{
    vector<string> paths = StringUtil::split(pathEntry, "^", false);
    assert(paths.size() > 0);
    string path = paths[0];

    if (paths.size() == 1) {
        if (!path.empty()) {
            THROW_IF_FS_ERROR(fileSystem->MakeDirectory(path, DirectoryOption()), "");
        }
        return;
    }

    assert(paths.size() == 2);
    string type = paths[1];
    if (type == "I") {
        if (!path.empty()) {
            THROW_IF_FS_ERROR(fileSystem->MakeDirectory(path, DirectoryOption()), "");
        }
    } else if (type == "F") {
        std::shared_ptr<FileWriter> writer = fileSystem->CreateFileWriter(path, WriterOption()).GetOrThrow();
        writer->Close().GetOrThrow();
    } else if (type == "S") {
        std::shared_ptr<FileWriter> writer =
            fileSystem->CreateFileWriter(path, WriterOption::Slice(DEFAULT_SLICE_LEN, DEFAULT_SLICE_NUM)).GetOrThrow();
        writer->Close().GetOrThrow();
    } else {
        assert(false);
    }
}
}} // namespace indexlib::file_system
