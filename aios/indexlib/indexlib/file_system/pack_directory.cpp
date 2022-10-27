#include "indexlib/file_system/pack_directory.h"
#include "indexlib/file_system/package_file_writer.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackDirectory);

PackDirectory::PackDirectory(
        const string& path, 
        const IndexlibFileSystemPtr& indexFileSystem)
    : Directory(path, indexFileSystem)
{
}

PackDirectory::~PackDirectory() 
{
}

void PackDirectory::Init(const PackageFileWriterPtr& packageFileWriter)
{
    assert(packageFileWriter);
    string packFileParentPath = PathUtil::GetParentDirPath(
            packageFileWriter->GetFilePath());
    
    string relativePath;
    if (!GetRelativePath(packFileParentPath,
                         FileSystemWrapper::NormalizeDir(mRootDir),
                         relativePath))
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "packageFile path[%s] miss match with dirPath[%s]",
                             packageFileWriter->GetFilePath().c_str(),
                             mRootDir.c_str());
    }
    mPackageFileWriter = packageFileWriter; 
}

FileWriterPtr PackDirectory::CreateFileWriter(
    const string& filePath, const FSWriterParam& param)
{
    assert(mPackageFileWriter);
    string absFilePath = PathUtil::JoinPath(mRootDir, filePath);
    string packFileParentPath = PathUtil::GetParentDirPath(
            mPackageFileWriter->GetFilePath());
    
    string relativePath;
    GetRelativePath(packFileParentPath, absFilePath, relativePath);
    return mPackageFileWriter->CreateInnerFileWriter(relativePath, param);
}

FileReaderPtr PackDirectory::CreateFileReader(
            const string& filePath, FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support CreateFileReader!");
    return FileReaderPtr();
}

FileReaderPtr PackDirectory::CreateFileReaderWithoutCache(
        const std::string& filePath, FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support CreateFileReaderWithoutCache!");
    return FileReaderPtr();
}

size_t PackDirectory::EstimateFileMemoryUse(const string& filePath, 
        FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support estimate file memory use!");
    return 0;
}

PackageFileWriterPtr PackDirectory::CreatePackageFileWriter(
            const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support CreatePackageFileWriter!");
    return PackageFileWriterPtr();
}

PackageFileWriterPtr PackDirectory::GetPackageFileWriter(
            const string& filePath) const
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support GetPackageFileWrite!");
    return PackageFileWriterPtr();
}

DirectoryPtr PackDirectory::MakeInMemDirectory(const string& dirName)
{
    return MakeDirectory(dirName);
}

DirectoryPtr PackDirectory::MakeDirectory(const string& dirPath)
{
    assert(mPackageFileWriter);
    string absFilePath = PathUtil::JoinPath(mRootDir, dirPath);
    string packFileParentPath = PathUtil::GetParentDirPath(
            mPackageFileWriter->GetFilePath());
    
    string relativePath;
    GetRelativePath(packFileParentPath, absFilePath, relativePath);
    mPackageFileWriter->MakeInnerDir(relativePath);
    return GetDirectory(dirPath, true);
}

DirectoryPtr PackDirectory::GetDirectory(const string& dirPath,
        bool throwExceptionIfNotExist)
{
    string innerDirPath = PathUtil::JoinPath(mRootDir, dirPath);
    PackDirectoryPtr packDirectory(new PackDirectory(innerDirPath, mFileSystem));
    packDirectory->Init(mPackageFileWriter);
    return packDirectory;
}

FSOpenType PackDirectory::GetIntegratedOpenType(const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support GetIntegratedOpenType!");
    return FSOT_UNKNOWN;
}

bool PackDirectory::GetRelativePath(
        const string& packFileParentPath, 
        const string& path, string& relativePath) const
{
    return PathUtil::GetRelativePath(packFileParentPath, path, relativePath);
}

void PackDirectory::ClosePackageFileWriter()
{
    assert(mPackageFileWriter);
    mPackageFileWriter->Close();
}

InMemoryFilePtr PackDirectory::CreateInMemoryFile(
        const string& path, uint64_t fileLen)
{
    InMemoryFilePtr inMemFile = Directory::CreateInMemoryFile(path, fileLen);
    InMemPackageFileWriter* packFileWriter =
        dynamic_cast<InMemPackageFileWriter*>(mPackageFileWriter.get());
    inMemFile->SetInPackageFileWriter(packFileWriter);
    return inMemFile;
}

void PackDirectory::Rename(const string& srcPath, const DirectoryPtr& destDirectory,
                            const string& destPath)
{
    assert(false);

    string absSrcPath = FileSystemWrapper::JoinPath(mRootDir, srcPath);
    string absDestPath = FileSystemWrapper::JoinPath(destDirectory->GetPath(),
            (destPath.empty() ? srcPath : destPath));
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "Only support LocalDirectory, rename from [%s] to [%s]",
                         absSrcPath.c_str(), absDestPath.c_str());
}

IE_NAMESPACE_END(file_system);

