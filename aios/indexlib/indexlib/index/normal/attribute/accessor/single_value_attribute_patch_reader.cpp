#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/snappy_compress_file_reader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SinglePatchFile);

SinglePatchFile::SinglePatchFile(segmentid_t segmentId,
                                 bool patchCompressed)
    : mFileLength(0)
    , mCursor(0)
    , mDocId(INVALID_DOCID)
    , mSegmentId(segmentId)
    , mPatchCompressed(patchCompressed)
{
}

SinglePatchFile::~SinglePatchFile()
{
}

void SinglePatchFile::Open(
        const DirectoryPtr& directory, const string& fileName)
{
    file_system::FileReaderPtr file =  directory->CreateFileReader(fileName, FSOT_BUFFERED);
    assert(file);
    if (!mPatchCompressed)
    {
        mFile = file;
        mFileLength = mFile->GetLength();
        return;
    }
    SnappyCompressFileReaderPtr compressFileReader(new SnappyCompressFileReader);
    if (!compressFileReader->Init(file))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Read compressed patch file failed, file path is: %s", 
                             file->GetPath().c_str());
    }
    mFile = compressFileReader;
    mFileLength = compressFileReader->GetUncompressedFileLength();   
}

void SinglePatchFile::Open(const string& filePath)
{ 
    string path = PathUtil::GetParentDirPath(filePath);
    string fileName = PathUtil::GetFileName(filePath);
    
    PartitionMemoryQuotaControllerPtr quotaController =
        MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    IndexlibFileSystemPtr fileSystem(
            new IndexlibFileSystemImpl(path));
    FileSystemOptions options;
    options.memoryQuotaController = quotaController;
    fileSystem->Init(options);
    DirectoryPtr directory = DirectoryCreator::Get(fileSystem, path, true);
    
    Open(directory, fileName);
}

IE_NAMESPACE_END(index);

