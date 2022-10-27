#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/snappy_compress_file_reader.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/config/pack_attribute_config.h"

using namespace std;
using namespace fslib;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributePatchFile);

VarNumAttributePatchFile::VarNumAttributePatchFile(
        segmentid_t segmentId,     
        const AttributeConfigPtr& attrConfig)
    : mFileLength(0)
    , mCursor(0)
    , mDocId(INVALID_DOCID)
    , mSegmentId(segmentId)
    , mPatchItemCount(0)
    , mMaxPatchLen(0)
    , mPatchDataLen(0)
    , mFixedValueCount(attrConfig->GetFieldConfig()->GetFixedMultiValueCount())
    , mPatchCompressed(attrConfig->GetCompressType().HasPatchCompress())
{
    if (mFixedValueCount != -1)
    {
        mPatchDataLen = PackAttributeConfig::GetFixLenFieldSize(
                attrConfig->GetFieldConfig());
    }
}

VarNumAttributePatchFile::~VarNumAttributePatchFile()
{
}

void VarNumAttributePatchFile::InitPatchFileReader(
        const DirectoryPtr& directory, const string& fileName)
{
    file_system::FileReaderPtr file = 
        directory->CreateFileReader(fileName, FSOT_BUFFERED);
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

void VarNumAttributePatchFile::Open(
        const DirectoryPtr& directory, const string& fileName)
{
    InitPatchFileReader(directory, fileName);
    if (mFileLength < ((int64_t)sizeof(uint32_t) * 2))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Read patch file failed, file path is: %s", 
                             mFile->GetPath().c_str());
    }

    size_t beginPos = mFileLength - sizeof(uint32_t) * 2;
    if (mFile->Read((void*)&mPatchItemCount, 
                    sizeof(uint32_t), beginPos) < (size_t)sizeof(uint32_t))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Read patch file failed, file path is: %s", 
                             mFile->GetPath().c_str());
    }
    beginPos += sizeof(uint32_t);
    if (mFile->Read((void*)&mMaxPatchLen, 
                    sizeof(uint32_t), beginPos) < (ssize_t)sizeof(uint32_t))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Read patch file failed, file path is: %s", 
                             mFile->GetPath().c_str());
    }
}

void VarNumAttributePatchFile::Open(const string& filePath)
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

