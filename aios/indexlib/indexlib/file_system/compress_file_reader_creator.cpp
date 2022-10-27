#include "indexlib/file_system/compress_file_reader_creator.h"
#include "indexlib/file_system/normal_compress_file_reader.h"
#include "indexlib/file_system/integrated_compress_file_reader.h"
#include "indexlib/file_system/block_cache_compress_file_reader.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CompressFileReaderCreator);

CompressFileReaderCreator::CompressFileReaderCreator() 
{
}

CompressFileReaderCreator::~CompressFileReaderCreator() 
{
}

CompressFileReaderPtr CompressFileReaderCreator::Create(
        const FileReaderPtr& fileReader,
        const CompressFileInfo& compressInfo, Directory* directory)
{
    assert(fileReader);
    CompressFileReaderPtr compressReader;
    if (fileReader->GetBaseAddress())
    {
        compressReader.reset(new IntegratedCompressFileReader);
    }
    else if (NeedCacheDecompressFile(fileReader, compressInfo))
    {
        compressReader.reset(new BlockCacheCompressFileReader);
    }
    else
    {
        compressReader.reset(new NormalCompressFileReader);
    }
    
    if (!compressReader->Init(fileReader, compressInfo, directory))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "Init compress file reader for [%s] fail!",
                             fileReader->GetPath().c_str());
    }
    return compressReader;
}

bool CompressFileReaderCreator::NeedCacheDecompressFile(
        const FileReaderPtr& fileReader, const CompressFileInfo& compressInfo)
{
    BlockFileNodePtr blockFileNode = DYNAMIC_POINTER_CAST(
            BlockFileNode, fileReader->GetFileNode());
    if (!blockFileNode)
    {
        return false;
    }

    if (!blockFileNode->EnableCacheDecompressFile())
    {
        return false;
    }

    BlockCache* blockCache = blockFileNode->GetBlockCache();
    assert(blockCache);
    if (compressInfo.blockSize != (size_t)blockCache->GetBlockSize())
    {
        IE_LOG(WARN, "blockSize mismatch: compress block size [%lu], "
               "block cache size [%u], will not cache decompress file!",
               compressInfo.blockSize, blockCache->GetBlockSize());
        return false;
    }
    return true;
}

IE_NAMESPACE_END(file_system);

