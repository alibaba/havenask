#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/snappy_compress_file_writer.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributePatchMerger);

AttributePatchMerger::AttributePatchMerger(
        const AttributeConfigPtr& attrConfig,
        const SegmentUpdateBitmapPtr& segUpdateBitmap)
    : mAttrConfig(attrConfig)
    , mSegUpdateBitmap(segUpdateBitmap)
{
}

AttributePatchMerger::~AttributePatchMerger() 
{
}

void AttributePatchMerger::CopyFile(const DirectoryPtr& directory,
                                    const string& srcFileName,
                                    const FileWriterPtr& dstFileWriter)
{
    const int COPY_BUFFER_SIZE = 2 * 1024 * 1024; // 2MB
    std::vector<uint8_t> copyBuffer;
    copyBuffer.resize(COPY_BUFFER_SIZE);
    char* buffer = (char*)copyBuffer.data();

    FileReaderPtr srcFileReader = directory->CreateFileReader(
            srcFileName, FSOT_BUFFERED);
    size_t blockCount = (srcFileReader->GetLength() + COPY_BUFFER_SIZE - 1)
                        / COPY_BUFFER_SIZE;
    for (size_t i = 0; i < blockCount; i++)
    {
        size_t readSize = srcFileReader->Read(buffer, COPY_BUFFER_SIZE);
        assert(readSize > 0);
        dstFileWriter->Write(buffer, readSize);
    }
    srcFileReader->Close();
    dstFileWriter->Close();
}

file_system::FileWriterPtr AttributePatchMerger::CreatePatchFileWriter(
        const FileWriterPtr& destPatchFile) const
{
    if (!mAttrConfig->GetCompressType().HasPatchCompress()) 
    {
        return destPatchFile;
    }
    SnappyCompressFileWriterPtr compressWriter(new SnappyCompressFileWriter);
    compressWriter->Init(destPatchFile);
    return compressWriter;
}

IE_NAMESPACE_END(index);

