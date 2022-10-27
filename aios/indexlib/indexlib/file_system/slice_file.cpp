#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_node.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/slice_file_writer.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SliceFile);

SliceFile::SliceFile(const SliceFileNodePtr& sliceFileNode)
    : mSliceFileNode(sliceFileNode)
{
}

SliceFile::~SliceFile() 
{
    Close();
}

void SliceFile::Close()
{
    if (mSliceFileNode)
    {
        mSliceFileNode.reset();
    }
}

SliceFileWriterPtr SliceFile::CreateSliceFileWriter() const
{
    return SliceFileWriterPtr(new SliceFileWriter(mSliceFileNode));
}

SliceFileReaderPtr SliceFile::CreateSliceFileReader() const
{
    return SliceFileReaderPtr(new SliceFileReader(mSliceFileNode));
}

void SliceFile::ReserveSliceFile(size_t sliceFileLen)
{
    const size_t bufSize = 4 * 1024; //4k
    SliceFileWriterPtr sliceFileWriter = CreateSliceFileWriter();
    char buffer[bufSize] = {0};
    for (size_t i = 0; i < sliceFileLen / bufSize; i++)
    {
        sliceFileWriter->Write(buffer, bufSize);
    }
    uint64_t leftSize = sliceFileLen % bufSize;
    if (leftSize > 0)
    {
        sliceFileWriter->Write(buffer, leftSize);
    }
    sliceFileWriter->Close();
}
IE_NAMESPACE_END(file_system);

