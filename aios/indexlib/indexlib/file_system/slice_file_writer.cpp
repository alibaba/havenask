#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/file_system/storage.h"
#include "indexlib/file_system/slice_file_node.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SliceFileWriter);

SliceFileWriter::SliceFileWriter(const SliceFileNodePtr& sliceFileNode) 
    : mSliceFileNode(sliceFileNode) 
{
}

SliceFileWriter::~SliceFileWriter() 
{
    if (mSliceFileNode)
    {
        IE_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]",
               mSliceFileNode->GetPath().c_str(),
               misc::IoExceptionController::HasFileIOException());
        assert(misc::IoExceptionController::HasFileIOException()); 
    }
}

void SliceFileWriter::Open(const string& path)
{
}

size_t SliceFileWriter::Write(const void* buffer, size_t length)
{
    assert(mSliceFileNode);
    return mSliceFileNode->Write(buffer, length);
}

size_t SliceFileWriter::GetLength() const
{
    assert(mSliceFileNode);
    return mSliceFileNode->GetLength();
}

const std::string& SliceFileWriter::GetPath() const
{
    assert(mSliceFileNode);
    return mSliceFileNode->GetPath();
}

void SliceFileWriter::Close()
{
    if (mSliceFileNode)
    {
        mSliceFileNode.reset();
    }
}

IE_NAMESPACE_END(file_system);

