#ifndef __INDEXLIB_SLICE_FILE_H
#define __INDEXLIB_SLICE_FILE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, SliceFileNode);
DECLARE_REFERENCE_CLASS(file_system, SliceFileReader);
DECLARE_REFERENCE_CLASS(file_system, SliceFileWriter);

IE_NAMESPACE_BEGIN(file_system);

class SliceFile
{
public:
    SliceFile(const SliceFileNodePtr& sliceFileNode);
    ~SliceFile();

public:
    void Close();

    SliceFileWriterPtr CreateSliceFileWriter() const;
    SliceFileReaderPtr CreateSliceFileReader() const;
    void ReserveSliceFile(size_t sliceFileLen);
    
private:
    SliceFileNodePtr mSliceFileNode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SliceFile);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SLICE_FILE_H
