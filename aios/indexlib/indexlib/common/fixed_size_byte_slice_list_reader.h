#ifndef __INDEXLIB_FIXED_SIZE_BYTE_SLICE_LIST_READER_H
#define __INDEXLIB_FIXED_SIZE_BYTE_SLICE_LIST_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"

IE_NAMESPACE_BEGIN(common);

typedef ByteSliceReader FixedSizeByteSliceListReader;
DEFINE_SHARED_PTR(FixedSizeByteSliceListReader);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_FIXED_SIZE_BYTE_SLICE_LIST_READER_H
