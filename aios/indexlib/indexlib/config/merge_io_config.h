#ifndef __INDEXLIB_MERGE_IO_CONFIG_H
#define __INDEXLIB_MERGE_IO_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/io_config.h"

IE_NAMESPACE_BEGIN(config);

typedef storage::IOConfig MergeIOConfig;

DEFINE_SHARED_PTR(MergeIOConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MERGE_IO_CONFIG_H
