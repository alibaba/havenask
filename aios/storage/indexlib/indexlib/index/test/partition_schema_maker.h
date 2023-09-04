#ifndef __INDEXLIB_PARTITION_SCHEMA_MAKER_H
#define __INDEXLIB_PARTITION_SCHEMA_MAKER_H

#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

typedef config::IndexPartitionSchemaMaker PartitionSchemaMaker;
typedef std::shared_ptr<PartitionSchemaMaker> PartitionSchemaMakerPtr;
}} // namespace indexlib::index

#endif //__INDEXLIB_PARTITION_SCHEMA_MAKER_H
