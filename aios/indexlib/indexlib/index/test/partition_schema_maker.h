#ifndef __INDEXLIB_PARTITION_SCHEMA_MAKER_H
#define __INDEXLIB_PARTITION_SCHEMA_MAKER_H

#include <vector>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/index_partition_schema_maker.h"

IE_NAMESPACE_BEGIN(index);

typedef config::IndexPartitionSchemaMaker PartitionSchemaMaker;
typedef std::tr1::shared_ptr<PartitionSchemaMaker> PartitionSchemaMakerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PARTITION_SCHEMA_MAKER_H
