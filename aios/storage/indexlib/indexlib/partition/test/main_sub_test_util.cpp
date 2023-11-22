#include "indexlib/partition/test/main_sub_test_util.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/test/partition_data_maker.h"

namespace indexlib { namespace partition {

partition::SubDocSegmentWriterPtr
MainSubTestUtil::PrepareSubSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                                         const file_system::IFileSystemPtr& fileSystem)
{
    index_base::PartitionDataPtr partitionData =
        test::PartitionDataMaker::CreatePartitionData(fileSystem, config::IndexPartitionOptions(), schema);
    index_base::InMemorySegmentPtr inMemSeg = partitionData->CreateNewSegment();
    partition::SubDocSegmentWriterPtr subDocSegmentWriter =
        DYNAMIC_POINTER_CAST(partition::SubDocSegmentWriter, inMemSeg->GetSegmentWriter());
    return subDocSegmentWriter;
}

config::IndexPartitionSchemaPtr MainSubTestUtil::CreateSchema()
{
    std::string field = "pkstr:string;text1:text;long1:uint32;";
    std::string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    std::string attr = "long1;";
    std::string summary = "pkstr;";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
    config::IndexPartitionSchemaPtr subSchema = test::SchemaMaker::MakeSchema(
        "substr1:string;subpkstr:string;sub_long:uint32;", "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
        "substr1;subpkstr;sub_long;", "");
    schema->SetSubIndexPartitionSchema(subSchema);
    index_base::SchemaRewriter::RewriteForSubTable(schema);
    return schema;
}

}} // namespace indexlib::partition
