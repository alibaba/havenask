#include "indexlib/partition/remote_access/test/var_num_patch_data_writer_unittest.h"

#include <iostream>

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/config/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::config;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, VarNumPatchDataWriterTest);

VarNumPatchDataWriterTest::VarNumPatchDataWriterTest() {}

VarNumPatchDataWriterTest::~VarNumPatchDataWriterTest() {}

void VarNumPatchDataWriterTest::CaseSetUp() {}

void VarNumPatchDataWriterTest::CaseTearDown() {}

void VarNumPatchDataWriterTest::TestSimpleProcess()
{
    VarNumPatchDataWriter writer;
    MergeIOConfig mergeIOConfig;
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("value:string:true", "valueIndex:string:value", "value", "");
    SchemaMaker::EnableNullFields(schema, "value");
    auto attrConfig = schema->GetAttributeSchema()->GetAttributeConfig("value");
    writer.Init(attrConfig, mergeIOConfig, GET_PARTITION_DIRECTORY());
    writer.AppendNullValue();
    ASSERT_EQ(1u, writer.mDataWriter->GetDataItemCount());
    auto file = writer.TEST_GetDataFileWriter();
    ASSERT_EQ(4u, file->GetLength());
    writer.Close();
}
}} // namespace indexlib::partition
