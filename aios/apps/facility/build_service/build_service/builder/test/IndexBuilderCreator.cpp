#include "build_service/builder/test/IndexBuilderCreator.h"

#include "gmock/gmock.h"
#include <iosfwd>
#include <memory>

#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::partition;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, IndexBuilderCreator);

IndexBuilderCreator::IndexBuilderCreator() {}

IndexBuilderCreator::~IndexBuilderCreator() {}

MockIndexBuilderPtr IndexBuilderCreator::CreateMockIndexBuilder(const string& rootDir)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          // Field schema
                                          "int:INT32",
                                          // Primary key index schema
                                          "pk:primarykey64:int",
                                          // Attribute schema
                                          "int",
                                          // Summary schema
                                          "");
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder;
    return MockIndexBuilderPtr(new ::testing::NiceMock<MockIndexBuilder>(options, rootDir, schema));
}

IndexBuilderPtr IndexBuilderCreator::CreateIndexBuilder(const string& rootDir, const IndexPartitionSchemaPtr& schema)
{
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder;
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    indexBuilder.reset(new IndexBuilder(rootDir, options, schema, quotaControl, BuilderBranchHinter::Option::Test()));
    indexBuilder->Init();
    return indexBuilder;
}

}} // namespace build_service::builder
