#include "build_service/builder/test/IndexBuilderCreator.h"
#include <indexlib/config/index_partition_schema_maker.h>

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

namespace build_service {
namespace builder {
BS_LOG_SETUP(builder, IndexBuilderCreator);

IndexBuilderCreator::IndexBuilderCreator() {
}

IndexBuilderCreator::~IndexBuilderCreator() {
}

MockIndexBuilderPtr IndexBuilderCreator::CreateMockIndexBuilder(const string& rootDir)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            //Field schema
            "int:INT32",
            //Primary key index schema
            "pk:primarykey64:int",
            //Attribute schema
            "int",
            //Summary schema
            "" );
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder;
    return MockIndexBuilderPtr(new MockIndexBuilder(options, rootDir, schema));
}

IndexBuilderPtr IndexBuilderCreator::CreateIndexBuilder(
        const string& rootDir, const IndexPartitionSchemaPtr& schema)
{
    IndexPartitionOptions options;
    IndexBuilderPtr indexBuilder;
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    indexBuilder.reset(new IndexBuilder(rootDir, options, schema, quotaControl));
    indexBuilder->Init();
    return indexBuilder;
}

}
}
