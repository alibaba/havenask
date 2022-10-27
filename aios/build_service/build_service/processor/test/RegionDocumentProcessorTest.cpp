#include "build_service/test/unittest.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/test/ProcessorTestUtil.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/CLIOptionNames.h"

#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/index_partition_options.h>
using namespace std;
using namespace testing;

using namespace build_service::config;
using namespace build_service::document;
using namespace heavenask::indexlib;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace processor {

class RegionDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RegionDocumentProcessorTest::setUp() {
}

void RegionDocumentProcessorTest::tearDown() {
}

TEST_F(RegionDocumentProcessorTest, testSingleRegionDispatch) {
    RegionDocumentProcessor processor;

    vector<string> clusternames = {"multi_region"};
    string configPath = TEST_DATA_PATH"region_document_processor_test/config/";
    string schemaFileName = ResourceReader::getSchemaConfRelativePath("multi_region");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.clusterNames = clusternames;
    param.schemaPtr = schema;
    param.parameters[REGION_DISPATCHER_CONFIG] = FIELD_DISPATCHER_TYPE;
    param.parameters[REGION_FIELD_NAME_CONFIG] = "region_field"; 
    
    ASSERT_TRUE(processor.init(param));

    auto checkRegionId = [this, &processor] (const string &docStr,
            regionid_t expectedRegionId) {
        ExtendDocumentPtr extendDoc = ProcessorTestUtil::createExtendDoc(docStr);
        if (expectedRegionId < 0) {
            EXPECT_FALSE(processor.process(extendDoc));
        } else {
            EXPECT_TRUE(processor.process(extendDoc));
            EXPECT_EQ(expectedRegionId, extendDoc->getRegionId());
        }
    };
    checkRegionId("region_field:region_non_exist", -1);    
    checkRegionId("region_field:region1", 0);
    checkRegionId("region_field:region2", 1);    
    checkRegionId("region_field:region3", 2);
    checkRegionId("region_field:region4", 3); 
    checkRegionId("region_field:region5", 4); 
}

TEST_F(RegionDocumentProcessorTest, testInit) {
    vector<string> clusternames = {"multi_region"};
    string configPath = TEST_DATA_PATH"region_document_processor_test/config/";
    string schemaFileName = ResourceReader::getSchemaConfRelativePath("multi_region");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    {
        RegionDocumentProcessor processor;
        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusternames;
        param.schemaPtr = schema;
        param.parameters[REGION_DISPATCHER_CONFIG] = REGION_ID_DISPATCHER_TYPE;
        param.parameters[REGION_ID_CONFIG] = "0";
        ASSERT_TRUE(processor.init(param));
    }
    {
        RegionDocumentProcessor processor;
        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusternames;
        param.schemaPtr = schema;
        param.parameters[REGION_DISPATCHER_CONFIG] = FIELD_DISPATCHER_TYPE;
        param.parameters[REGION_FIELD_NAME_CONFIG] = "test_region_field"; 
        ASSERT_TRUE(processor.init(param));
    }     
    {
        RegionDocumentProcessor processor;
        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusternames;
        param.schemaPtr = schema;
        param.parameters[REGION_DISPATCHER_CONFIG] = FIELD_DISPATCHER_TYPE;
        ASSERT_FALSE(processor.init(param));
    } 
    {
        RegionDocumentProcessor processor;
        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusternames;
        param.schemaPtr = schema;
        param.parameters[REGION_DISPATCHER_CONFIG] = REGION_ID_DISPATCHER_TYPE;
        param.parameters[REGION_ID_CONFIG] = "-1";
        ASSERT_FALSE(processor.init(param));
    }
    {
        RegionDocumentProcessor processor;
        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusternames;
        param.schemaPtr = schema;
        param.parameters[REGION_DISPATCHER_CONFIG] = REGION_ID_DISPATCHER_TYPE;
        param.parameters[REGION_ID_CONFIG] = "100";
        ASSERT_FALSE(processor.init(param));
    }
    {
        RegionDocumentProcessor processor;
        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusternames;
        param.schemaPtr = schema;
        param.parameters[REGION_DISPATCHER_CONFIG] = "non_exist_dispatcher_type";
        ASSERT_FALSE(processor.init(param));
    }     
}


}
}
