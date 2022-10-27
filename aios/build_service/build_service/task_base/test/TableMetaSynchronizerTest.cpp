#include "build_service/test/unittest.h"
#include "build_service/processor/Processor.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/test/ProcessorTestUtil.h"
#include "build_service/document/RawDocument.h"

#include <autil/TimeUtility.h>
#include <autil/AtomicCounter.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/test/partition_state_machine.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);


using namespace std;
using namespace testing;

namespace build_service {
namespace processor {

class MultiRegionTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    vector<IE_NAMESPACE(document)::DocumentPtr> createDocuments(
            Processor& processor, const string& docStr);
private:
    proto::PartitionId _partitionId;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap; 
};

void MultiRegionTest::setUp() {
}

void MultiRegionTest::tearDown() {
}

vector<IE_NAMESPACE(document)::DocumentPtr> MultiRegionTest::createDocuments(
        Processor& processor, const string& docStr)
{
    vector<document::RawDocumentPtr> rawDocs = ProcessorTestUtil::createRawDocuments(docStr, "#");
    vector<IE_NAMESPACE(document)::DocumentPtr> indexDocs;
    for (const auto& rawDoc : rawDocs) {
        processor.processDoc(rawDoc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        for (size_t i = 0; i < processedDocVec->size(); ++i)
        {
            indexDocs.push_back((*processedDocVec)[i]->getDocument());
        }
    }
    return indexDocs;
}

TEST_F(MultiRegionTest, testKKVSimpleBuild) {
    Processor processor;
    proto::PartitionId partitionId;
    partitionId.Clear();
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
    string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
    ResourceReaderPtr resourceReader(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReader, partitionId, NULL, counterMap));


    vector<IE_NAMESPACE(document)::DocumentPtr> docs = createDocuments(processor,
            "nid:0;friendid:100;timestamp:0;#nid:0;friendid:200;timestamp:0;#"
            "nid:1;friendid:101;timestamp:1;#nid:2;friendid:102;timestamp:2;#"
            "nid:3;friendid:103;timestamp:3;#nid:4;friendid:104;timestamp:4;#"
            "nid:4;friendid:200;timestamp:5;#nid:4;friendid:101;timestamp:6;");    
    string fullDocBinStr = PartitionStateMachine::SerializeDocuments(docs);
    string schemaFileName = ResourceReader::getSchemaConfRelativePath("two_region");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                resourceReader->getConfigPath(), schemaFileName);

    ASSERT_TRUE(schema);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(true).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;
    options.SetEnablePackageFile(false); 
    
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    psm.SetPsmOption("documentFormat", "binary");

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocBinStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@0",
                             "nid=0,friendid=100,timestamp=0;nid=0,friendid=200,timestamp=0;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@1",
                             "nid=1,friendid=101,timestamp=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@2",
                             "nid=2,friendid=102,timestamp=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@3",
                             "nid=3,friendid=103,timestamp=3;"));         
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@101",
                             "nid=1,friendid=101,timestamp=1;nid=4,friendid=101,timestamp=6;")); 
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@200",
                             "nid=4,friendid=200,timestamp=5;nid=0,friendid=200,timestamp=0;")); 
    
}

TEST_F(MultiRegionTest, testKKVWithDifferentFieldSchema) {
    Processor processor;
    proto::PartitionId partitionId;
    partitionId.Clear();
    partitionId.mutable_buildid()->set_datatable("kkv_table_two_region");
    *partitionId.add_clusternames() = "simple_cluster1";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
    string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
    ResourceReaderPtr resourceReader(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReader, partitionId, NULL, counterMap));


    vector<IE_NAMESPACE(document)::DocumentPtr> docs = createDocuments(processor,
            "nid:0;friendid:100;timestamp:0;region_name:region1;#"
            "nid:0;friendid:200;timestamp:0;region_name:region1;#"
            "nid:1;friendid:101;timestamp:1;region_name:region1;#"
            "nid:2;friendid:102;timestamp:2;region_name:region1;#"
            "nid:superman;friendid:103;timestamp:3;region_name:region2;#"
            "nid:superman;friendid:104;timestamp:4;region_name:region2;#"
            "nid:ironman;friendid:200;timestamp:5;region_name:region2;#"
            "nid:ironman;friendid:101;timestamp:6;region_name:region2;"); 
    string fullDocBinStr = PartitionStateMachine::SerializeDocuments(docs);
    string schemaFileName = ResourceReader::getSchemaConfRelativePath("two_region_fields");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                resourceReader->getConfigPath(), schemaFileName);

    ASSERT_TRUE(schema);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(true).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;
    options.SetEnablePackageFile(false); 
    
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    psm.SetPsmOption("documentFormat", "binary");

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocBinStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@0",
                             "nid=0,friendid=100,timestamp=0;nid=0,friendid=200,timestamp=0;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@1",
                             "nid=1,friendid=101,timestamp=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@2",
                             "nid=2,friendid=102,timestamp=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@superman",
                             "nid=superman,friendid=103,timestamp=3;"
                             "nid=superman,friendid=104,timestamp=4;"));         
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@ironman",
                             "nid=ironman,friendid=200,timestamp=5;"
                             "nid=ironman,friendid=101,timestamp=6;")); 

}

TEST_F(MultiRegionTest, testKVSimpleBuild) {
    Processor processor;
    proto::PartitionId partitionId;
    partitionId.Clear();
    partitionId.mutable_buildid()->set_datatable("kv_table_all_region_dispatch");
    *partitionId.add_clusternames() = "kv_table";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
    string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
    ResourceReaderPtr resourceReader(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReader, partitionId, NULL, counterMap));

    // region1: nid -> nid, friendid, pidvid
    // region2: friendid -> nid, pidvid
    vector<IE_NAMESPACE(document)::DocumentPtr> docs = createDocuments(processor,
            "nid:0;friendid:100;pidvid:0;#"
            "nid:1;friendid:101;pidvid:1;#nid:2;friendid:102;pidvid:2;#"
            "nid:1;friendid:103;pidvid:3;#nid:4;friendid:101;pidvid:6;#"); 
    string fullDocBinStr = PartitionStateMachine::SerializeDocuments(docs);
    string schemaFileName = ResourceReader::getSchemaConfRelativePath("kv_table");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                resourceReader->getConfigPath(), schemaFileName);

    ASSERT_TRUE(schema);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(true).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetOfflineConfig().fullIndexStoreKKVTs = true;
    options.SetEnablePackageFile(false); 
    
    PartitionStateMachine psm;
    string rootPath = GET_TEST_DATA_PATH();
    ASSERT_TRUE(psm.Init(schema, options, rootPath));
    psm.SetPsmOption("documentFormat", "binary");

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocBinStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@0",
                             "nid=0,friendid=100,pidvid=0;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@1",
                             "nid=1,friendid=103,pidvid=3;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@2",
                             "nid=2,friendid=102,pidvid=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region1@4",
                             "nid=4,friendid=101,pidvid=6;")); 
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@101",
                             "nid=4,pidvid=6;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "region2@103",
                             "nid=1,pidvid=3;")); 
}

TEST_F(MultiRegionTest, testMultiClusterWithMultiRegion) {
    Processor processor;
    proto::PartitionId partitionId;
    partitionId.Clear();
    partitionId.mutable_buildid()->set_datatable("kv_table_multi_cluster");
    *partitionId.add_clusternames() = "kv_table";
    *partitionId.add_clusternames() = "kv_table2";    
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
    string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
    ResourceReaderPtr resourceReader(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReader, partitionId, NULL, counterMap));

    // cluster: kv_table, dispatch_to_all_region
    // region1: nid -> nid, friendid, pidvid
    // region2: friendid -> nid, pidvid
    // cluster: kv_table2, dispatch_by_field
    // region3: nid2 -> nid2, friendid2, pidvid
    // region4: friendid2 -> nid2, pidvid    
    vector<document::RawDocumentPtr> rawDocs = ProcessorTestUtil::createRawDocuments(
            "nid:0;friendid:100;pidvid:0;nid2:1;friendid2:101;ha_reserved_region_field:region3#"
            "nid:1;friendid:101;pidvid:1;nid2:2;friendid2:102;ha_reserved_region_field:region4#"
            "nid:2;friendid:102;pidvid:2;nid2:3;friendid2:103;ha_reserved_region_field:region3#"
            "nid:3;friendid:103;pidvid:3;nid2:4;friendid2:104;ha_reserved_region_field:region4#", "#");

    auto checkProcessedDocVec = [] (const ProcessedDocumentVec& pDocVec, size_t i)
    {
        ASSERT_EQ(3u, pDocVec.size());
        for (size_t j = 0 ; j < pDocVec.size(); ++j) {
            const auto& clusterMetaVec = pDocVec[j]->getDocClusterMetaVec();
            ASSERT_EQ(1u, clusterMetaVec.size());
            const auto& clusterName = clusterMetaVec[0].clusterName;
            IE_NAMESPACE(document)::NormalDocumentPtr indexDoc = DYNAMIC_POINTER_CAST(
                    IE_NAMESPACE(document)::NormalDocument, pDocVec[j]->getDocument());
            ASSERT_TRUE(indexDoc);
            
            regionid_t regionId = indexDoc->GetRegionId();
            const auto& kvDoc = indexDoc->GetKVIndexDocument();

            if (j < 2) {
                ASSERT_EQ(string("kv_table"), clusterName);
                ASSERT_EQ(j, (size_t)regionId);

                if (j == 0) {
                    ASSERT_EQ(i, kvDoc->GetPkeyHash());
                } else {
                    ASSERT_EQ(100u + i, kvDoc->GetPkeyHash());
                }
                    
            } else
            {
                ASSERT_EQ(string("kv_table2"), clusterName);
                ASSERT_EQ(i % 2, (size_t)regionId);
                if (regionId == 0) {
                    ASSERT_EQ(i + 1, kvDoc->GetPkeyHash());
                } else {
                    ASSERT_EQ(i + 101, kvDoc->GetPkeyHash());
                }
            }
        }
    };
    
    for (size_t i = 0; i < rawDocs.size(); ++i) {
        processor.processDoc(rawDocs[i]);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        checkProcessedDocVec(*processedDocVec, i);
    }
}


}
}
