#include "build_service/test/unittest.h"
#include "build_service/processor/HashDocumentProcessor.h"
#include "build_service/config/ResourceReader.h"
#include <autil/StringTokenizer.h>
#include <autil/HashFuncFactory.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;

using namespace build_service::config;
using namespace build_service::document;
using namespace heavenask::indexlib;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
namespace build_service {
namespace processor {

class HashDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    ExtendDocumentPtr createExtendDoc(
            const string &fieldMap, regionid_t regionId = DEFAULT_REGIONID);
};

void HashDocumentProcessorTest::setUp() {
}

void HashDocumentProcessorTest::tearDown() {
}

TEST_F(HashDocumentProcessorTest, testSimpleProcess) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("mainse_searcher");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    
    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.clusterNames = StringTokenizer::tokenize(ConstString("mainse_searcher,mainse_summary"), ",");
    param.schemaPtr = schema;
    ASSERT_TRUE(processor.init(param));

    ExtendDocumentPtr extendDoc = createExtendDoc("id:1");
    ASSERT_TRUE(processor.process(extendDoc));

    const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
    ASSERT_TRUE(processedDoc);
    const ProcessedDocument::DocClusterMetaVec &metaVec =
        processedDoc->getDocClusterMetaVec();
    ASSERT_EQ(size_t(2), metaVec.size());

    EXPECT_EQ(string("mainse_searcher"), metaVec[0].clusterName);
    EXPECT_EQ(HashFuncFactory::createHashFunc("HASH")->getHashId("1"), metaVec[0].hashId);
    EXPECT_EQ(string("mainse_summary"), metaVec[1].clusterName);
    EXPECT_EQ(HashFuncFactory::createHashFunc("KINGSO_HASH#720")->getHashId("1"), metaVec[1].hashId);
}

TEST_F(HashDocumentProcessorTest, testMultiFieldHash) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("multi_field_hash");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    
    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.clusterNames = StringTokenizer::tokenize(ConstString("multi_field_hash"), ",");
    param.schemaPtr = schema;
    ASSERT_TRUE(processor.init(param));
    {
        ExtendDocumentPtr extendDoc = createExtendDoc("id:1");
        ASSERT_TRUE(processor.process(extendDoc));
        ASSERT_TRUE(extendDoc->testWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY));
        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(1), metaVec.size());

        EXPECT_EQ(string("multi_field_hash"), metaVec[0].clusterName);
        map<string, string> params = {{"routing_ratio","0.2"}};
        auto funcBase = HashFuncFactory::createHashFunc("ROUTING_HASH", params);
        ASSERT_TRUE(funcBase != nullptr);
        vector<string> hashVec = {"1", ""};
        EXPECT_EQ(funcBase->getHashId(hashVec), metaVec[0].hashId);
    }
    {
        ExtendDocumentPtr extendDoc = createExtendDoc("id:1;sid:123");
        ASSERT_TRUE(processor.process(extendDoc));
        ASSERT_FALSE(extendDoc->testWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY));

        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(1), metaVec.size());

        EXPECT_EQ(string("multi_field_hash"), metaVec[0].clusterName);
        map<string, string> params = {{"routing_ratio","0.2"}};
        auto funcBase = HashFuncFactory::createHashFunc("ROUTING_HASH", params);
        ASSERT_TRUE(funcBase != nullptr);
        vector<string> hashVec = {"1", "123"};
        EXPECT_EQ(funcBase->getHashId(hashVec), metaVec[0].hashId);
    }
}

TEST_F(HashDocumentProcessorTest, testMultiFieldHashWithDynamicField) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("multi_field_hash");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    
    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.clusterNames = StringTokenizer::tokenize(ConstString("dynamic_field_hash"), ",");
    param.schemaPtr = schema;
    ASSERT_TRUE(processor.init(param));
    {
        ExtendDocumentPtr extendDoc = createExtendDoc("id:1;sid:123");
        ASSERT_TRUE(processor.process(extendDoc));
        ASSERT_FALSE(extendDoc->testWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY));
        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(1), metaVec.size());

        EXPECT_EQ(string("dynamic_field_hash"), metaVec[0].clusterName);
        map<string, string> params = {{"routing_ratio","0.2"}};
        auto funcBase = HashFuncFactory::createHashFunc("ROUTING_HASH", params);
        ASSERT_TRUE(funcBase != nullptr);
        vector<string> hashVec = {"1", "123"};
        EXPECT_EQ(funcBase->getHashId(hashVec), metaVec[0].hashId);
    }
    {
        ExtendDocumentPtr extendDoc = createExtendDoc("id:1;sid:123;ratio:0.1");
        ASSERT_TRUE(processor.process(extendDoc));

        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(1), metaVec.size());

        EXPECT_EQ(string("dynamic_field_hash"), metaVec[0].clusterName);
        map<string, string> params = {{"routing_ratio","0.1"}};
        auto funcBase = HashFuncFactory::createHashFunc("ROUTING_HASH", params);
        ASSERT_TRUE(funcBase != nullptr);
        vector<string> hashVec = {"1", "123"};
        EXPECT_EQ(funcBase->getHashId(hashVec), metaVec[0].hashId);
    }

}

TEST_F(HashDocumentProcessorTest, testNotExistCluster) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("mainse_searcher");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);

    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.schemaPtr = schema;
    param.clusterNames = StringTokenizer::tokenize(ConstString("mainse_searcher,not_exist_cluster"), ",");
    ASSERT_TRUE(!processor.init(param));
}

TEST_F(HashDocumentProcessorTest, testInvalidHashModeConfig) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("mainse_searcher");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    
    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.schemaPtr = schema;
    param.clusterNames = StringTokenizer::tokenize(ConstString("invalid_hash_mode"), ",");
    ASSERT_TRUE(!processor.init(param));
}

TEST_F(HashDocumentProcessorTest, testHashFieldEmpty) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("mainse_searcher");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);

    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.schemaPtr = schema;
    param.clusterNames = StringTokenizer::tokenize(ConstString("empty_hash_field"), ",");
    ASSERT_TRUE(!processor.init(param));
}

TEST_F(HashDocumentProcessorTest, testInvalidHashFunction) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("mainse_searcher");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    
    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.schemaPtr = schema;
    param.clusterNames = StringTokenizer::tokenize(ConstString("invalid_hash_function"), ",");
    ASSERT_TRUE(!processor.init(param));
}

TEST_F(HashDocumentProcessorTest, testProcessType) {
    HashDocumentProcessorPtr processor(new HashDocumentProcessor);
    DocumentProcessorPtr clonedProcessor(processor->clone());
    EXPECT_TRUE(clonedProcessor->needProcess(ADD_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_SUB_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(UPDATE_FIELD));
    EXPECT_TRUE(!clonedProcessor->needProcess(SKIP_DOC));
}

TEST_F(HashDocumentProcessorTest, testInitMultiRegionHashMode) {
    HashDocumentProcessor processor;
    auto checkInit = [this, &processor] (const string &clusterName,
            const string &schemaName, bool isSuccess) {
        vector<string> clusterNames = {clusterName};
        string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
        string schemaFileName = ResourceReader::getSchemaConfRelativePath(schemaName);
        IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                configPath, schemaFileName);
        ResourceReaderPtr resourceReader(new ResourceReader(configPath));

        DocProcessorInitParam param;
        param.resourceReader = resourceReader.get();
        param.clusterNames = clusterNames;
        param.schemaPtr = schema;
        ASSERT_EQ(isSuccess, processor.init(param));
    };
    // one region, no hash_mode, no region_hash_mode
    checkInit("empty_hash_field", "single_region", false);
    // one region, one hash_mode, no region_hash_mode
    checkInit("mainse_summary", "single_region", true);
    // regionName not exist in schema file
    checkInit("multi_region", "single_region", false);

    // one region, no hash_mode, one region_hash_mode
    checkInit("single_region", "single_region", true);
    // one region, no hash_mode,  invalid hash_field for region_hash_mode
    checkInit("invalid_region_hash_mode", "single_region", false);
    // one region, invalid hash_function for hash_mode, no region_hash_mode
    checkInit("invalid_hash_function", "single_region", false);
    // one region, one hash_mode, invalid hash_function for region_hash_mode
    checkInit("multi_region_invalid_hash_function", "single_region", false);
    // multi region, one hash_mode, invalid region_name for region_hash_mode
    checkInit("invalid_region_name", "multi_region", false);
    // multi region, one hash_mode, empty region_name for region_hash_mode
    checkInit("empty_region_name", "multi_region", false);
}

TEST_F(HashDocumentProcessorTest, testMultiRegionHashMode) {
    HashDocumentProcessor processor;

    vector<string> clusterNames = {"multi_region", "mainse_searcher"};
    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    string schemaFileName = ResourceReader::getSchemaConfRelativePath(clusterNames[0]);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.clusterNames = clusterNames;
    param.schemaPtr = schema;
    ASSERT_TRUE(processor.init(param));

    auto checkHash = [this, &processor, &clusterNames] (const string &docStr,
            regionid_t regionId, vector<string> hashFunc, vector<string> hashId) {
        ExtendDocumentPtr extendDoc = createExtendDoc(docStr, regionId);
        ASSERT_TRUE(processor.process(extendDoc));

        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(clusterNames.size()), metaVec.size());
        for (size_t i = 0; i < clusterNames.size(); i++) {
            EXPECT_EQ(clusterNames[i], metaVec[i].clusterName);
            EXPECT_EQ(HashFuncFactory::createHashFunc(hashFunc[i])->getHashId(hashId[i]), metaVec[i].hashId);
        }
    };

    checkHash("region_name:region1;nid:1;timestamp:1234;id:67", 0, {"HASH64", "HASH"}, {"1", "67"});
    checkHash("region_name:region2;nid:2;timestamp:124;id:68", 1, {"NUMBER_HASH", "HASH"}, {"124", "68"});
    checkHash("region_name:region3;nid:2;id:555", 2, {"GALAXY_HASH", "HASH"}, {"555", "555"});
    checkHash("region_name:region4;nid:2;id:556", 3, {"KINGSO_HASH#110", "HASH"}, {"556", "556"});
    checkHash("region_name:region5;nid:2;id:557", 4, {"HASH", "HASH"}, {"557", "557"});
    checkHash("region_name:region6;nid:2;id:558", 5, {"HASH", "HASH"}, {"558", "558"});
    checkHash("region_name:region6;nid:2;id:558", 5, {"HASH", "ROUTING_HASH"}, {"558", "558"});
    checkHash("region_name:region7;nid:2;id:559", 6, {"ROUTING_HASH", "ROUTING_HASH"}, {"559", "559"});
}

TEST_F(HashDocumentProcessorTest, testHashFieldMissingInDoc) {
    HashDocumentProcessor processor;

    string configPath = TEST_DATA_PATH"hash_document_processor_test/config/";
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));

    string schemaFileName = ResourceReader::getSchemaConfRelativePath("mainse_excellent");
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            configPath, schemaFileName);
    
    DocProcessorInitParam param;
    param.resourceReader = resourceReader.get();
    param.clusterNames = StringTokenizer::tokenize(ConstString("mainse_excellent"), ",");
    param.schemaPtr = schema;
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    param.counterMap = counterMap;
    ASSERT_TRUE(processor.init(param));
    const std::string counterName = BS_COUNTER(processor.hashFieldEmptyError);
    auto emptyHashFieldCounter = counterMap->GetAccCounter(counterName);
    ASSERT_TRUE(emptyHashFieldCounter);
    {
        ExtendDocumentPtr extendDoc = createExtendDoc("id:1;uid:1");
        ASSERT_TRUE(processor.process(extendDoc));
        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(1), metaVec.size());

        EXPECT_EQ(string("mainse_excellent"), metaVec[0].clusterName);
        EXPECT_EQ(HashFuncFactory::createHashFunc("HASH")->getHashId("1"), metaVec[0].hashId);
        EXPECT_EQ(int64_t(0), emptyHashFieldCounter->Get());
        EXPECT_FALSE(extendDoc->testWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY));
        
    }
    {
        ExtendDocumentPtr extendDoc = createExtendDoc("uid:1");
        ASSERT_TRUE(processor.process(extendDoc));
        const ProcessedDocumentPtr &processedDoc = extendDoc->getProcessedDocument();
        ASSERT_TRUE(processedDoc);
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        ASSERT_EQ(size_t(1), metaVec.size());

        EXPECT_EQ(string("mainse_excellent"), metaVec[0].clusterName);
        EXPECT_EQ(HashFuncFactory::createHashFunc("HASH")->getHashId(""), metaVec[0].hashId);
        EXPECT_EQ(int64_t(1), emptyHashFieldCounter->Get());
        EXPECT_TRUE(extendDoc->testWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY));        
    }
}    

ExtendDocumentPtr HashDocumentProcessorTest::createExtendDoc(
        const string &fieldMap, regionid_t regionId)
{
    ExtendDocumentPtr extendDoc(new ExtendDocument());
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    vector<string> fields = StringTokenizer::tokenize(ConstString(fieldMap), ";");
    for (size_t i = 0; i < fields.size(); ++i) {
        vector<string> kv = StringTokenizer::tokenize(ConstString(fields[i]), ":");
        rawDoc->setField(kv[0], kv[1]);
    }
    extendDoc->setRawDocument(rawDoc);
    extendDoc->setRegionId(regionId);
    return extendDoc;
}

    
}
}

