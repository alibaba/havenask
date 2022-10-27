#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/IndexOptionsConfig.h>
#include <string>

using namespace suez::turing;
using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(config);

class IndexOptionsConfigTest : public TESTBASE {
public:
    IndexOptionsConfigTest();
    ~IndexOptionsConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkIndexOptionsConfig(const IndexOptionsConfig &config);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, IndexOptionsConfigTest);


IndexOptionsConfigTest::IndexOptionsConfigTest() { 
}

IndexOptionsConfigTest::~IndexOptionsConfigTest() { 
}

void IndexOptionsConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void IndexOptionsConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(IndexOptionsConfigTest, testSerilizeAndDeserilize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string jsonString = " { \
       \"check_point_doc_num\" : 512000, \
       \"doc_num_per_dump\" : 256, \
       \"pool_size_per_dump\" : 1024, \
       \"keep_version_count\" : 2 \
}";
    
    IndexOptionsConfig config;
    FromJsonString(config, jsonString);
    checkIndexOptionsConfig(config);

    string tmpString = ToJsonString(config);
    IndexOptionsConfig config2;
    FromJsonString(config2, tmpString);
    checkIndexOptionsConfig(config2);
}

void IndexOptionsConfigTest::checkIndexOptionsConfig(const IndexOptionsConfig &config) {
    // ASSERT_EQ(0.19, config._indexCacheRatio);
    // ASSERT_EQ(0.91, config._summaryCacheRatio);
    // ASSERT_EQ((uint32_t)5, config._panguMaxCopy);
    // ASSERT_EQ((uint32_t)3, config._panguMinCopy);
    ASSERT_EQ((uint32_t)512000, config._checkPointDocNum);
    ASSERT_EQ((uint32_t)256, config._docNumPerDump);
    ASSERT_EQ((uint32_t)1024, config._poolSizePerDump);
    ASSERT_EQ((uint32_t)2, config._keepVersionCount);
    // ASSERT_EQ(string("DumpByDocCount"), config._dumpCondition);
}

TEST_F(IndexOptionsConfigTest, testSerilizeAndDeserilizeWithEmptyString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string jsonString = "{}";
    
    IndexOptionsConfig config;
    FromJsonString(config, jsonString);
    // ASSERT_EQ(0.0, config._indexCacheRatio);
    // ASSERT_EQ(0.0, config._summaryCacheRatio);
    // ASSERT_EQ((uint32_t)3, config._panguMaxCopy);
    // ASSERT_EQ((uint32_t)1, config._panguMinCopy);
    ASSERT_EQ((uint32_t)(128 * 1024), config._checkPointDocNum);
    ASSERT_EQ(uint32_t(IndexOptionsConfig::INVALID_DOC_NUM_PER_DUMP_VALUE),
                         config._docNumPerDump);
    ASSERT_EQ(uint32_t(IndexOptionsConfig::INVALID_POOL_SIZE_PER_DUMP_VALUE),
                         config._poolSizePerDump);
    ASSERT_EQ(uint32_t(2),
                         config._keepVersionCount);
    // ASSERT_EQ(string(), config._dumpCondition);
}

TEST_F(IndexOptionsConfigTest, testConvertToIndexPartitionOptions) { 
    HA3_LOG(DEBUG, "Begin Test!");

    IndexOptionsConfig indexOptionsConfig;
    // indexOptionsConfig._indexCacheRatio = 0.18;
    // indexOptionsConfig._summaryCacheRatio = 0.81;
    // indexOptionsConfig._panguMaxCopy = 10;
    // indexOptionsConfig._panguMinCopy = 2;
    indexOptionsConfig._docNumPerDump = 1234;
    indexOptionsConfig._poolSizePerDump = 567;
    indexOptionsConfig._keepVersionCount = 10;

    // vector<string> indexNameVec;
    // indexNameVec.push_back("phrase");
    // indexNameVec.push_back("id");
    // indexNameVec.push_back("userid");

    // IndexPartitionOptions options 
    //     = indexOptionsConfig.convertToIndexPartitionOptions("serviceName", "partName", indexNameVec);

    IndexPartitionOptions options 
        = indexOptionsConfig.convertToIndexPartitionOptions();
    ASSERT_EQ(uint32_t(1234), options.dumpCondition.maxDocCnt);
    ASSERT_EQ(uint32_t(567), options.dumpCondition.maxPoolSize);
    ASSERT_EQ(uint32_t(10), options.keepVersionCount);
    
    // PanguFileSystemOptionsPtr fileSystemOptionsPtr = 
    //     tr1::dynamic_pointer_cast<PanguFileSystemOptions>(options.fileSystemOptions);
    // ASSERT_TRUE(fileSystemOptionsPtr);
    
    // ASSERT_EQ(10, fileSystemOptionsPtr->maxCopy);
    // ASSERT_EQ(2, fileSystemOptionsPtr->minCopy);
    // ASSERT_EQ(string("MachineLevel_serviceName"), fileSystemOptionsPtr->appName);
    // ASSERT_EQ(string("partName"), fileSystemOptionsPtr->partName);
    // ASSERT_EQ(uint64_t(1234), options.docNumPerDump);
    // ASSERT_EQ(IndexPartitionOptions::DUMP_BY_DOC_COUNT, options.dumpCondition);
    // ASSERT_EQ((uint64_t)1000, options.segmentPoolSize);

    // CacheCenterOptionsPtr 
    //     cacheCenterOptionsPtr = options.cacheCenterOptions;

    // CacheOptionItem item;

    // ASSERT_TRUE(cacheCenterOptionsPtr->GetCacheOptionItem("phrase", item));
    // ASSERT_EQ(RATIO_MODE, item.mode);
    // ASSERT_EQ(0.18, item.ratio);
    // ASSERT_EQ((uint64_t)-1, item.maxSizeInBytes);

    // ASSERT_TRUE(cacheCenterOptionsPtr->GetCacheOptionItem("userid", item));
    // ASSERT_EQ(RATIO_MODE, item.mode);
    // ASSERT_EQ(0.18, item.ratio);
    // ASSERT_EQ((uint64_t)-1, item.maxSizeInBytes);

    // ASSERT_TRUE(cacheCenterOptionsPtr->GetCacheOptionItem("id", item));
    // ASSERT_EQ(RATIO_MODE, item.mode);
    // ASSERT_EQ(0.18, item.ratio);
    // ASSERT_EQ((uint64_t)-1, item.maxSizeInBytes);

    // ASSERT_TRUE(cacheCenterOptionsPtr->GetCacheOptionItem("summary", item));
    // ASSERT_EQ(RATIO_MODE, item.mode);
    // ASSERT_EQ(0.81, item.ratio);
    // ASSERT_EQ((uint64_t)-1, item.maxSizeInBytes);
}

// void IndexOptionsConfigTest::testConvertToIndexPartitionOptionsWithDefaultValue() { 
//     HA3_LOG(DEBUG, "Begin Test!");

//     IndexOptionsConfig indexOptionsConfig;

//     // vector<string> indexNameVec;
//     // indexNameVec.push_back("phrase");

//     // IndexPartitionOptions options 
//     //     = indexOptionsConfig.convertToIndexPartitionOptions("serviceName", "partName", indexNameVec);

//     IndexPartitionOptions options 
//         = indexOptionsConfig.convertToIndexPartitionOptions();

//     // LocalFileSystemOptionsPtr fileSystemOptionsPtr = 
//     //     tr1::dynamic_pointer_cast<LocalFileSystemOptions>(options.fileSystemOptions);
//     // ASSERT_TRUE(fileSystemOptionsPtr);
    
//     ASSERT_EQ(uint64_t(IndexPartitionOptions::DEFAULT_MAX_DOC_NUM_PER_DUMP), options.docNumPerDump);
//     // ASSERT_EQ(IndexPartitionOptions::DUMP_BY_MEMORY, options.dumpCondition);

//     CacheCenterOptionsPtr 
//         cacheCenterOptionsPtr = options.cacheCenterOptions;

//     CacheOptionItem item;

//     ASSERT_TRUE(!cacheCenterOptionsPtr->GetCacheOptionItem("phrase", item));
//     ASSERT_TRUE(!cacheCenterOptionsPtr->GetCacheOptionItem("summary", item));
// }

// void IndexOptionsConfigTest::testConvertToIndexPartitionOptionsForMerge()
// {
//     IndexOptionsConfig indexOptionsConfig;
//     indexOptionsConfig._mergeStrategy = "optimize_for_incremental";
//     indexOptionsConfig._dataCleanPolicy = "by-version";
//     indexOptionsConfig._dataCleanPolicyParam = "keep_count=2";

//     vector<string> indexNameVec;
//     indexNameVec.push_back("phrase");

//     IndexPartitionOptions options
//         = indexOptionsConfig.convertToIndexPartitionOptions("serviceName", "partName", indexNameVec, builder::BUILD_MODE_FULL);

//     LocalFileSystemOptionsPtr fileSystemOptionsPtr = 
//         tr1::dynamic_pointer_cast<LocalFileSystemOptions>(options.fileSystemOptions);
//     ASSERT_TRUE(fileSystemOptionsPtr);
    
//     ASSERT_EQ(string("optimize"), options.mergeStrategy->GetIdentifier());
//     ASSERT_EQ(string("by-version"), options.dataCleanPolicy->GetIdentifier());
//     ASSERT_EQ(string("keep_count = 1"), options.dataCleanPolicy->GetParameter());

//     options = indexOptionsConfig.convertToIndexPartitionOptions("serviceName", "partName", indexNameVec, builder::BUILD_MODE_INCREMENTAL);

//     ASSERT_EQ(string("optimize_for_incremental"), options.mergeStrategy->GetIdentifier());
//     ASSERT_EQ(string("by-version"), options.dataCleanPolicy->GetIdentifier());
//     ASSERT_EQ(string("keep_count = 2"), options.dataCleanPolicy->GetParameter());
// }

END_HA3_NAMESPACE(config);

