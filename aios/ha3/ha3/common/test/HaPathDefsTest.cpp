#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/HaPathDefs.h>
#include <fslib/fslib.h>
#include <ha3/test/test.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace fslib;
using namespace autil;
USE_HA3_NAMESPACE(proto);
BEGIN_HA3_NAMESPACE(common);

class HaPathDefsTest : public TESTBASE {
public:
    HaPathDefsTest();
    ~HaPathDefsTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, HaPathDefsTest);


HaPathDefsTest::HaPathDefsTest() { 
}

HaPathDefsTest::~HaPathDefsTest() { 
}

void HaPathDefsTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void HaPathDefsTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(HaPathDefsTest, testConfigPathToVersion) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    std::string configPath = "/home/config/123/";
    int32_t version = 0;
    ASSERT_TRUE(HaPathDefs::configPathToVersion(configPath, version));
    ASSERT_EQ(123, version);

    configPath = "/home/config/0";
    version = 1;
    ASSERT_TRUE(HaPathDefs::configPathToVersion(configPath, version));
    ASSERT_EQ(0, version);

    configPath = "/home/config/-1";
    version = 1;
    ASSERT_TRUE(HaPathDefs::configPathToVersion(configPath, version));
    ASSERT_EQ(-1, version);
    
}

TEST_F(HaPathDefsTest, testGetConfigRootPath) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string configPath = "/home/config/123/";
    string rootPath;
    ASSERT_TRUE(HaPathDefs::getConfigRootPath(configPath, rootPath));
    ASSERT_EQ(string("/home/config/"), rootPath);
    
    configPath = "hdfs://a/b/c/1//";
    ASSERT_TRUE(HaPathDefs::getConfigRootPath(configPath, rootPath));
    ASSERT_EQ(string("hdfs://a/b/c/"), rootPath);
    
    configPath = "/2";
    ASSERT_TRUE(HaPathDefs::getConfigRootPath(configPath, rootPath));
    ASSERT_EQ(string("/"), rootPath);
    
    configPath = "2";
    ASSERT_TRUE(!HaPathDefs::getConfigRootPath(configPath, rootPath));
}

TEST_F(HaPathDefsTest, testConfigPathToVersionWithWrongFormat) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    int32_t version = 0;
    std::string configPath = "/home/config/-0123/";

    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version));

    configPath = "/home/config/00";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version));
    
    configPath = "/home/config/01";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version));
    
    configPath = "/home/config/0.1";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version));

    configPath = "/home/config/-0";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version)); 

    configPath = "/home/config/a";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version)); 
    
    configPath = "/home/config/0x1";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version)); 

    configPath = "/home/config/11lu";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version)); 

    configPath = "/home/config/0xabc";
    ASSERT_TRUE(!HaPathDefs::configPathToVersion(configPath, version)); 
}

TEST_F(HaPathDefsTest, testPartitionIdToIndexPath)
{
    HA3_LOG(DEBUG, "Begin Test!");

    PartitionID partitionId;
    partitionId.set_fullversion(0);
    partitionId.set_clustername("daogou");
    Range range;
    range.set_from(1);
    range.set_to(2);
    *(partitionId.mutable_range()) = range;
    std::string cwdPath = "/cwd";
    std::string indexPath;
    ASSERT_TRUE(HaPathDefs::partitionIdToIndexPath(partitionId, cwdPath, indexPath));
    ASSERT_EQ(std::string("/cwd/runtimedata/daogou/generation_0/partition_1_2/"), indexPath);
}

TEST_F(HaPathDefsTest, testPartitionIdToIndexPathWithWrongPartitionID)
{
    HA3_LOG(DEBUG, "Begin Test!");

    PartitionID partitionId;

    std::string cwdPath = "/cwd";
    std::string indexPath;

    ASSERT_TRUE(!HaPathDefs::partitionIdToIndexPath(partitionId, cwdPath, indexPath));
    partitionId.set_fullversion(0);

    ASSERT_TRUE(!HaPathDefs::partitionIdToIndexPath(partitionId, cwdPath, indexPath));
    partitionId.set_clustername("daogou");

    ASSERT_TRUE(!HaPathDefs::partitionIdToIndexPath(partitionId, cwdPath, indexPath));
    Range range;
    range.set_from(1);
    range.set_to(2);
    *(partitionId.mutable_range()) = range;
    ASSERT_TRUE(HaPathDefs::partitionIdToIndexPath(partitionId, cwdPath, indexPath));
    ASSERT_EQ(std::string("/cwd/runtimedata/daogou/generation_0/partition_1_2/"), indexPath);
}


TEST_F(HaPathDefsTest, testIndexPathToPartitionID) {
    HA3_LOG(DEBUG, "Begin Test!");
    string baseUri = "/cwd/runtimedata/test_cluster/generation_0/partition_1_2/";
    PartitionID pid;
    bool ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(ret);
    ASSERT_EQ(string("ROLE_SEARCHER_test_cluster_0_0_1_2"), 
                                ProtoClassUtil::partitionIdToString(pid));

    baseUri = "/cwd/runtimedata/test_cluster/generation_7/partition_8_9";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(ret);
    ASSERT_EQ(string("ROLE_SEARCHER_test_cluster_0_7_8_9"), 
                                ProtoClassUtil::partitionIdToString(pid));

    baseUri = "/generation_7/partition_8_9";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(!ret);

    baseUri = "/cwd/runtimedata/test_cluster/generation_0/partition_1";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(!ret);

    baseUri = "/cwd/runtimedata/test_cluster/generation/partition_1_2";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(!ret);

    baseUri = "cwd/runtimedata/test_cluster/generation_0/partition_1_2";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(ret);
    ASSERT_EQ(string("ROLE_SEARCHER_test_cluster_0_0_1_2"), 
                                ProtoClassUtil::partitionIdToString(pid));

    baseUri = "/cwd/runtimedata/test_cluster/generation_0/part_1_2";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(!ret);

    baseUri = "/cwd/runtimedata/test_cluster/gen_0/partition_1_2";
    ret = HaPathDefs::indexSourceToPartitionID(baseUri, ROLE_SEARCHER, pid);
    ASSERT_TRUE(!ret);
}

TEST_F(HaPathDefsTest, testMakeAndParseWorkerAddress) 
{
    HA3_LOG(DEBUG, "Begin Test!");
    
    std::string ip = "127.0.0.1";
    uint16_t port = 0;
    std::string ip2="aaaa";
    uint16_t port2=123;
    
    ASSERT_TRUE(HaPathDefs::parseWorkerAddress(HaPathDefs::makeWorkerAddress(ip, port), ip2, port2));

    ASSERT_EQ(ip, ip2);
    ASSERT_EQ(port, port2);
    
    ASSERT_TRUE(!HaPathDefs::parseWorkerAddress("127.0.0.1:a", ip2, port2));
}

TEST_F(HaPathDefsTest, testDirNameToIncrementalVersion)
{
    uint32_t version = 0;
    std::string name = "version.1"; 
    ASSERT_TRUE(HaPathDefs::dirNameToIncrementalVersion(name, version));
    ASSERT_EQ((uint32_t)1, version);
    std::vector<std::string> names;
    names.push_back("");
    names.push_back("xxx");
    names.push_back("xxx.2");
    names.push_back("version. 2");
    names.push_back("version.2x");
    names.push_back("version.x");
    names.push_back("version.1.1");
    names.push_back("version.-3");
    names.push_back("version.05");
    for (std::vector<std::string>::iterator it = names.begin();
         it != names.end(); ++it) 
    {
        ASSERT_TRUE(!HaPathDefs::dirNameToIncrementalVersion(*it, version));
    }    
}

TEST_F(HaPathDefsTest, testFileNameToVersion)
{
    string name = "abc";
    uint32_t version = 0;
    bool ret = HaPathDefs::fileNameToVersion(name, version);
    ASSERT_TRUE(!ret);

    name = "version.03";
    ret = HaPathDefs::fileNameToVersion(name, version);
    ASSERT_TRUE(!ret);

    name = "version.3";
    ret = HaPathDefs::fileNameToVersion(name, version);
    ASSERT_TRUE(ret);
    ASSERT_EQ((uint32_t)3, version);
}

TEST_F(HaPathDefsTest, testSortGenerationList) {
    FileList fileList;
    vector<uint32_t> generationIdVec;
    HaPathDefs::sortGenerationList(fileList, generationIdVec);
    ASSERT_EQ(size_t(0), fileList.size());
    ASSERT_EQ(size_t(0), generationIdVec.size());
    
    fileList.push_back("generation_0");
    fileList.push_back("_generation_3");
    fileList.push_back("generation_2");
    fileList.push_back("generation_a");
    fileList.push_back("generation_1");
    fileList.push_back("generation_04");
    
    HaPathDefs::sortGenerationList(fileList, generationIdVec);
    ASSERT_EQ(size_t(3), fileList.size());
    ASSERT_EQ(size_t(3), generationIdVec.size());
    
    for (size_t i = 0; i < fileList.size(); ++i) {
        ASSERT_EQ(uint32_t(2 - i), generationIdVec[i]);
        ASSERT_EQ(string("generation_") + StringUtil::toString<uint32_t>(2 - i),
                             fileList[i]);
    }
}

END_HA3_NAMESPACE(common);
