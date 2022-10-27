#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/PersistentMap.h>
#include <ha3/test/test.h>
#include <fslib/fslib.h>
BEGIN_HA3_NAMESPACE(util);

class PersistentMapTest : public TESTBASE {
public:
    PersistentMapTest();
    ~PersistentMapTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, PersistentMapTest);


using namespace std;

PersistentMapTest::PersistentMapTest() { 
}

PersistentMapTest::~PersistentMapTest() { 
}

void PersistentMapTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void PersistentMapTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(PersistentMapTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    std::string fslibURI = TEST_DATA_PATH"/PersistentMapTest.testSimpleProcess";
    std::string key = "key1";
    std::string value = "value1";
    std::string value1;
    bool ret = persistentMap.init(fslibURI);
    ASSERT_TRUE(ret);
    persistentMap.set(key, value);
    ret = persistentMap.upload();
    ASSERT_TRUE(ret);
    ret = persistentMap.download();
    ASSERT_TRUE(ret);
    ASSERT_EQ(size_t(1), persistentMap.size());
    ASSERT_TRUE(persistentMap.find(key, value1));
    ASSERT_EQ(value, value1);
    fslib::fs::FileSystem::remove(fslibURI);
}

TEST_F(PersistentMapTest, testEnCodeAndDecode)
{
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string in = ":ab|d\\d:a::def\\\\f|||ff|";
    string eout;
    string dout;
    persistentMap.encodeString(in, eout);
    persistentMap.decodeString(eout, dout);
    
    ASSERT_EQ(in, dout);
}

TEST_F(PersistentMapTest, testGetNextSeperatorPosWithEmptyString){
    PersistentMap persistentMap;
    string in;
    ASSERT_EQ(string::npos, persistentMap.getNextSeperatorPos(in, 0, '|'));
}
TEST_F(PersistentMapTest, testGetNextSeperatorPosWithEndPos){
    PersistentMap persistentMap;
    string in = "\\|a|";
    size_t expectPos = 3;
    size_t resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);

    in = "|a|";
    expectPos = 0;
    resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);

    expectPos = 2;
    resultPos = persistentMap.getNextSeperatorPos(in, 1, '|');
    ASSERT_EQ(expectPos, resultPos);

    in = "||a|";
    expectPos = 0;
    resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);
   
}
TEST_F(PersistentMapTest, testGetNextSeperatorPosWithNotFind){

    PersistentMap persistentMap;
    string in = "\\a";
    size_t expectPos = string::npos;
    size_t resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);
}
TEST_F(PersistentMapTest, testGetNextSeperatorPosWithOddSlashCount){
    PersistentMap persistentMap;
    string in = "b\\\\\\|a|";
    size_t expectPos = 6;
    size_t resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);

    in = "\\\\\\|a|";
    expectPos = 5;
    resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);

    in = "\\\\\\|a|b";
    expectPos = 5;
    resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);
}
TEST_F(PersistentMapTest, testGetNextSeperatorPosWithEvenSlashCount){
    PersistentMap persistentMap;
    string in = "b\\\\|a|";
    size_t expectPos = 3;
    size_t resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);

    in = "\\\\|";
    expectPos = 2;
    resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);

    in = "b\\\\|a";
    expectPos = 3;
    resultPos = persistentMap.getNextSeperatorPos(in, 0, '|');
    ASSERT_EQ(expectPos, resultPos);
}
TEST_F(PersistentMapTest, testGetNextSeperatorPos){
    PersistentMap persistentMap;
    string in = "b\\\\a|bbb:cccc";
    size_t expectPos = 8;
    size_t resultPos = persistentMap.getNextSeperatorPos(in, 0, ':');
    ASSERT_EQ(expectPos, resultPos);
}

TEST_F(PersistentMapTest, testExtractKVPair) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string content = "a:b";
    string key;
    string value;
    ASSERT_TRUE(persistentMap.extractKVPair(content, key, value));
    ASSERT_EQ(string("a"), key);
    ASSERT_EQ(string("b"), value);
}

TEST_F(PersistentMapTest, testExtractKVPaiWithMoreThanOneSeperator) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string content = "a:b:c";
    string key;
    string value;
    ASSERT_TRUE(!persistentMap.extractKVPair(content, key, value));
}

TEST_F(PersistentMapTest, testExtractKVPairWithoutKey) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string content = ":b";
    string key;
    string value;
    ASSERT_TRUE(persistentMap.extractKVPair(content, key, value));
    ASSERT_TRUE(key.empty());
    ASSERT_EQ(string("b"), value);
}
TEST_F(PersistentMapTest, testExtractKVPairWithoutValue) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string content = "a:";
    string key;
    string value;
    ASSERT_TRUE(persistentMap.extractKVPair(content, key, value));
    ASSERT_TRUE(value.empty());
    ASSERT_EQ(string("a"), key);
}
TEST_F(PersistentMapTest, testExtractKVPairWithoutKeyAndWithoutValue) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string content = ":";
    string key;
    string value;
    ASSERT_TRUE(persistentMap.extractKVPair(content, key, value));
    ASSERT_TRUE(key.empty());
    ASSERT_TRUE(value.empty());
}
TEST_F(PersistentMapTest, testExtractKVPairWithSlash) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string content = "a\\:b:c";
    string key;
    string value;
    ASSERT_TRUE(persistentMap.extractKVPair(content, key, value));
    ASSERT_EQ(string("a:b"), key);
    ASSERT_EQ(string("c"), value);
}

TEST_F(PersistentMapTest, testSerializeWithEmpty) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    string result = persistentMap.serialize();
    ASSERT_TRUE(result.empty());
}

TEST_F(PersistentMapTest, testSerialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    persistentMap[string("Sichuan")] = string("chengdu");
    persistentMap[string("Zhejiang")] = string("hangzhou");
    persistentMap[string("Jiangsu")] = string("nanjing");
    string result = persistentMap.serialize();
    ASSERT_TRUE(!result.empty());
    PersistentMap persistentMap2;
    ASSERT_TRUE(persistentMap2.deserialize(result));
    ASSERT_EQ(string("chengdu"), persistentMap2[string("Sichuan")]);
    ASSERT_EQ(string("hangzhou"),persistentMap2[string("Zhejiang")] );
    ASSERT_EQ(string("nanjing"),persistentMap2[string("Jiangsu")] );    
}

TEST_F(PersistentMapTest, testDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    string result = "a:b|c:d|e:f";
    PersistentMap persistentMap2;
    string value;
    ASSERT_TRUE(persistentMap2.deserialize(result));

    ASSERT_EQ( string("b"), persistentMap2[string("a")]);
    ASSERT_EQ( string("d"), persistentMap2[string("c")] );
    ASSERT_EQ( string("f"), persistentMap2[string("e")] );
}

TEST_F(PersistentMapTest, testDoubleDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    string result = "a:b|c:d|e:f";
    PersistentMap persistentMap2;
    string value;
    string result2 = "g:h|i:j|k:l";
    ASSERT_TRUE(persistentMap2.deserialize(result2));
    ASSERT_TRUE(persistentMap2.deserialize(result));

    ASSERT_EQ( size_t(3), persistentMap2.size());
    ASSERT_EQ( string("b"), persistentMap2[string("a")]);
    ASSERT_EQ( string("d"), persistentMap2[string("c")] );
    ASSERT_EQ( string("f"), persistentMap2[string("e")] );
}

TEST_F(PersistentMapTest, testBadDeserialize) {
    HA3_LOG(DEBUG, "Begin Test!");
    string result = "bad";
    PersistentMap persistentMap2;
    string value;
    ASSERT_TRUE(!persistentMap2.deserialize(result));
}

TEST_F(PersistentMapTest, testSerializeAndDeserializeWithSlash) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PersistentMap persistentMap;
    persistentMap[string("Sichuan")] = string("che:ng\\du");
    persistentMap[string("\\:")] = string("han\\gz|hou:");
    persistentMap[string("Jiangsu")] = string(":");
    persistentMap[string(":")] = string(":");
    persistentMap[string("|")] = string("|");
    persistentMap[string("\\")] = string("\\");
    persistentMap[string("\\\\")] = string("");
    persistentMap[string(":::")] = string("abc");
    string result = persistentMap.serialize();
    ASSERT_TRUE(!result.empty());
    PersistentMap persistentMap2;
    ASSERT_TRUE(persistentMap2.deserialize(result));
    size_t size = persistentMap2.size();
    ASSERT_EQ(size_t(8), size);
    ASSERT_EQ(string("che:ng\\du"), persistentMap2[string("Sichuan")]);
    ASSERT_EQ(string("han\\gz|hou:"),persistentMap2[string("\\:")] );
    ASSERT_EQ(string(":"),persistentMap2[string("Jiangsu")] );    
    ASSERT_EQ(string(":"),persistentMap2[string(":")] );    
    ASSERT_EQ(string("|"),persistentMap2[string("|")] );    
    ASSERT_EQ(string("\\"),persistentMap2[string("\\")] );    
    ASSERT_EQ(string(""),persistentMap2[string("\\\\")] );    
    ASSERT_EQ(string("abc"),persistentMap2[string(":::")] );    
}

TEST_F(PersistentMapTest, testUploadAndDownload) {
    HA3_LOG(DEBUG, "Begin Test");

    PersistentMap persistentMap;
    std::string fslibURI = TEST_DATA_PATH"/PersistentMapTest.testUploadAndDownload";
    ASSERT_TRUE(persistentMap.init(fslibURI));
    persistentMap[string("a")] = string("A");
    persistentMap[string(":")] = string("\\");
    ASSERT_TRUE(persistentMap.upload());
    PersistentMap persistentMap2;
    ASSERT_TRUE(persistentMap2.init(fslibURI));
    ASSERT_TRUE(persistentMap2.download());

    for (map<string, string>::const_iterator iter = persistentMap._map.begin(); 
         iter != persistentMap._map.end(); ++iter)
    {
        ASSERT_EQ(iter->second, persistentMap2[iter->first]);
    }
    
    ASSERT_TRUE(persistentMap.download());
    for (map<string, string>::const_iterator iter = persistentMap._map.begin(); 
         iter != persistentMap._map.end(); ++iter)
    {
        ASSERT_EQ(iter->second, persistentMap2[iter->first]);
    }
    fslib::fs::FileSystem::remove(fslibURI);
}

TEST_F(PersistentMapTest, testUploadAndDownloadWithNoneExistDir) {
    HA3_LOG(DEBUG, "Begin Test");

    PersistentMap persistentMap;
    std::string fslibURI = TEST_DATA_PATH"/testUploadAndDownloadWithNoneExistDir/folders/testUploadAndDownload";
    ASSERT_TRUE(persistentMap.init(fslibURI));
    persistentMap[string("a")] = string("A");
    persistentMap[string(":")] = string("\\");
    ASSERT_TRUE(persistentMap.upload());
    PersistentMap persistentMap2;
    ASSERT_TRUE(persistentMap2.init(fslibURI));
    ASSERT_TRUE(persistentMap2.download());

    for (map<string, string>::const_iterator iter = persistentMap._map.begin(); 
         iter != persistentMap._map.end(); ++iter)
    {
        ASSERT_EQ(iter->second, persistentMap2[iter->first]);
    }
    
    fslib::fs::FileSystem::remove(TEST_DATA_PATH"/testUploadAndDownloadWithNoneExistDir");
}

TEST_F(PersistentMapTest, testInitWithDir) {
    HA3_LOG(DEBUG, "Begin Test");

    PersistentMap persistentMap;
    std::string fslibURI = TEST_DATA_PATH"/PersistentMapTest_testUploadAndDownloadWithDir/folders/testUploadAndDownload/";
    ASSERT_TRUE(!persistentMap.init(fslibURI));
    fslibURI = TEST_DATA_PATH;
    ASSERT_TRUE(!persistentMap.init(fslibURI));
}

TEST_F(PersistentMapTest, testInitWithEmptyURI) {
    HA3_LOG(DEBUG, "Begin Test");

    PersistentMap persistentMap;
    std::string fslibURI;
    ASSERT_TRUE(!persistentMap.init(fslibURI));
}

TEST_F(PersistentMapTest, testUploadWithEmptyFolder) {
    PersistentMap persistentMap;
    std::string fslibURI = "abc";
    ASSERT_TRUE(persistentMap.init(fslibURI));
    ASSERT_TRUE(!persistentMap.upload());
}

TEST_F(PersistentMapTest, testDownloadWithNoneExistFilePath) {
    PersistentMap persistentMap;
    std::string fslibURI = TEST_DATA_PATH"/none_exist";
    ASSERT_TRUE(persistentMap.init(fslibURI));
    persistentMap[string("key")] = string("value");
    ASSERT_TRUE(persistentMap.download());
    ASSERT_EQ(size_t(0), persistentMap.size());
}

TEST_F(PersistentMapTest, testDownloadWithEmptyFile) {
    PersistentMap persistentMap;
    std::string fslibURI = TEST_DATA_PATH"/PersistentMapTest_testDownloadWithEmptyFile";
    ASSERT_TRUE(persistentMap.init(fslibURI));
    ASSERT_TRUE(persistentMap.upload());
    persistentMap[string("key")] = string("value");
    ASSERT_EQ(size_t(1), persistentMap.size());
    ASSERT_TRUE(persistentMap.download());
    ASSERT_EQ(size_t(0), persistentMap.size());
    fslib::fs::FileSystem::remove(fslibURI);
}

TEST_F(PersistentMapTest, testSetAndRemove) {
    PersistentMap persistentMap;
    persistentMap.set("key1", "value1");
    ASSERT_EQ(string("value1"), persistentMap["key1"]);
    persistentMap.remove("key1");
    string value;
    ASSERT_TRUE(!persistentMap.find("key1", value));
}

TEST_F(PersistentMapTest, testFindWithDefaultValue) {
    PersistentMap persistentMap;
    persistentMap.set("key1", "value1");
    ASSERT_EQ(string("value1"), persistentMap.findWithDefaultValue("key1", "abc"));
    ASSERT_EQ(string("abc"), persistentMap.findWithDefaultValue("key2", "abc"));
}
END_HA3_NAMESPACE(util);

