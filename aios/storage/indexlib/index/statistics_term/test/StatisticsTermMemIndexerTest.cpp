#include "indexlib/index/statistics_term/StatisticsTermMemIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/statistics_term/StatisticsTermLeafReader.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class StatisticsTermMemIndexerTest : public TESTBASE
{
public:
    void setUp() override { _indexRoot = GET_TEMP_DATA_PATH(); }
    void tearDown() override {}

private:
    std::string _indexRoot;
};

TEST_F(StatisticsTermMemIndexerTest, testSimpleProcess)
{
    // StatisticsTermMemIndexer
    std::map<std::string, size_t> initialKeyCount;
    auto indexer = std::make_shared<StatisticsTermMemIndexer>(initialKeyCount);

    auto jsonStr = R"(
        {
            "index_name" : "statistics_term",
            "index_type" : "statistics_term",
            "associated_inverted_index" : ["index1", "index2"]
        }
    )";
    auto config = std::make_shared<StatisticsTermIndexConfig>();
    ASSERT_NO_THROW(FromJsonString(*config, jsonStr));
    indexer->_indexConfig = config;

    auto termHashMap1 = std::make_unique<StatisticsTermMemIndexer::TermHashMap>(*(indexer->_allocator));
    (*termHashMap1)[10424182954406993763ull] = "hello";
    (*termHashMap1)[9128971427530205461ull] = "world";

    auto termHashMap2 = std::make_unique<StatisticsTermMemIndexer::TermHashMap>(*(indexer->_allocator));
    (*termHashMap2)[10424182954406993763ull] = "hello";
    (*termHashMap2)[14319748486496374167ull] = "you";

    indexer->_termOriginValueMap.insert(std::make_pair("index1", std::move(termHashMap1)));
    indexer->_termOriginValueMap.insert(std::make_pair("index2", std::move(termHashMap2)));

    auto dir = indexlib::file_system::Directory::GetPhysicalDirectory(_indexRoot);
    auto status = indexer->Dump(nullptr, dir, nullptr);
    ASSERT_TRUE(status.IsOK());

    // StatisticsTermLeafReader
    auto reader = std::make_shared<StatisticsTermLeafReader>();
    status = reader->Open(config, dir->GetIDirectory());
    ASSERT_TRUE(status.IsOK());
    // check meta
    auto termMeta = reader->_termMetas;
    auto iter = termMeta.find("index1");
    ASSERT_TRUE(iter != termMeta.end());
    std::string info1 = "index1\t2\n";
    std::string blockStr1 = "10424182954406993763\t5\thello\n9128971427530205461\t5\tworld\n";
    auto totalLen1 = info1.length();
    totalLen1 = totalLen1 + blockStr1.length();
    ASSERT_EQ(iter->second, std::make_pair(0ul, totalLen1));

    iter = termMeta.find("index2");
    ASSERT_TRUE(iter != termMeta.end());
    std::string info2 = "index2\t2\n";
    std::string blockStr2 = "10424182954406993763\t5\thello\n14319748486496374167\t3\tyou\n";
    auto totalLen2 = info2.length();
    totalLen2 = totalLen2 + blockStr2.length();
    ASSERT_EQ(iter->second, std::make_pair(totalLen1, totalLen2));

    // check data
    std::unordered_map<size_t, std::string> termStatistics;
    status = reader->GetTermStatistics("index1", termStatistics);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(termStatistics.size() != 0);
    auto it = termStatistics.find(10424182954406993763ull);
    ASSERT_TRUE(it != termStatistics.end());
    ASSERT_EQ("hello", it->second);
    it = termStatistics.find(9128971427530205461ull);
    ASSERT_TRUE(it != termStatistics.end());
    ASSERT_EQ("world", it->second);

    std::unordered_map<size_t, std::string> termStatistics2;
    status = reader->GetTermStatistics("index2", termStatistics2);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(termStatistics2.size() != 0);
    it = termStatistics2.find(14319748486496374167ull);
    ASSERT_TRUE(it != termStatistics2.end());
    ASSERT_EQ("you", it->second);
}

TEST_F(StatisticsTermMemIndexerTest, testSpecialCharacter)
{
    // StatisticsTermMemIndexer
    std::map<std::string, size_t> initialKeyCount;
    auto indexer = std::make_shared<StatisticsTermMemIndexer>(initialKeyCount);

    auto jsonStr = R"(
        {
            "index_name" : "statistics_term",
            "index_type" : "statistics_term",
            "associated_inverted_index" : ["index1", "index2"]
        }
    )";
    auto config = std::make_shared<StatisticsTermIndexConfig>();
    ASSERT_NO_THROW(FromJsonString(*config, jsonStr));
    indexer->_indexConfig = config;

    auto termHashMap1 = std::make_unique<StatisticsTermMemIndexer::TermHashMap>(*(indexer->_allocator));
    (*termHashMap1)[5645106440212814756ull] = "he\tllo";
    (*termHashMap1)[11873638451522258648ull] = "wor\nd";

    auto termHashMap2 = std::make_unique<StatisticsTermMemIndexer::TermHashMap>(*(indexer->_allocator));
    (*termHashMap2)[3619413284006683062ull] = "you\t1\n";

    indexer->_termOriginValueMap.insert(std::make_pair("index1", std::move(termHashMap1)));
    indexer->_termOriginValueMap.insert(std::make_pair("index2", std::move(termHashMap2)));

    auto dir = indexlib::file_system::Directory::GetPhysicalDirectory(_indexRoot);
    auto status = indexer->Dump(nullptr, dir, nullptr);
    ASSERT_TRUE(status.IsOK());

    // StatisticsTermLeafReader
    auto reader = std::make_shared<StatisticsTermLeafReader>();
    status = reader->Open(config, dir->GetIDirectory());
    ASSERT_TRUE(status.IsOK());
    // check meta
    auto termMeta = reader->_termMetas;
    auto iter = termMeta.find("index1");
    ASSERT_TRUE(iter != termMeta.end());
    std::string info1 = "index1\t2\n";
    std::string blockStr1 = "5645106440212814756\t6\the\tllo\n11873638451522258648\t6\twor\nd\n";
    auto totalLen1 = info1.length();
    totalLen1 = totalLen1 + blockStr1.length();
    ASSERT_EQ(iter->second, std::make_pair(0ul, totalLen1));

    iter = termMeta.find("index2");
    ASSERT_TRUE(iter != termMeta.end());
    std::string info2 = "index2\t1\n";
    std::string blockStr2 = "3619413284006683062\t6\tyou\t1\n\n";
    auto totalLen2 = info2.length();
    totalLen2 = totalLen2 + blockStr2.length();
    ASSERT_EQ(iter->second, std::make_pair(totalLen1, totalLen2));

    // check data
    std::unordered_map<size_t, std::string> termStatistics;
    status = reader->GetTermStatistics("index1", termStatistics);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(termStatistics.size() != 0);
    auto it = termStatistics.find(5645106440212814756ull);
    ASSERT_TRUE(it != termStatistics.end());
    ASSERT_EQ("he\tllo", it->second);
    it = termStatistics.find(11873638451522258648ull);
    ASSERT_TRUE(it != termStatistics.end());
    ASSERT_EQ("wor\nd", it->second);

    std::unordered_map<size_t, std::string> termStatistics2;
    status = reader->GetTermStatistics("index2", termStatistics2);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(termStatistics2.size() != 0);
    it = termStatistics2.find(3619413284006683062ull);
    ASSERT_TRUE(it != termStatistics2.end());
    ASSERT_EQ("you\t1\n", it->second);
}

} // namespace indexlibv2::index