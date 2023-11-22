#include "indexlib/index/field_meta/meta/DataStatisticsMeta.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/IDirectory.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DataStatisticsMetaTest : public TESTBASE
{
public:
    DataStatisticsMetaTest() = default;
    ~DataStatisticsMetaTest() = default;

public:
    void setUp() override;
    void tearDown() override;
    void generateFieldValueBatch(const std::vector<std::string>& sources, IFieldMeta::FieldValueBatch& batch) const;
};

void DataStatisticsMetaTest::setUp() {}

void DataStatisticsMetaTest::tearDown() {}

void DataStatisticsMetaTest::generateFieldValueBatch(const std::vector<std::string>& sources,
                                                     IFieldMeta::FieldValueBatch& batch) const
{
    docid_t docId = 0;
    for (const auto& source : sources) {
        FieldValueMeta meta {source, source.size()};
        bool isNull = source.empty();
        batch.push_back({meta, isNull, docId});
        docId++;
    }
}

TEST_F(DataStatisticsMetaTest, testUsage)
{
    std::string metaStr = R"({
        "metaName" : "DataStatistics",
        "metaParams" : {
            "top_num" : 2
        }
    })";
    FieldMetaConfig::FieldMetaInfo info;
    FromJsonString(info, metaStr);
    DataStatisticsMeta fieldMeta(info);
    std::vector<std::string> sourceFields = {"1", "3123", "hdkajshd", "dasd", "", "1", "2", ""};
    IFieldMeta::FieldValueBatch batch;
    generateFieldValueBatch(sourceFields, batch);
    ASSERT_TRUE(fieldMeta.Build(batch).IsOK());
    autil::mem_pool::Pool pool;
    auto dir = file_system::IDirectory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    ASSERT_TRUE(fieldMeta.Store(&pool, dir).IsOK());

    auto check = [&](const DataStatisticsMeta& fieldMeta) {
        ASSERT_EQ(6, fieldMeta.GetNDV());
        std::map<std::string, uint32_t> mcv = {{"1", 2}, {"", 2}};
        std::map<std::string, uint32_t> lcv = {{"2", 1}, {"3123", 1}};
        ASSERT_EQ(mcv, fieldMeta.GetMCV());
        ASSERT_EQ(lcv, fieldMeta.GetLCV());

        ASSERT_EQ(1, fieldMeta.GetEstimateCount("dasd"));
        ASSERT_EQ(2, fieldMeta.GetEstimateCount("1"));
        ASSERT_EQ(1, fieldMeta.GetEstimateCount("2"));
        ASSERT_EQ(sourceFields.size(), fieldMeta.TEST_GetDocCount());
    };
    check(fieldMeta);

    {
        DataStatisticsMeta fieldMeta(info);
        ASSERT_TRUE(fieldMeta.Load(dir).IsOK());
        check(fieldMeta);
    }
}

TEST_F(DataStatisticsMetaTest, testMerge)
{
    auto build = [this](DataStatisticsMeta& fieldMeta, const std::vector<std::string>& fields) {
        IFieldMeta::FieldValueBatch batch;
        generateFieldValueBatch(fields, batch);
        ASSERT_TRUE(fieldMeta.Build(batch).IsOK());
        autil::mem_pool::Pool pool;
        auto dir = file_system::IDirectory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
        ASSERT_TRUE(fieldMeta.Store(&pool, dir).IsOK());
    };

    std::string metaStr = R"({
        "metaName" : "DataStatistics",
        "metaParams" : {
            "top_num" : 3
        }
    })";
    FieldMetaConfig::FieldMetaInfo info;
    FromJsonString(info, metaStr);
    DataStatisticsMeta fieldMeta1(info);
    std::vector<std::string> fields1 = {"dads", "12s", "78eq", "pps", "12s", "pps", "dads"};
    build(fieldMeta1, fields1);

    auto fieldMeta2 = std::make_shared<DataStatisticsMeta>(info);
    std::vector<std::string> fields2 = {"dads", "78eq", "nm", "12s", "abc", "s1"};
    build(*fieldMeta2, fields2);

    ASSERT_TRUE(fieldMeta1.Merge(fieldMeta2).IsOK());
    std::map<std::string, uint32_t> mcv {{"dads", 3}, {"12s", 3}, {"pps", 2}};
    std::map<std::string, uint32_t> lcv {{"nm", 1}, {"abc", 1}, {"s1", 1}};
    ASSERT_EQ(mcv, fieldMeta1.GetMCV());
    ASSERT_EQ(lcv, fieldMeta1.GetLCV());

    ASSERT_EQ(6, fieldMeta1.GetNDV());
    ASSERT_EQ(fields1.size() + fields2.size(), fieldMeta1.TEST_GetDocCount());
}

TEST_F(DataStatisticsMetaTest, testSmallData)
{
    std::string metaStr = R"({"metaName" : "DataStatistics"    })";
    FieldMetaConfig::FieldMetaInfo info;
    FromJsonString(info, metaStr);
    DataStatisticsMeta fieldMeta(info);
    std::vector<std::string> sourceFields = {"1", "1"};
    IFieldMeta::FieldValueBatch batch;
    generateFieldValueBatch(sourceFields, batch);
    ASSERT_TRUE(fieldMeta.Build(batch).IsOK());
    autil::mem_pool::Pool pool;
    auto dir = file_system::IDirectory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    ASSERT_TRUE(fieldMeta.Store(&pool, dir).IsOK());

    ASSERT_EQ(1, fieldMeta.GetNDV());
    std::map<std::string, uint32_t> mcv = {{"1", 2}};
    std::map<std::string, uint32_t> lcv = {{"1", 2}};
    ASSERT_EQ(mcv, fieldMeta.GetMCV());
    ASSERT_EQ(lcv, fieldMeta.GetLCV());

    ASSERT_EQ(0, fieldMeta.GetEstimateCount("dasd"));
    ASSERT_EQ(0, fieldMeta.GetEstimateCount("noexist"));
    ASSERT_EQ(2, fieldMeta.GetEstimateCount("1"));
}

} // namespace indexlib::index
