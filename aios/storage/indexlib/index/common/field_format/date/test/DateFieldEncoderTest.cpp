#include "indexlib/index/common/field_format/date/DateFieldEncoder.h"

#include "autil/StringUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlib::index {
class DateFieldEncoderTest : public TESTBASE
{
private:
    void TestSimpleProcess();

    void InnerTestCreateTerms(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                              const std::string& timestamp, const std::string& terms,
                              const config::DateLevelFormat& format);
    void CreateTerms(const std::string& terms, std::vector<DateTerm>& dateTerms);
    void CreateTerms(const std::string& terms, std::vector<dictkey_t>& dateTerms);
};

TEST_F(DateFieldEncoderTest, TestSimpleProcess) { TestSimpleProcess(); }

void DateFieldEncoderTest::TestSimpleProcess()
{
    std::string index = "time_index:date:time;";
    std::string attribute = "time";

    auto timestampSchema = indexlibv2::table::NormalTabletSchemaMaker::Make("time:timestamp", index, attribute, "");
    auto uint64Schema = indexlibv2::table::NormalTabletSchemaMaker::Make("time:uint64", index, attribute, "");

    {
        config::DateLevelFormat format;
        format.Init(3415, config::DateLevelFormat::MILLISECOND);
        std::string timestamp64 = "1516259027300"; // 2018-1-18 07:03:47 300miliseconds, G.M.T
        std::string timestamp = "2018-01-18 07:03:47.300";
        std::string termStr = "12:48;11:48:1;9:48:1:0:18;7:48:1:0:18:0:7;"
                              "5:48:1:0:18:0:7:0:3;3:48:1:0:18:0:7:0:3:0:47;"
                              "2:48:1:0:18:0:7:0:3:0:47:9;"
                              "1:48:1:0:18:0:7:0:3:0:47:9:12;";

        InnerTestCreateTerms(uint64Schema, timestamp64, termStr, format);
        InnerTestCreateTerms(timestampSchema, timestamp, termStr, format);
    }
    {
        config::DateLevelFormat format;
        format.Init(3415, config::DateLevelFormat::MINUTE);
        std::string timestamp64 = "1516096346000"; // 2018-1-16 09:52:26 000miliseconds, G.M.T
        std::string timestamp = "2018-01-16 09:52:26";
        std::string termStr = "12:48;11:48:1;9:48:1:0:16;7:48:1:0:16:0:9;"
                              "5:48:1:0:16:0:9:0:52;";
        InnerTestCreateTerms(uint64Schema, timestamp64, termStr, format);
        InnerTestCreateTerms(timestampSchema, timestamp, termStr, format);
    }
    {
        config::DateLevelFormat format;
        format.Init(4032, config::DateLevelFormat::MILLISECOND);
        std::string timestamp64 = "1516939889000"; // 2018-1-26 04:11:29 000miliseconds, G.M.T
        std::string timestamp = "2018-01-26 04:11:29";
        std::string termStr = "12:48;"
                              "11:48:1;"
                              "10:48:1:6;"
                              "9:48:1:6:2;"
                              "8:48:1:6:2:1;"
                              "7:48:1:6:2:1:0";
        InnerTestCreateTerms(uint64Schema, timestamp64, termStr, format);
        InnerTestCreateTerms(timestampSchema, timestamp, termStr, format);
    }
    {
        config::DateLevelFormat format;
        format.Init(3583, config::DateLevelFormat::MILLISECOND);
        std::string timestamp64 = "1516939889111"; // 2018-1-26 04:11:29 111miliseconds, G.M.T
        std::string timestamp = "2018-01-26 04:11:29.111";
        std::string termStr = "12:48;"
                              "11:48:1;"
                              "9:48:1:0:26;"
                              "8:48:1:0:26:1;"
                              "7:48:1:0:26:1:0;"
                              "6:48:1:0:26:1:0:1;"
                              "5:48:1:0:26:1:0:1:3;"
                              "4:48:1:0:26:1:0:1:3:3;"
                              "3:48:1:0:26:1:0:1:3:3:5;"
                              "2:48:1:0:26:1:0:1:3:3:5:3;"
                              "1:48:1:0:26:1:0:1:3:3:5:3:15;";

        InnerTestCreateTerms(uint64Schema, timestamp64, termStr, format);
        InnerTestCreateTerms(timestampSchema, timestamp, termStr, format);
    }
}

void DateFieldEncoderTest::InnerTestCreateTerms(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                                const std::string& timestamp, const std::string& termStr,
                                                const config::DateLevelFormat& format)
{
    ASSERT_TRUE(schema);
    auto indexConfig = schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, "time_index");
    ASSERT_TRUE(indexConfig);
    auto dateConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(indexConfig);
    ASSERT_TRUE(dateConfig);
    dateConfig->SetDateLevelFormat(format);

    DateFieldEncoder encoder({dateConfig});
    std::vector<dictkey_t> dictKeys;
    encoder.Encode(0, timestamp, dictKeys);
    std::vector<dictkey_t> expectedKeys;
    CreateTerms(termStr, expectedKeys);
    sort(dictKeys.begin(), dictKeys.end());
    sort(expectedKeys.begin(), expectedKeys.end());
    ASSERT_EQ(expectedKeys, dictKeys);
}

// level:year:month:midmonth:day:midday:hour:midmin:min:midsecond:second:midmilli:milisecond;
// level:year:month:midmonth:day:midday:hour:midmin:min:midsecond:second:midmilli:milisecond;
void DateFieldEncoderTest::CreateTerms(const std::string& terms, std::vector<dictkey_t>& dateTerms)
{
    std::vector<uint64_t> shift = {60, 36, 32, 29, 27, 24, 22, 19, 16, 13, 10, 5, 0};
    std::vector<std::string> termInfos;
    autil::StringUtil::fromString(terms, termInfos, ";");
    for (size_t i = 0; i < termInfos.size(); i++) {
        std::vector<std::string> termInfo;
        uint64_t value = 0;
        autil::StringUtil::fromString(termInfos[i], termInfo, ":");
        for (size_t j = 0; j < termInfo.size(); j++) {
            uint64_t num = autil::StringUtil::numberFromString<uint64_t>(termInfo[j]);
            value |= (num << shift[j]);
        }
        dateTerms.push_back((dictkey_t)value);
    }
}

void DateFieldEncoderTest::CreateTerms(const std::string& terms, std::vector<DateTerm>& dateTerms)
{
    std::vector<dictkey_t> dictKeys;
    CreateTerms(terms, dictKeys);
    for (auto x : dictKeys) {
        dateTerms.push_back(DateTerm(x));
    }
}

} // namespace indexlib::index
