#include "indexlib/common/field_format/date/test/date_field_encoder_unittest.h"
#include "indexlib/test/schema_maker.h"
#include <autil/StringUtil.h>
using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);


IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(date, DateFieldEncoderTest);

DateFieldEncoderTest::DateFieldEncoderTest()
{
}

DateFieldEncoderTest::~DateFieldEncoderTest()
{
}

void DateFieldEncoderTest::CaseSetUp()
{
}

void DateFieldEncoderTest::CaseTearDown()
{
}

void DateFieldEncoderTest::TestSimpleProcess()
{
    string field = "time:uint64;";
    string index = "time_index:date:time;";
    string attribute = "time";
    
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    DateIndexConfigPtr dateConfig = 
        DYNAMIC_POINTER_CAST(DateIndexConfig,
                             schema->GetIndexSchema()->GetIndexConfig("time_index"));
    {
        DateLevelFormat format;
        format.Init(3415, DateLevelFormat::MILLISECOND);
        dateConfig->SetDateLevelFormat(format);        
        string timestamp = "1516259027300"; // 2018-1-18 07:03:47 300miliseconds, G.M.T
        string termStr = "12:48;11:48:1;9:48:1:0:18;7:48:1:0:18:0:7;"
                         "5:48:1:0:18:0:7:0:3;3:48:1:0:18:0:7:0:3:0:47;"
                         "2:48:1:0:18:0:7:0:3:0:47:9;"
                         "1:48:1:0:18:0:7:0:3:0:47:9:12;";
        InnerTestCreateTerms(schema, timestamp, termStr);
    }
    {
        DateLevelFormat format;
        format.Init(3415, DateLevelFormat::MINUTE);
        dateConfig->SetDateLevelFormat(format);

        string timestamp = "1516096346000"; // 2018-1-16 09:52:26 000miliseconds, G.M.T
        string termStr = "12:48;11:48:1;9:48:1:0:16;7:48:1:0:16:0:9;"
                         "5:48:1:0:16:0:9:0:52;";
        InnerTestCreateTerms(schema, timestamp, termStr);
    }
    {
        DateLevelFormat format;
        format.Init(4032, DateLevelFormat::MILLISECOND);
        dateConfig->SetDateLevelFormat(format);

        string timestamp = "1516939889000"; // 2018-1-26 04:11:29 000miliseconds, G.M.T
        string termStr = "12:48;"
                         "11:48:1;"
                         "10:48:1:6;"
                         "9:48:1:6:2;"
                         "8:48:1:6:2:1;"
                         "7:48:1:6:2:1:0";
        InnerTestCreateTerms(schema, timestamp, termStr);
    }
    {
        DateLevelFormat format;
        format.Init(3583, DateLevelFormat::MILLISECOND);
        dateConfig->SetDateLevelFormat(format);

        string timestamp = "1516939889111"; // 2018-1-26 04:11:29 111miliseconds, G.M.T
        string termStr = "12:48;"
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
        InnerTestCreateTerms(schema, timestamp, termStr);
    }
}

void DateFieldEncoderTest::InnerTestCreateTerms(config::IndexPartitionSchemaPtr schema,
                   const std::string& timestamp, const std::string& termStr)
{
    DateFieldEncoder encoder(schema);
    std::vector<dictkey_t> dictKeys;
    encoder.Encode(0, timestamp, dictKeys);
    std::vector<dictkey_t> expectedKeys;
    CreateTerms(termStr, expectedKeys);
    sort(dictKeys.begin(), dictKeys.end());
    sort(expectedKeys.begin(), expectedKeys.end());
    ASSERT_EQ(expectedKeys, dictKeys);
}
//level:year:month:midmonth:day:midday:hour:midmin:min:midsecond:second:midmilli:milisecond;
//level:year:month:midmonth:day:midday:hour:midmin:min:midsecond:second:midmilli:milisecond;
void DateFieldEncoderTest::CreateTerms(const std::string& terms, std::vector<dictkey_t>& dateTerms)
{
    vector<uint64_t> shift = {60,36,32,29,27,24,22,19,16,13,10,5,0};
    vector<string> termInfos;
    StringUtil::fromString(terms, termInfos, ";");
    for (size_t i = 0; i < termInfos.size(); i++)
    {
        vector<string> termInfo;
        uint64_t value = 0;
        StringUtil::fromString(termInfos[i], termInfo, ":");
        for (size_t j = 0; j < termInfo.size(); j++)
        {
            uint64_t num = StringUtil::numberFromString<uint64_t>(termInfo[j]);
            value |= (num << shift[j]);
        }
        dateTerms.push_back((dictkey_t)value);
    }
}

void DateFieldEncoderTest::CreateTerms(const std::string& terms,
                                       std::vector<DateTerm>& dateTerms)
{
    vector<dictkey_t> dictKeys;
    CreateTerms(terms, dictKeys);
    for(auto x: dictKeys)
    {
        dateTerms.push_back(DateTerm(x));
    }
}

IE_NAMESPACE_END(common);

