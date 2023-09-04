#include "indexlib/index/inverted_index/format/dictionary/DefaultTermDictionaryReader.h"

#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/format/DictInlineDecoder.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DefaultTermDictionaryReaderTest : public TESTBASE
{
public:
    DefaultTermDictionaryReaderTest() = default;
    ~DefaultTermDictionaryReaderTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void DefaultTermDictionaryReaderTest::setUp() {}

void DefaultTermDictionaryReaderTest::tearDown() {}

TEST_F(DefaultTermDictionaryReaderTest, testSimple)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig("test", ft_integer, false));
    fieldConfig->SetDefaultValue("123");
    std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> config(
        new indexlibv2::config::SingleFieldIndexConfig("test", it_number));
    ASSERT_TRUE(config->SetFieldConfig(fieldConfig).IsOK());
    {
        DefaultTermDictionaryReader reader(config, 10);
        ASSERT_TRUE(reader.Open(nullptr, "", false).IsOK());

        auto iter = reader.CreateIterator();
        ASSERT_TRUE(iter->HasNext());
        index::DictKeyInfo key;
        dictvalue_t value;
        iter->Next(key, value);
        ASSERT_TRUE(reader.InnerLookup(key.GetKey(), {}, value).Ok());

        ASSERT_FALSE(iter->HasNext());
        bool isDocList = false;
        bool dfFirst = false;
        ShortListOptimizeUtil::IsDictInlineCompressMode(value, isDocList, dfFirst);
        ASSERT_TRUE(isDocList);
        ASSERT_TRUE(dfFirst);
        DictInlineDecoder decoder;
        docid_t startDocId = -1;
        df_t df = 0;
        decoder.DecodeContinuousDocId({dfFirst, value}, startDocId, df);
        ASSERT_EQ(0, startDocId);
        ASSERT_EQ(10, df);
    }

    {
        DefaultTermDictionaryReader reader(config, 1 << 30);
        ASSERT_TRUE(reader.Open(nullptr, "", false).IsOK());

        auto iter = reader.CreateIterator();
        ASSERT_TRUE(iter->HasNext());
        index::DictKeyInfo key;
        dictvalue_t value;
        iter->Next(key, value);
        ASSERT_TRUE(reader.InnerLookup(key.GetKey(), {}, value).Ok());

        ASSERT_FALSE(iter->HasNext());
        bool isDocList = false;
        bool dfFirst = false;
        ShortListOptimizeUtil::IsDictInlineCompressMode(value, isDocList, dfFirst);
        ASSERT_TRUE(isDocList);
        ASSERT_FALSE(dfFirst);
        DictInlineDecoder decoder;
        docid_t startDocId = -1;
        df_t df = 0;
        decoder.DecodeContinuousDocId({dfFirst, value}, startDocId, df);
        ASSERT_EQ(0, startDocId);
        ASSERT_EQ(1 << 30, df);
    }
}

TEST_F(DefaultTermDictionaryReaderTest, testErrorConfig)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig("test", ft_integer, false));
    std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> config(
        new indexlibv2::config::SingleFieldIndexConfig("test", it_number));
    ASSERT_TRUE(config->SetFieldConfig(fieldConfig).IsOK());
    DefaultTermDictionaryReader reader(config, 10);
    ASSERT_TRUE(reader.Open(nullptr, "", false).IsInternalError());
}

TEST_F(DefaultTermDictionaryReaderTest, testForEmptySegment)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig("test", ft_integer, false));
    std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> config(
        new indexlibv2::config::SingleFieldIndexConfig("test", it_number));
    ASSERT_TRUE(config->SetFieldConfig(fieldConfig).IsOK());
    DefaultTermDictionaryReader reader(config, 0);
    ASSERT_TRUE(reader.Open(nullptr, "", false).IsOK());
    auto iter = reader.CreateIterator();
    ASSERT_FALSE(iter->HasNext());
}

TEST_F(DefaultTermDictionaryReaderTest, testIterSeek)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig("test", ft_integer, false));
    fieldConfig->SetDefaultValue("123");
    std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> config(
        new indexlibv2::config::SingleFieldIndexConfig("test", it_number));
    ASSERT_TRUE(config->SetFieldConfig(fieldConfig).IsOK());

    DefaultTermDictionaryReader reader(config, 10);
    ASSERT_TRUE(reader.Open(nullptr, "", false).IsOK());

    auto iter = reader.CreateIterator();
    ASSERT_TRUE(iter->HasNext());
    index::DictKeyInfo key;
    dictvalue_t value;
    iter->Next(key, value);

    auto iter2 = reader.CreateIterator();
    iter2->Seek(key.GetKey() - 1);
    ASSERT_TRUE(iter2->HasNext());

    iter2->Seek(key.GetKey() + 1);
    ASSERT_FALSE(iter2->HasNext());
}

} // namespace indexlib::index
