#include "indexlib/index/summary/SummaryMemReader.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/SummaryMemReaderContainer.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/index/summary/test/SummaryMaker.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace indexlibv2::config;

namespace indexlibv2::index {

class SummaryMemReaderTest : public TESTBASE
{
public:
    SummaryMemReaderTest() = default;
    ~SummaryMemReaderTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void TestSegmentReader(bool compress, uint32_t docCount);
    void CheckReadSummaryDocument(const std::shared_ptr<SummaryMemReader>& reader,
                                  const SummaryMaker::DocumentArray& answerDocArray);
    std::shared_ptr<SummaryIndexConfig> GetSummaryIndexConfig(const std::shared_ptr<ITabletSchema>& schema);

private:
    std::shared_ptr<config::ITabletSchema> _schema;
};

void SummaryMemReaderTest::setUp()
{
    _schema = table::NormalTabletSchemaMaker::Make(
        /* field */ "title:text;user_name:string;user_id:integer;price:float",
        /* index */ "pk:primarykey64:user_id",
        /* attribute */ "",
        /* summary */ "title;user_name;user_id;price");
}

void SummaryMemReaderTest::tearDown() {}

std::shared_ptr<SummaryIndexConfig>
SummaryMemReaderTest::GetSummaryIndexConfig(const std::shared_ptr<ITabletSchema>& schema)
{
    auto summaryConf = std::dynamic_pointer_cast<SummaryIndexConfig>(
        schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    assert(summaryConf);
    return summaryConf;
}

void SummaryMemReaderTest::TestSegmentReader(bool compress, uint32_t docCount)
{
    SummaryMaker::DocumentArray answerDocArray;
    autil::mem_pool::Pool pool;
    auto summaryMemIndexer = SummaryMaker::BuildOneSegmentWithoutDump(docCount, _schema, &pool, answerDocArray);

    auto container = std::dynamic_pointer_cast<SummaryMemReaderContainer>(summaryMemIndexer->CreateInMemReader());
    assert(container != nullptr);

    CheckReadSummaryDocument(container->GetSummaryMemReader(0), answerDocArray);
}

void SummaryMemReaderTest::CheckReadSummaryDocument(const std::shared_ptr<SummaryMemReader>& reader,
                                                    const SummaryMaker::DocumentArray& answerDocArray)
{
    for (size_t i = 0; i < answerDocArray.size(); ++i) {
        auto answerDoc = answerDocArray[i];
        indexlib::document::SearchSummaryDocument gotDoc(nullptr, GetSummaryIndexConfig(_schema)->GetSummaryCount());
        reader->GetDocument(i, &gotDoc);

        ASSERT_EQ(answerDoc->GetNotEmptyFieldCount(), gotDoc.GetFieldCount());

        for (uint32_t j = 0; j < gotDoc.GetFieldCount(); ++j) {
            const autil::StringView& expectField = answerDoc->GetField((fieldid_t)j);
            const autil::StringView* str = gotDoc.GetFieldValue((summaryfieldid_t)j);
            ASSERT_EQ(expectField, *str);
        }
    }
}

TEST_F(SummaryMemReaderTest, TestCaseForRead)
{
    // no compress
    TestSegmentReader(false, 2000);
    // using compress
    TestSegmentReader(true, 2000);
}

} // namespace indexlibv2::index
