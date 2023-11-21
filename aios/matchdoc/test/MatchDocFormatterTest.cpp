#include "matchdoc/MatchDocFormatter.h"

#include "gtest/gtest.h"

#include "FiveBytesString.h"
#include "matchdoc/toolkit/MatchDocTestHelper.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

class MatchDocFormatterTest : public testing::Test {
public:
    void SetUp() override { ReferenceTypesWrapper::getInstance()->registerType<FiveBytesString>(); }
    void TearDown() override { ReferenceTypesWrapper::getInstance()->unregisterType<FiveBytesString>(); }
};

TEST_F(MatchDocFormatterTest, testSimple) {
    auto pool = std::make_shared<autil::mem_pool::Pool>();
    MatchDocAllocator allocator(pool);
    Reference<FiveBytesString> *fbsField = allocator.declare<FiveBytesString>("five");
    ASSERT_TRUE(fbsField);
    allocator.extend();
    EXPECT_EQ(5, allocator.getDocSize());
    FiveBytesString fbs;
    fbs.data[0] = 'a';

    MatchDoc doc1 = allocator.allocate();
    fbsField->set(doc1, fbs);

    vector<string> fieldNameVec{"fbsField"};
    ReferenceVector refs{fbsField};
    vector<MatchDoc> docs{doc1};
    flatbuffers::FlatBufferBuilder fbb;
    std::string errMsg;
    string tableName = "test";

    auto offset = MatchDocFormatter::toFBResultRecordsByColumns(tableName, fieldNameVec, refs, docs, fbb, errMsg);
    fbb.Finish(offset);

    const auto *matchdocPtr = flatbuffers::GetRoot<MatchRecords>(fbb.GetBufferPointer());
    const auto &doc_count = matchdocPtr->doc_count();
    ASSERT_EQ(1, doc_count);

    const auto &table_name = matchdocPtr->table_name();
    ASSERT_EQ(tableName, table_name->str());

    const auto &field_names = matchdocPtr->field_name();
    ASSERT_EQ(1, field_names->size());

    const auto &record_columns = matchdocPtr->record_columns();
    ASSERT_EQ(1u, record_columns->size());
    const auto &five = record_columns->Get(0);
    ASSERT_EQ(five->field_value_column_type(), matchdoc::FieldValueColumn_UnknownValueColumn);
    auto value = static_cast<const matchdoc::UnknownValueColumn *>(five->field_value_column())->value();
    ASSERT_EQ(0, value);
}
