#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_writer.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index {

class CustomizedIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    CustomizedIndexWriterTest();
    ~CustomizedIndexWriterTest();

    DECLARE_CLASS_NAME(CustomizedIndexWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestException();
    void TestNoException();

private:
    void DoTestException(bool ignoreBuildError);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomizedIndexWriterTest, TestException);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexWriterTest, TestNoException);
IE_LOG_SETUP(index, CustomizedIndexWriterTest);

CustomizedIndexWriterTest::CustomizedIndexWriterTest() {}

CustomizedIndexWriterTest::~CustomizedIndexWriterTest() {}

void CustomizedIndexWriterTest::CaseSetUp() {}

void CustomizedIndexWriterTest::CaseTearDown() {}

namespace {
class MockIndexer : public Indexer
{
public:
    MockIndexer() : Indexer(util::KeyValueMap()) {}

public:
    MOCK_METHOD(bool, Build, (const std::vector<const document::Field*>& fieldVec, docid_t docId), (override));
    MOCK_METHOD(bool, Dump, (const file_system::DirectoryPtr& dir), (override));
    MOCK_METHOD(int64_t, EstimateTempMemoryUseInDump, (), (const, override));
    MOCK_METHOD(int64_t, EstimateDumpFileSize, (), (const, override));
};
DEFINE_SHARED_PTR(MockIndexer);
} // namespace

void CustomizedIndexWriterTest::TestException() { DoTestException(false); }
void CustomizedIndexWriterTest::TestNoException() { DoTestException(true); }
void CustomizedIndexWriterTest::DoTestException(bool ignoreBuildError)
{
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "customized_index_schema2.json";
    string jsonString;
    FslibWrapper::AtomicLoadE(confFile, jsonString);

    config::IndexPartitionSchemaPtr schema;
    index_base::SchemaAdapter::LoadSchema(jsonString, schema);
    config::IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("cindex1");

    CustomizedIndexWriter writer(plugin::PluginManagerPtr(), 0, /*ignoreBuildError=*/ignoreBuildError);
    MockIndexerPtr indexer(new MockIndexer);
    writer.mIndexer = indexer;
    writer._indexConfig = indexConfig;
    util::BuildResourceMetrics buildResourceMetrics;
    writer._buildResourceMetricsNode = buildResourceMetrics.AllocateNode();
    EXPECT_CALL(*indexer, Build(_, _)).WillOnce(Return(false));

    autil::mem_pool::Pool pool;
    document::IndexDocument indexDoc(&pool);
    if (ignoreBuildError) {
        ASSERT_NO_THROW(writer.EndDocument(indexDoc));
    } else {
        ASSERT_ANY_THROW(writer.EndDocument(indexDoc));
    }
    EXPECT_CALL(*indexer, Dump(_)).WillOnce(Return(false));
    ASSERT_ANY_THROW(writer.Dump(GET_SEGMENT_DIRECTORY(), &pool));
}
}} // namespace indexlib::index
