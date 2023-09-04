#include "indexlib/index/kv/test/hash_table_fix_writer_unittest.h"

#include "indexlib/common/hash_table/special_value.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashTableFixWriterTest);

HashTableFixWriterTest::HashTableFixWriterTest() {}

HashTableFixWriterTest::~HashTableFixWriterTest() {}

void HashTableFixWriterTest::CaseSetUp()
{
    string field = "key:int32;value:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");
    mKVConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
}

void HashTableFixWriterTest::CaseTearDown() {}

void HashTableFixWriterTest::TestAddDocument()
{
    InnerTestAddDocument<uint64_t>();
    typedef TimestampValue<uint64_t> ValueType;
    InnerTestAddDocument<ValueType>();
}

template <typename T>
void HashTableFixWriterTest::InnerTestAddDocument()
{
}
}} // namespace indexlib::index
