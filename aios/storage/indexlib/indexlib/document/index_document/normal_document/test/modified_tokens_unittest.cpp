#include <limits>
#include <string>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace document {
class ModifiedTokensTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ModifiedTokensTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestSerialize()
    {
        autil::mem_pool::Pool pool;
        ModifiedTokens tokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/2, {1, -2});

        autil::DataBuffer buffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &pool);
        autil::StringView data = tokens.Serialize(buffer);

        ModifiedTokens tokens2;
        autil::DataBuffer buffer2((char*)data.data(), data.size());
        tokens2.Deserialize(buffer2);

        ASSERT_TRUE(ModifiedTokens::Equal(tokens, tokens2));
    }
};

INDEXLIB_UNIT_TEST_CASE(ModifiedTokensTest, TestSerialize);
}} // namespace indexlib::document
