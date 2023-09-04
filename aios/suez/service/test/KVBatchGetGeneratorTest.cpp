#include "suez/service/KVBatchGetGenerator.h"

#include "absl/container/flat_hash_set.h"
#include "unittest/unittest.h"
using namespace std;
using namespace testing;

namespace suez {

class KVBatchGetGeneratorTest : public TESTBASE {};

TEST_F(KVBatchGetGeneratorTest, testConstructHashKeysRequest) {
    KVBatchGetGenerator::KVBatchGetOptions options;
    options.attrs = {"weights"};
    options.indexName = "key";
    options.serviceName = "test_service";
    options.timeout = 5000;

    std::unordered_map<multi_call::PartIdTy, KVBatchGetGenerator::HashKeyVec> inputMap;
    inputMap[0].emplace_back(absl::flat_hash_set<uint64_t>({123456789}));
    inputMap[0].emplace_back(absl::flat_hash_set<uint64_t>({987654321}));
    KVBatchGetGenerator generator(options);
    generator.setPartIdToHashKeyMap(inputMap);

    multi_call::PartIdTy partCnt = 1;
    multi_call::PartRequestMap requestMap;
    generator.generate(partCnt, requestMap);

    ASSERT_EQ(1, requestMap.size());
    auto message = dynamic_cast<suez::KVBatchGetRequest *>(
        dynamic_pointer_cast<GigKVBatchGetRequest>(requestMap[0])->serializeMessage());

    ASSERT_EQ(options.indexName, message->indexname());
    ASSERT_EQ(options.tableName, message->tablename());
    ASSERT_EQ(options.timeout, message->timeoutinus());
    ASSERT_EQ(options.attrs[0], message->attrs(0));
    ASSERT_EQ(*(inputMap[0][0].begin()), message->inputkeys(0).hashkeys(0));
    ASSERT_EQ(*(inputMap[0][1].begin()), message->inputkeys(1).hashkeys(0));
}

} // namespace suez
