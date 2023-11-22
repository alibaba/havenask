#include "suez/sdk/RemoteTableWriter.h"

#include "multi_call/interface/QuerySession.h"
#include "multi_call/interface/Reply.h"
#include "suez/sdk/RemoteTableWriterClosure.h"
#include "suez/sdk/RemoteTableWriterRequestGenerator.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace autil;
using namespace std;
using namespace multi_call;

namespace suez {

class MockSearchService : public multi_call::SearchService {
public:
    MOCK_METHOD2(search, void(const SearchParam &param, ReplyPtr &reply));
};

class RemoteTableWriterTest : public TESTBASE {};

TEST_F(RemoteTableWriterTest, testWrite) {
    MockSearchService searchService;
    RemoteTableWriter writer("zone", &searchService);
    multi_call::SearchParam searchParam;
    auto replyPtr = make_shared<Reply>();
    EXPECT_CALL(searchService, search(_, _)).WillOnce(DoAll(SaveArg<0>(&searchParam), SetArgReferee<1>(replyPtr)));
    MessageWriteParam writeParam;
    writeParam.docs = {{1, "doc1"}, {3, "doc2"}};
    writeParam.cb = [](Result<WriteCallbackParam> result) { ASSERT_TRUE(false); };
    writeParam.timeoutUs = 1000;
    writer.write("table1", "ha3", std::move(writeParam));
    ASSERT_EQ(1, searchParam.generatorVec.size());
    auto *generator = dynamic_cast<RemoteTableWriterRequestGenerator *>(searchParam.generatorVec[0].get());
    ASSERT_NE(nullptr, generator);
    const auto &param = generator->_param;
    ASSERT_EQ("zone.table1.write", param.bizName);
    ASSERT_EQ("table1", param.tableName);
    ASSERT_EQ("ha3", param.format);
    ASSERT_THAT(param.docs, ElementsAre(Pair(1, "doc1"), Pair(3, "doc2")));
    ASSERT_EQ(1000, param.timeoutUs);
    auto *closure = dynamic_cast<RemoteTableWriterClosure *>(searchParam.closure);
    ASSERT_NE(nullptr, closure);
    ASSERT_EQ(closure->getReplyPtr(), replyPtr);
    delete searchParam.closure;
}

TEST_F(RemoteTableWriterTest, testFollowWrite) {
    MockSearchService searchService;
    RemoteTableWriter writer("zone", &searchService, true);
    multi_call::SearchParam searchParam;
    auto replyPtr = make_shared<Reply>();
    EXPECT_CALL(searchService, search(_, _)).WillOnce(DoAll(SaveArg<0>(&searchParam), SetArgReferee<1>(replyPtr)));
    MessageWriteParam writeParam;
    writeParam.cb = [](Result<WriteCallbackParam> result) { ASSERT_TRUE(false); };
    writeParam.timeoutUs = 1000;
    writer.write("table1", "ha3", std::move(writeParam));
    ASSERT_EQ(1, searchParam.generatorVec.size());
    auto *generator = dynamic_cast<RemoteTableWriterRequestGenerator *>(searchParam.generatorVec[0].get());
    ASSERT_NE(nullptr, generator);
    auto &tagMap = *generator->_tags;
    auto iter = tagMap.find(GeneratorDef::LEADER_TAG);
    ASSERT_NE(tagMap.end(), iter);
    ASSERT_EQ(multi_call::TMT_PREFER, iter->second.type);
    auto *closure = dynamic_cast<RemoteTableWriterClosure *>(searchParam.closure);
    ASSERT_NE(nullptr, closure);
    ASSERT_EQ(closure->getReplyPtr(), replyPtr);
    delete searchParam.closure;
}

} // namespace suez
