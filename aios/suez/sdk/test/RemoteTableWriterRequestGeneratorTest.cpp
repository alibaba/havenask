#include "suez/sdk/RemoteTableWriterRequestGenerator.h"

#include "autil/RangeUtil.h"
#include "suez/service/Service.pb.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace autil;

namespace suez {

class RemoteTableWriterRequestGeneratorTest : public TESTBASE {
public:
    void setUp() {
        _param.bizName = "biz";
        _param.tableName = "table";
        _param.format = "ha3";
        _param.timeoutUs = 1000;
    }

private:
    RemoteTableWriterParam _param;
};

TEST_F(RemoteTableWriterRequestGeneratorTest, testCreatePartMap) {
    _param.docs = {{0, "doc1"}, {RangeUtil::MAX_PARTITION_RANGE, "doc3"}, {RangeUtil::MAX_PARTITION_RANGE / 2, "doc2"}};
    RemoteTableWriterRequestGenerator generator(_param);
    PartDocMap partMap;
    generator.createPartMap(8, partMap);
    ASSERT_THAT(partMap,
                ElementsAre(Pair(0, ElementsAre(Pair(0, "doc1"))),
                            Pair(3, ElementsAre(Pair(RangeUtil::MAX_PARTITION_RANGE / 2, "doc2"))),
                            Pair(7, ElementsAre(Pair(RangeUtil::MAX_PARTITION_RANGE, "doc3")))));
}

TEST_F(RemoteTableWriterRequestGeneratorTest, testCreateWriteRequset) {
    std::vector<WriteDoc> docs = {
        {0, "doc1"}, {RangeUtil::MAX_PARTITION_RANGE, "doc3"}, {RangeUtil::MAX_PARTITION_RANGE / 2, "doc2"}};
    RemoteTableWriterRequestGenerator generator(_param);
    multi_call::RequestPtr requestPtr = generator.createWriteRequest(docs);
    auto *request = dynamic_cast<RemoteTableWriterRequest *>(requestPtr.get());
    ASSERT_NE(nullptr, request);
    auto *writeReq = dynamic_cast<suez::WriteRequest *>(request->_message);
    ASSERT_NE(nullptr, writeReq);
    ASSERT_EQ(_param.tableName, writeReq->tablename());
    ASSERT_EQ(_param.format, writeReq->format());
    ASSERT_EQ(3, writeReq->writes_size());
}

TEST_F(RemoteTableWriterRequestGeneratorTest, testGenerate) {
    _param.docs = {{0, "doc1"},
                   {RangeUtil::MAX_PARTITION_RANGE, "doc3"},
                   {RangeUtil::MAX_PARTITION_RANGE / 2, "doc2"},
                   {1, "doc4"}};
    RemoteTableWriterRequestGenerator generator(_param);
    multi_call::PartRequestMap requestMap;
    generator.generate(8, requestMap);
    ASSERT_THAT(requestMap, ElementsAre(Pair(0, Ne(nullptr)), Pair(3, Ne(nullptr)), Pair(7, Ne(nullptr))));
}

TEST_F(RemoteTableWriterRequestGeneratorTest, testSetLeaderTags) {
    RemoteTableWriterRequestGenerator generator(_param);
    generator.setLeaderTags(multi_call::TMT_PREFER);
    auto &tagMap = *generator._tags;
    auto iter = tagMap.find(GeneratorDef::LEADER_TAG);
    ASSERT_NE(tagMap.end(), iter);
    ASSERT_EQ(multi_call::TMT_PREFER, iter->second.type);
}

} // namespace suez
