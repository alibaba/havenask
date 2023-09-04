#include "sql/resource/TableMessageWriter.h"

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/NoCopyable.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "sql/resource/MessageWriter.h"
#include "suez/sdk/RemoteTableWriter.h"
#include "suez/sdk/RemoteTableWriterParam.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace suez;
using namespace autil;
using namespace autil::result;

namespace sql {

class MockRemoteTableWriter : public suez::RemoteTableWriter {
public:
    MockRemoteTableWriter()
        : suez::RemoteTableWriter("", nullptr) {}

public:
    MOCK_METHOD3(write,
                 void(const std::string &tableName,
                      const std::string &format,
                      MessageWriteParam param));
};

class TableMessageWriterTest : public TESTBASE {};

TEST_F(TableMessageWriterTest, testWriteNull) {
    RemoteTableWriterPtr remoteWriter;
    TableMessageWriter writer("table", remoteWriter);
    autil::Notifier notifier;
    MessageWriteParam param;
    param.cb = [&](Result<suez::WriteCallbackParam> result) {
        [&]() {
            ASSERT_FALSE(result.is_ok());
            ASSERT_EQ("remote table message writer is empty!", result.get_error().message());
        }();
        notifier.notifyExit();
    };
    param.timeoutUs = 1000;
    writer.write(param);
    notifier.waitNotification();
}

TEST_F(TableMessageWriterTest, testWriteSuccess) {
    auto mockWriter = make_shared<MockRemoteTableWriter>();
    TableMessageWriter writer("table1", mockWriter);
    string tableName;
    string format;
    MessageWriteParam inParam;
    EXPECT_CALL(*mockWriter, write(_, _, _))
        .WillOnce(DoAll(SaveArg<0>(&tableName), SaveArg<1>(&format), SaveArg<2>(&inParam)));
    std::vector<std::pair<uint16_t, std::string>> inputs = {{1, "doc1"}, {2, "doc2"}};
    MessageWriteParam param;
    param.docs = inputs;
    param.cb = [](Result<suez::WriteCallbackParam> result) {};
    param.timeoutUs = 1000;
    writer.write(param);
    ASSERT_EQ("table1", tableName);
    ASSERT_EQ(TableMessageWriter::RAW_DOCUMENT_FORMAT_SWIFT, format);
    ASSERT_THAT(inParam.docs, ElementsAre(Pair(1, "doc1"), Pair(2, "doc2")));
    ASSERT_NE(nullptr, inParam.cb);
    ASSERT_EQ(1000, inParam.timeoutUs);
}

} // namespace sql
