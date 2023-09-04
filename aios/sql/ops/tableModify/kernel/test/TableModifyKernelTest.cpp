#include "sql/ops/tableModify/kernel/TableModifyKernel.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "autil/Span.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/result/ForwardList.h"
#include "navi/common.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/resource/MessageWriterManager.h"
#include "sql/resource/MessageWriterManagerR.h"
#include "sql/resource/SwiftMessageWriter.h"
#include "sql/resource/SwiftMessageWriterManager.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/helper/CheckpointBuffer.h"
#include "swift/client/helper/SwiftWriterAsyncHelper.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/protocol/Common.pb.h"
#include "swift/testlib/MockSwiftWriterAsyncHelper.h"
#include "table/Table.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace navi;
using namespace table;
using namespace swift::common;
using namespace swift::testlib;
using namespace swift::protocol;
using namespace swift::client;
using namespace autil::legacy;
using namespace autil::result;
using namespace autil::legacy::json;

namespace sql {

class TableModifyKernelTest : public OpTestBase {
public:
    void SetUp() override;

private:
    void prepareSwiftMessageWriterManager();
    void prepareAttributeMap();
    KernelTesterPtr buildTester(KernelTesterBuilder &builder);

private:
    MockSwiftWriterAsyncHelper *_mockSwiftHelper = nullptr;
};

void TableModifyKernelTest::SetUp() {
    _tableName = "test_table";
    OpTestBase::SetUp();
    ASSERT_NO_FATAL_FAILURE(prepareSwiftMessageWriterManager());
}

KernelTesterPtr TableModifyKernelTest::buildTester(KernelTesterBuilder &builder) {
    setResource(builder);
    builder.kernel("sql.TableModifyKernel");
    builder.output("output0");
    builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
    return builder.build();
}

void TableModifyKernelTest::prepareAttributeMap() {
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["db_name"] = string("zone");
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["pk_field"] = string("buyer_id");
    _attributeMap["operation"] = string("INSERT");
    _attributeMap["table_distribution"] = ParseJson(R"json({
    "hash_mode" : {"hash_function" : "HASH", "hash_field":"$buyer_id"}
})json");
    _attributeMap["modify_field_exprs"] = ParseJson(R"json({
    "$modify_time" : "1657274727",
    "$title" : "hello world",
    "$buyer_id" : 12
})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$ROWCOUNT"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT"])json");
}

void TableModifyKernelTest::prepareSwiftMessageWriterManager() {
    auto mockSwiftHelper = make_shared<MockSwiftWriterAsyncHelper>();
    SwiftMessageWriterPtr swiftMessageWriter(new SwiftMessageWriter(mockSwiftHelper));
    auto *manager = new SwiftMessageWriterManager();
    manager->_swiftWriterMap = {{_tableName, swiftMessageWriter}};
    auto managerR = make_shared<MessageWriterManagerR>();
    managerR->_messageWriterManager.reset(manager);
    _naviRHelper.addExternalRes(managerR);
    _mockSwiftHelper = mockSwiftHelper.get();
}

void checkKV(const FieldGroupReader &reader, const string &key, const string &expect) {
    auto *field = reader.getField(key);
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(expect, field->value.to_string());
    ASSERT_TRUE(field->isUpdated);
}

// TEST_F(TableModifyKernelTest, testParseToMessageInfo_Success) {
//     TableModifyKernel kernel;
//     kernel._initParam.tableDist.hashMode._hashFields.push_back("$buyer_id");
//     kernel._initParam.tableDist.hashMode._hashFunction = "HASH";
//     kernel._kernelPool.reset(new autil::mem_pool::PoolAsan);
//     std::string modifyFieldExprsJson = R"json({
//     "$modify_time" : "1657274727",
//     "$title" : "hello world",
//     "$buyer_id" : 12
// })json";
//     MessageInfo messageInfo;
//     NaviLoggerProvider provider("TRACE3");
//     ASSERT_TRUE(kernel.parseToMessageInfo(modifyFieldExprsJson, false, messageInfo));
//     ASSERT_EQ(autil::HashFuncFactory::createHashFunc("HASH")->getHashId("12"),
//     messageInfo.uint16Payload); ASSERT_NE("", messageInfo.data); FieldGroupReader reader;
//     ASSERT_TRUE(reader.fromProductionString(messageInfo.data));
//     ASSERT_EQ(4, reader.getFieldSize());
//     ASSERT_NO_FATAL_FAILURE(checkKV(reader, MESSAGE_KEY_CMD, MESSAGE_CMD_ADD));
//     ASSERT_NO_FATAL_FAILURE(checkKV(reader, "modify_time", "1657274727"));
//     ASSERT_NO_FATAL_FAILURE(checkKV(reader, "title", "hello world"));
//     ASSERT_NO_FATAL_FAILURE(checkKV(reader, "buyer_id", "12"));
// }

TEST_F(TableModifyKernelTest, testAll_Success) {
    ASSERT_NO_FATAL_FAILURE(prepareAttributeMap());
    EXPECT_CALL(*_mockSwiftHelper, write(_, _, _, _))
        .WillOnce(testing::Invoke([](MessageInfo *msgInfos,
                                     size_t count,
                                     SwiftWriteCallbackItem::WriteCallbackFunc callback,
                                     int64_t timeout) {
            callback(SwiftWriteCallbackParam {nullptr, count});
        }));
    KernelTesterBuilder builder;
    builder.logLevel("TRACE3");
    auto tester = buildTester(builder);
    ASSERT_NE(nullptr, tester);

    TablePtr table;
    bool eof = false;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernel(*tester, table, eof));
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(table,
                                                                      "ROWCOUNT",
                                                                      {
                                                                          1,
                                                                      }));
    ASSERT_TRUE(eof);
}

} // namespace sql
