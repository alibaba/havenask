#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/builtin/InputTableTvfFunc.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);

class InputTableTvfFuncTest : public OpTestBase {
public:
    void setUp() {
        _context.queryPool = &_pool;
    }

    void tearDown() {
        _pool.reset();
    }

    void assertInputTableInfo(const vector<vector<vector<string>>>& expect) {
        ASSERT_EQ(expect, _tvfFunc._inputTableInfo);
    }

    TablePtr prepareInputTable(const shared_ptr<mem_pool::Pool>& pool) {
        TablePtr table;
        table.reset(new Table(pool));
        table->declareColumn("signed int8", bt_int8, false);
        table->declareColumn("signed int16", bt_int16, false);
        table->declareColumn("signed int32", bt_int32, false);
        table->declareColumn("signed int64", bt_int64, true);
        table->declareColumn("unsigned int8", bt_uint8, false);
        table->declareColumn("unsigned int16", bt_uint16, false);
        table->declareColumn("unsigned int32", bt_uint32, true);
        table->declareColumn("unsigned int64", bt_uint64, false);
        table->declareColumn("double", bt_double, true);
        table->declareColumn("float", bt_float, false);
        table->declareColumn("string", bt_string, true);
        return table;
    }

    void assertCompute(bool expect, TablePtr& output) {
        shared_ptr<mem_pool::Pool> pool1(new mem_pool::Pool());
        TablePtr input = prepareInputTable(pool1);
        TablePtr tmp;
        ASSERT_EQ(expect, _tvfFunc.compute(input, true, tmp));
        shared_ptr<mem_pool::Pool> pool2(new mem_pool::Pool());
        output = prepareInputTable(pool2);
        output->copyTable(tmp);
    }
protected:
    mem_pool::Pool _pool;
    TvfFuncInitContext _context;
    InputTableTvfFunc _tvfFunc;
};

TEST_F(InputTableTvfFuncTest, testInitErrorParamNumber) {
    _context.params = {"1", "2"};
    ASSERT_FALSE(_tvfFunc.init(_context));
}

TEST_F(InputTableTvfFuncTest, testInitEmptyInput) {
    _context.params = {""};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {""} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitSingleRowCase1) {
    _context.params = {"1"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitSingleRowCase2) {
    _context.params = {"1:2"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1", "2"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitSingleRowCase3) {
    _context.params = {"1,2:22,3:33:333"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"}, {"2", "22"}, {"3", "33", "333"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitSingleRowCase4) {
    _context.params = {"1,,3"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"}, {""}, {"3"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitSingleRowCase5) {
    _context.params = {"1,2,"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"}, {"2"}, {""} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitMultiRowCase1) {
    _context.params = {"1;2;3"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"} },
            { {"2"} },
            { {"3"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitMultiRowCase2) {
    _context.params = {"1;;3"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"} },
            { {""} },
            { {"3"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitMultiRowCase3) {
    _context.params = {"1;2;"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"} },
            { {"2"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitMultiRowCase4) {
    _context.params = {"1;2;;"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"1"} },
            { {"2"} },
            { {""} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitMultiRowCase5) {
    _context.params = {";;1"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {""} },
            { {""} },
            { {"1"} },
        });
}

TEST_F(InputTableTvfFuncTest, testInitMultiRowCase6) {
    _context.params = {"11:12:13,14:15,16;21,22:23,24;31,32,33"};
    ASSERT_TRUE(_tvfFunc.init(_context));
    assertInputTableInfo(
        {
            { {"11", "12", "13"}, {"14", "15"}, {"16"} },
            { {"21"}, {"22", "23"}, {"24"} },
            { {"31"}, {"32"}, {"33"} },
        });
}

TEST_F(InputTableTvfFuncTest, testColNumNotMatch) {
    _context.params = {"1,2,3;4,5"};
    ASSERT_FALSE(_tvfFunc.init(_context));
}

TEST_F(InputTableTvfFuncTest, testMultiColEmptyRow) {
    _context.params = {"1,2;;3,4"};
    ASSERT_FALSE(_tvfFunc.init(_context));
}

TEST_F(InputTableTvfFuncTest, testComputeSuccess) {
    _context.params = {
        "-1,,-3,-4:-5,6,7,8:9,10,11.1:12.2,13.3,test1::test3;"
        "-2,-4,-6,-8:-10,12,14,16:18,20,22.2:,26.6,:test5:test6;"
    };
    ASSERT_TRUE(_tvfFunc.init(_context));
    TablePtr output;
    assertCompute(true, output);
    ASSERT_TRUE(output.get() != nullptr);
    checkOutputColumn<int8_t>(output, "signed int8", {-1, -2});
    checkOutputColumn<int16_t>(output, "signed int16", {0,-4});
    checkOutputColumn<int32_t>(output, "signed int32", {-3,-6});
    checkOutputMultiColumn<int64_t>(output, "signed int64", {{-4,-5}, {-8,-10}});
    checkOutputColumn<uint8_t>(output, "unsigned int8", {6,12});
    checkOutputColumn<uint16_t>(output, "unsigned int16", {7,14});
    checkOutputMultiColumn<uint32_t>(output, "unsigned int32", {{8,9}, {16,18}});
    checkOutputColumn<uint64_t>(output, "unsigned int64", {10,20});
    checkOutputMultiColumn<double>(output, "double", {{11.1,12.2}, {22.2,0.0}});
    checkOutputColumn<float>(output, "float", {13.3,26.6});
    checkOutputMultiColumn<string>(output, "string", {{"test1", "", "test3"}, {"", "test5", "test6"}});
}

TEST_F(InputTableTvfFuncTest, testComputeMismatchColumnCount) {
    _context.params = {
        "-1,-2,-3,-4:-5,6,7,8:9,10,11.1:12.2,13.3"
    };
    ASSERT_TRUE(_tvfFunc.init(_context));
    TablePtr output;
    assertCompute(false, output);
}

END_HA3_NAMESPACE();
