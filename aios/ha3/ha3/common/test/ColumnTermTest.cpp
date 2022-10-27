#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <autil/MultiValueCreator.h>
#include "ha3/common/ColumnTerm.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;

BEGIN_HA3_NAMESPACE(common);

class ColumnTermTest : public TESTBASE {
public:
    void setUp() {
    }

    void tearDown() {
    }

    template <class T>
    void expectGetValueType(BuiltinType type) {
        ColumnTermTyped<T> term;
        EXPECT_EQ(type, term.getValueType());
    }

    template <class T>
    void expectSerialize(const vector<size_t> &offsets, const vector<T> &values) {
        ColumnTermTyped<T> term("title");
        term.getOffsets() = offsets;
        term.getValues() = values;
        mem_pool::Pool pool;
        checkSerialize(term, pool);
    }   

    void checkSerialize(const ColumnTerm &term, mem_pool::Pool &pool) {
        string data = serializeTerm(term, pool);
        unique_ptr<ColumnTerm> p(deserializeTerm(data, pool));
        ASSERT_TRUE(p != nullptr);
        EXPECT_TRUE(*p == term);
    }

    string serializeTerm(const ColumnTerm &term, mem_pool::Pool &pool) {
        DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &pool);
        ColumnTerm::serialize(&term, dataBuffer);
        return string(dataBuffer.getData(), dataBuffer.getDataLen());
    }

    ColumnTerm* deserializeTerm(const string& data, mem_pool::Pool &pool) {
        DataBuffer dataBuffer((void*)data.data(), data.size(), &pool);
        return ColumnTerm::deserialize(dataBuffer);
    }
private:
    HA3_LOG_DECLARE();
};

template<>
void ColumnTermTest::expectSerialize<string>(const vector<size_t> &offsets, const vector<string> &values) {
    vector<MultiChar> newValues(values.size());
    mem_pool::Pool pool;
    for (size_t i = 0; i < values.size(); ++i) {
        const auto &str = values[i];
        char *p = MultiValueCreator::createMultiValueBuffer<char>(
            str.data(), str.size(), &pool);
        MultiChar value(p);
        newValues[i] = value;
    }
    ColumnTermTyped<MultiChar> term("title");
    term.getOffsets() = offsets;
    term.getValues() = newValues;
    checkSerialize(term, pool);
}

HA3_LOG_SETUP(common, ColumnTermTest);

TEST_F(ColumnTermTest, testConstructor) {
    ColumnTermTyped<int64_t> term("title");
    EXPECT_EQ("title", term.getIndexName());
    EXPECT_TRUE(term.getOffsets().empty());
}

TEST_F(ColumnTermTest, testEqual) {
    ColumnTermTyped<int64_t> term("title");
    EXPECT_TRUE(term == term);
    {
        ColumnTermTyped<int32_t> other("title");
        EXPECT_FALSE(term == other);
    }
    {
        ColumnTermTyped<int64_t> other("xxx");
        EXPECT_FALSE(term == other);
    }
    term.getValues() = {111, 222, 333};
    {
        ColumnTermTyped<int64_t> other("title");
        other.getValues() = {111, 222};
        EXPECT_FALSE(term == other);
    }
    term.getOffsets() = {0, 2, 3};
    {
        ColumnTermTyped<int64_t> other("title");
        other.getValues() = term.getValues();
        EXPECT_FALSE(term == other);
    }
    {
        ColumnTermTyped<int64_t> other("title");
        other.getValues() = term.getValues();
        other.getOffsets() = term.getOffsets();
        EXPECT_TRUE(term == other);
    }
}

TEST_F(ColumnTermTest, testClone) {
    ColumnTermTyped<int64_t> term("title");
    unique_ptr<ColumnTerm> otherPtr(term.clone());
    EXPECT_FALSE(otherPtr.get() == &term);
    EXPECT_TRUE(*otherPtr == term);
    EXPECT_TRUE(term.getEnableCache());
    term.setEnableCache(false);
    EXPECT_FALSE(term.getEnableCache());
    EXPECT_FALSE(*otherPtr == term);
    otherPtr.reset(term.clone());
    EXPECT_FALSE(otherPtr.get() == &term);
    EXPECT_TRUE(*otherPtr == term);
}

TEST_F(ColumnTermTest, testToString) {
    ColumnTermTyped<int64_t> term("title");
    term.getValues() = {111, 222, 333};
    term.getOffsets() = {0, 2, 3};
    EXPECT_EQ("ColumnTerm:[0,2,3,]:[title:111,222,333,]", term.toString());
}

TEST_F(ColumnTermTest, testGetValueType) {
    expectGetValueType<uint64_t>(bt_uint64);
    expectGetValueType<int64_t>(bt_int64);
    expectGetValueType<uint32_t>(bt_uint32);
    expectGetValueType<int32_t>(bt_int32);
    expectGetValueType<uint16_t>(bt_uint16);
    expectGetValueType<int16_t>(bt_int16);
    expectGetValueType<uint8_t>(bt_uint8);
    expectGetValueType<int8_t>(bt_int8);
    expectGetValueType<float>(bt_float);
    expectGetValueType<double>(bt_double);
    expectGetValueType<MultiChar>(bt_string);
}

TEST_F(ColumnTermTest, testSerialize) {
    expectSerialize<uint64_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<int64_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<uint32_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<int32_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<uint16_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<int16_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<uint8_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<int8_t>({0, 2, 3}, {11, 22, 33});
    expectSerialize<float>({0, 2, 3}, {11, 22, 33});
    expectSerialize<double>({0, 2, 3}, {11, 22, 33});
    expectSerialize<string>({0, 2, 3}, {"111", "222", "333"});
}

END_HA3_NAMESPACE();

