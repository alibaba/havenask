#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/search/test/AggregateBase.h"
#include <ha3/search/NormalAggregator.h>
#include <ha3/search/BatchAggregator.h>

using namespace std;
using namespace testing;
using testing::Types;
using namespace suez::turing;
using namespace autil;
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

template <typename T>
class NormalAggregatorUtilTest : public TESTBASE, public AggregateBase {
public:
    NormalAggregatorUtilTest() {
    }
    ~NormalAggregatorUtilTest() {}
public:
    void setUp() {
        baseSetUp();
        _keyAttributeExpr = initKeyExpression<int32_t>("userid");
        _agg = new T((AttributeExpressionTyped<int32_t>*)_keyAttributeExpr,
                     1, _pool, 1, 2, 3);
    }
    void tearDown() {
        DELETE_AND_SET_NULL(_agg);
        baseTearDown();
    }

private:
    T *_agg;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(search, NormalAggregatorUtilTest, T);

typedef Types<BatchAggregator<int, int, DenseMapTraits<int>::GroupMapType>,
              BatchAggregator<int, int, UnorderedMapTraits<int>::GroupMapType>,
              NormalAggregator<int, int, DenseMapTraits<int>::GroupMapType>,
              NormalAggregator<int, int, UnorderedMapTraits<int>::GroupMapType>
              > Implementations;
// use macro TYPED_TEST_SUITE when gtest(version) > 1.8.0
TYPED_TEST_CASE(NormalAggregatorUtilTest, Implementations);

TYPED_TEST(NormalAggregatorUtilTest, testGetIdxStr) {
    auto normal = this->_agg;

    for (int i = 0; i < 20; i++) {
        ASSERT_TRUE(normal->getIdxStr(i) == std::to_string(i));
    }
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggItemSize) {

    auto normal = this->_agg;
    normal->_funVector.push_back(POOL_NEW_CLASS(this->_pool, CountAggregateFunction));
    ASSERT_TRUE(normal->getCavaAggItemSize() == "16");

    normal->_funVector.push_back(POOL_NEW_CLASS(this->_pool, CountAggregateFunction));
    ASSERT_TRUE(normal->getCavaAggItemSize() == "24");

    normal->_funVector.push_back(POOL_NEW_CLASS(this->_pool, CountAggregateFunction));
    ASSERT_TRUE(normal->getCavaAggItemSize() == "32");

    normal->_funVector.push_back(POOL_NEW_CLASS(this->_pool, CountAggregateFunction));
    ASSERT_TRUE(normal->getCavaAggItemSize() == "40");

    normal->_funVector.push_back(POOL_NEW_CLASS(this->_pool, CountAggregateFunction));
    ASSERT_EQ(normal->getCavaAggItemSize(), "48");

}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggRefName) {
    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, CountAggregateFunction);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggRefName(func, std::to_string(i));
            ASSERT_EQ(output, std::string("countRef") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, SumAggregateFunction<int>, "sum", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggRefName(func, std::to_string(i));
            ASSERT_EQ(output, std::string("sumRef") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, MinAggregateFunction<int>, "min", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggRefName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("minRef") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, MaxAggregateFunction<int>, "max", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggRefName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("maxRef") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggExprName) {
    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, CountAggregateFunction);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("countExpr") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, SumAggregateFunction<int>, "sum", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("sumExpr") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, MinAggregateFunction<int>, "min", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("minExpr") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, MaxAggregateFunction<int>, "max", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("maxExpr") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggItemField) {
    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, CountAggregateFunction);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("count") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, SumAggregateFunction<int>, "sum", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("sum") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, MinAggregateFunction<int>, "min", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("min") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }

    {
        auto normal = this->_agg;
        auto func = POOL_NEW_CLASS(this->_pool, MaxAggregateFunction<int>, "max", vt_int8, NULL);
        normal->_funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal->getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("max") + std::to_string(i));
        }
        normal->_funVector.clear();
        POOL_DELETE_CLASS(func);
    }
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggExprGetFunc) {
    auto normal = this->_agg;
#define CASE(vt_type, isMulti, ret, funName)                            \
    {                                                                   \
        std::string output;                                             \
        ASSERT_TRUE(normal->getCavaAggExprGetFunc(output, vt_type, isMulti) == ret); \
        if (ret) {                                                      \
            ASSERT_EQ(std::string(#funName), output);                   \
        }                                                               \
    }

    CASE(vt_int8, false, true, getInt8);
    CASE(vt_int16, false, true, getInt16);
    CASE(vt_int32, false, true, getInt32);
    CASE(vt_int64, false, true, getInt64);
    CASE(vt_uint8, false, true, getUInt8);
    CASE(vt_uint16, false, true, getUInt16);
    CASE(vt_uint32, false, true, getUInt32);
    CASE(vt_uint64, false, true, getUInt64);
    CASE(vt_float, false, true, getFloat);
    CASE(vt_double, false, true, getDouble);
    CASE(vt_string, false, true, getMChar);
    CASE(vt_unknown, false, false, unknow);

    CASE(vt_int8, true, true, getMInt8);
    CASE(vt_int16, true, true, getMInt16);
    CASE(vt_int32, true, true, getMInt32);
    CASE(vt_int64, true, true, getMInt64);
    CASE(vt_uint8, true, true, getMUInt8);
    CASE(vt_uint16, true, true, getMUInt16);
    CASE(vt_uint32, true, true, getMUInt32);
    CASE(vt_uint64, true, true, getMUInt64);
    CASE(vt_float, true, true, getMFloat);
    CASE(vt_double, true, true, getMDouble);
    CASE(vt_string, true, true, getMString);
    CASE(vt_unknown, true, false, unknow);

#undef CASE
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggRefSetFunc) {
    auto normal = this->_agg;
#define CASE(vt_type, isMulti, ret, funName)                            \
    {                                                                   \
        std::string output;                                             \
        matchdoc::ValueType valueType;                                  \
        valueType.setBuiltinType(vt_type);                              \
        valueType.setMultiValue(isMulti);                               \
        valueType.setBuiltin();                                         \
        valueType.setStdType(true);                                     \
        ASSERT_TRUE(normal->getCavaAggRefSetFunc(output, valueType) == ret); \
        if (ret) {                                                      \
            ASSERT_TRUE(std::string(#funName) == output);               \
        }                                                               \
    }

    CASE(vt_int8, false, true, setInt8);
    CASE(vt_int16, false, true, setInt16);
    CASE(vt_int32, false, true, setInt32);
    CASE(vt_int64, false, true, setInt64);
    CASE(vt_uint8, false, true, setUInt8);
    CASE(vt_uint16, false, true, setUInt16);
    CASE(vt_uint32, false, true, setUInt32);
    CASE(vt_uint64, false, true, setUInt64);
    CASE(vt_float, false, true, setFloat);
    CASE(vt_double, false, true, setDouble);
    CASE(vt_string, false, true, setSTLString);
    CASE(vt_unknown, false, false, unknow);

    CASE(vt_int8, true, false, setMInt8);
    CASE(vt_int16, true, false, setMInt16);
    CASE(vt_int32, true, false, setMInt32);
    CASE(vt_int64, true, false, setMInt64);
    CASE(vt_uint8, true, false, setMUInt8);
    CASE(vt_uint16, true, false, setMUInt16);
    CASE(vt_uint32, true, false, setMUInt32);
    CASE(vt_uint64, true, false, setMUInt64);
    CASE(vt_float, true, false, setMFloat);
    CASE(vt_double, true, false, setMDouble);
    CASE(vt_string, true, false, setMString);
    CASE(vt_unknown, true, false, unknow);

#undef CASE
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaAggItemMapType) {
    auto normal = this->_agg;
#define CASE(vt_type, ret, funName)                                     \
    {                                                                   \
        std::string output;                                             \
        ASSERT_TRUE(normal->getCavaAggItemMapType(output, vt_type) == ret); \
        if (ret) {                                                      \
            ASSERT_TRUE(std::string(#funName) == output);               \
        }                                                               \
    }

    CASE(vt_int8, true, ByteAggItemMap);
    CASE(vt_int16, true, ShortAggItemMap);
    CASE(vt_int32, true, IntAggItemMap);
    CASE(vt_int64, true, LongAggItemMap);
    CASE(vt_uint8, true, UByteAggItemMap);
    CASE(vt_uint16, true, UShortAggItemMap);
    CASE(vt_uint32, true, UIntAggItemMap);
    CASE(vt_uint64, true, ULongAggItemMap);
    CASE(vt_float, true, FloatAggItemMap);
    CASE(vt_double, true, DoubleAggItemMap);
    CASE(vt_string, true, MCharAggItemMap);
    CASE(vt_unknown, false, unknow);

#undef CASE
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaType) {
    auto normal = this->_agg;
#define CASE(vt_type, isMulti, ret, funName)                            \
    {                                                                   \
        std::string output;                                             \
        ASSERT_TRUE(normal->getCavaType(output, vt_type, isMulti) == ret); \
        if (ret) {                                                      \
            ASSERT_TRUE(std::string(#funName) == output);               \
        }                                                               \
    }

    CASE(vt_int8, false, true, byte);
    CASE(vt_int16, false, true, short);
    CASE(vt_int32, false, true, int);
    CASE(vt_int64, false, true, long);
    CASE(vt_uint8, false, true, ubyte);
    CASE(vt_uint16, false, true, ushort);
    CASE(vt_uint32, false, true, uint);
    CASE(vt_uint64, false, true, ulong);
    CASE(vt_float, false, true, float);
    CASE(vt_double, false, true, double);
    CASE(vt_string, false, true, MChar);
    CASE(vt_unknown, false, false, unknow);

    CASE(vt_int8, true, true, MInt8);
    CASE(vt_int16, true, true, MInt16);
    CASE(vt_int32, true, true, MInt32);
    CASE(vt_int64, true, true, MInt64);
    CASE(vt_uint8, true, true, MUInt8);
    CASE(vt_uint16, true, true, MUInt16);
    CASE(vt_uint32, true, true, MUInt32);
    CASE(vt_uint64, true, true, MUInt64);
    CASE(vt_float, true, true, MFloat);
    CASE(vt_double, true, true, MDouble);
    CASE(vt_string, true, true, MString);
    CASE(vt_unknown, true, false, unknow);

#undef CASE
}

TYPED_TEST(NormalAggregatorUtilTest, testGetCavaMinMaxStr) {
    auto normal = this->_agg;

#define CASE(bt_type, type, retValue)                                  \
    {                                                                   \
        std::string outputMin;                                          \
        std::string outputMax;                                          \
        matchdoc::ValueType vt;                                         \
        vt.setBuiltinType(bt_type);                                     \
        vt.setBuiltin();                                                \
        bool ret = normal->getCavaAggMinStr(outputMin, vt);              \
        ret |= normal->getCavaAggMaxStr(outputMax, vt);                  \
        ASSERT_TRUE(ret == retValue);                                   \
        if (ret) {                                                      \
            std::string suffix;                                         \
            if (bt_type == vt_float) {                                  \
                suffix = "F";                                           \
            } else if (bt_type == vt_double) {                          \
                suffix = "D";                                           \
            }                                                           \
            ASSERT_EQ(outputMin, std::to_string(util::NumericLimits<type>::min()) + suffix); \
            ASSERT_EQ(outputMax, std::to_string(util::NumericLimits<type>::max()) + suffix); \
        }                                                               \
    }

    CASE(vt_int8, int8_t, true);
    CASE(vt_int16, int16_t, true);
    CASE(vt_int32, int32_t, true);
    CASE(vt_uint8, uint8_t, true);
    CASE(vt_uint16, uint16_t, true);
    CASE(vt_float, float, true);
    CASE(vt_double, double, true);

    // CASE(vt_int64, int64_t, true);
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_int64);
        vt.setBuiltin();
        bool ret = normal->getCavaAggMinStr(outputMin, vt);
        ret |= normal->getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == true);
        if (ret) {
            ASSERT_EQ(outputMin, "-(1L<<63)");
            ASSERT_EQ(outputMax, std::to_string(util::NumericLimits<int64_t>::max()) + "L");
        }
    }
    //CASE(vt_uint32, uint32_t, true);
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_uint32);
        vt.setBuiltin();
        bool ret = normal->getCavaAggMinStr(outputMin, vt);
        ret |= normal->getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == true);
        if (ret) {
            ASSERT_EQ(outputMin, std::to_string(util::NumericLimits<uint32_t>::min()));
            ASSERT_EQ(outputMax, "-1");
        }
    }
    // CASE(vt_uint64, uint64_t, true);
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_uint64);
        vt.setBuiltin();
        bool ret = normal->getCavaAggMinStr(outputMin, vt);
        ret |= normal->getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == true);
        if (ret) {
            ASSERT_EQ(outputMin, std::to_string(util::NumericLimits<uint64_t>::min()));
            ASSERT_EQ(outputMax, "-1L");
        }
    }
    // test MultiChar
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_string);
        vt.setBuiltin();
        bool ret = normal->getCavaAggMinStr(outputMin, vt);
        ASSERT_TRUE(ret == false);
        ret = normal->getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == false);
    }

}

END_HA3_NAMESPACE();
