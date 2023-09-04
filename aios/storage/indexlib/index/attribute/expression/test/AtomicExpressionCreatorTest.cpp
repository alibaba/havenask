#include "indexlib/index/attribute/expression/AtomicExpressionCreator.h"

#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/expression/AtomicAttributeExpression.h"
#include "indexlib/index/attribute/expression/test/AttributeExpressionTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class AtomicExpressionCreatorTest : public TESTBASE
{
public:
    AtomicExpressionCreatorTest() = default;
    ~AtomicExpressionCreatorTest() = default;

    template <FieldType ft, bool isMulti>
    void InnerCreatorTest();

public:
    void setUp() override;
    void tearDown() override;
};

void AtomicExpressionCreatorTest::setUp() {}

void AtomicExpressionCreatorTest::tearDown() {}

TEST_F(AtomicExpressionCreatorTest, TestSimple)
{
    autil::mem_pool::Pool pool;
    expression::AttributeExpressionPool exprPool;
    AttributeExpressionTestHelper<ft_uint32, false> helper;
    ASSERT_TRUE(helper.Init());
    auto schema = std::shared_ptr<config::TabletSchema>(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema2.json").release());
    ASSERT_TRUE(schema);
    ASSERT_TRUE(schema->TEST_GetImpl()->AddIndexConfig(helper._config).IsOK());
    AtomicExpressionCreator creator(helper._segments, schema, &exprPool, &pool);
    ASSERT_EQ(0, creator.GetCreatedExpressionCount());
    expression::AttributeExpression* expr = creator.createAtomicExpr("attr_name", "");
    ASSERT_TRUE(expr);
    auto typedAttrExpression = dynamic_cast<AtomicAttributeExpression<uint32_t>*>(expr);
    ASSERT_TRUE(typedAttrExpression);
    ASSERT_EQ(1, creator.GetCreatedExpressionCount());
    AtomicAttributeExpression<uint32_t>* createdExpr =
        dynamic_cast<AtomicAttributeExpression<uint32_t>*>(creator.GetCreatedExpression(0));
    ASSERT_TRUE(createdExpr);
    ASSERT_EQ(typedAttrExpression, createdExpr);
}

template <FieldType ft, bool isMulti>
void AtomicExpressionCreatorTest::InnerCreatorTest()
{
    typedef typename indexlib::IndexlibFieldType2CppType<ft, isMulti>::CppType Cpp;
    autil::mem_pool::Pool pool;
    expression::AttributeExpressionPool exprPool;
    AttributeExpressionTestHelper<ft, isMulti> helper;
    ASSERT_TRUE(helper.Init());
    auto schema = std::shared_ptr<config::TabletSchema>(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema2.json"));
    ASSERT_TRUE(schema);
    ASSERT_TRUE(schema->TEST_GetImpl()->AddIndexConfig(helper._config).IsOK());
    AtomicExpressionCreator creator(helper._segments, schema, &exprPool, &pool);
    ASSERT_EQ(0, creator.GetCreatedExpressionCount());
    expression::AttributeExpression* expr = creator.createAtomicExpr("attr_name", "");
    ASSERT_TRUE(expr);
    auto typedAttrExpression = dynamic_cast<AtomicAttributeExpression<Cpp>*>(expr);
    ASSERT_TRUE(typedAttrExpression);
    ASSERT_EQ(1, creator.GetCreatedExpressionCount());
    AtomicAttributeExpression<Cpp>* createdExpr =
        dynamic_cast<AtomicAttributeExpression<Cpp>*>(creator.GetCreatedExpression(0));
    ASSERT_TRUE(createdExpr);
    ASSERT_EQ(typedAttrExpression, createdExpr);
}

TEST_F(AtomicExpressionCreatorTest, TestFullAttributeTypeForCreator)
{
    InnerCreatorTest<ft_int8, /*isMulti*/ false>();
    InnerCreatorTest<ft_int16, /*isMulti*/ false>();
    InnerCreatorTest<ft_int32, /*isMulti*/ false>();
    InnerCreatorTest<ft_int64, /*isMulti*/ false>();
    InnerCreatorTest<ft_uint8, /*isMulti*/ false>();
    InnerCreatorTest<ft_uint16, /*isMulti*/ false>();
    InnerCreatorTest<ft_uint32, /*isMulti*/ false>();
    InnerCreatorTest<ft_uint64, /*isMulti*/ false>();
    InnerCreatorTest<ft_float, /*isMulti*/ false>();
    InnerCreatorTest<ft_double, /*isMulti*/ false>();
    InnerCreatorTest<ft_string, /*isMulti*/ false>();
    InnerCreatorTest<ft_time, /*isMulti*/ false>();
    InnerCreatorTest<ft_date, /*isMulti*/ false>();
    InnerCreatorTest<ft_timestamp, /*isMulti*/ false>();
    // hash type not support by attibute
    // InnerCreatorTest<ft_hash_64, /*isMulti*/ false>();
    // InnerCreatorTest<ft_hash_128, /*isMulti*/ false>();
    InnerCreatorTest<ft_int8, /*isMulti*/ true>();
    InnerCreatorTest<ft_int16, /*isMulti*/ true>();
    InnerCreatorTest<ft_int32, /*isMulti*/ true>();
    InnerCreatorTest<ft_int64, /*isMulti*/ true>();
    InnerCreatorTest<ft_uint8, /*isMulti*/ true>();
    InnerCreatorTest<ft_uint16, /*isMulti*/ true>();
    InnerCreatorTest<ft_uint32, /*isMulti*/ true>();
    InnerCreatorTest<ft_uint64, /*isMulti*/ true>();
    InnerCreatorTest<ft_float, /*isMulti*/ true>();
    InnerCreatorTest<ft_double, /*isMulti*/ true>();
    InnerCreatorTest<ft_string, /*isMulti*/ true>();
    InnerCreatorTest<ft_time, /*isMulti*/ false>();
    InnerCreatorTest<ft_date, /*isMulti*/ false>();
    InnerCreatorTest<ft_timestamp, /*isMulti*/ false>();
    // InnerCreatorTest<ft_hash_64, /*isMulti*/ true>();
    // InnerCreatorTest<ft_hash_128, /*isMulti*/ true>();
}

} // namespace indexlibv2::index
