#include "indexlib/config/test/field_type_traits_unittest.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, FieldTypeTraitsTest);

FieldTypeTraitsTest::FieldTypeTraitsTest() {}

FieldTypeTraitsTest::~FieldTypeTraitsTest() {}

void FieldTypeTraitsTest::CaseSetUp() {}

void FieldTypeTraitsTest::CaseTearDown() {}

void FieldTypeTraitsTest::TestFieldToAttrType()
{
    ASSERT_EQ(AT_INT8, TransFieldTypeToAttrType(ft_int8));
    ASSERT_EQ(AT_UINT8, TransFieldTypeToAttrType(ft_uint8));
    ASSERT_EQ(AT_INT16, TransFieldTypeToAttrType(ft_int16));
    ASSERT_EQ(AT_UINT16, TransFieldTypeToAttrType(ft_uint16));
    ASSERT_EQ(AT_INT32, TransFieldTypeToAttrType(ft_int32));
    ASSERT_EQ(AT_UINT32, TransFieldTypeToAttrType(ft_uint32));
    ASSERT_EQ(AT_INT64, TransFieldTypeToAttrType(ft_int64));
    ASSERT_EQ(AT_UINT64, TransFieldTypeToAttrType(ft_uint64));
    ASSERT_EQ(AT_FLOAT, TransFieldTypeToAttrType(ft_float));
    ASSERT_EQ(AT_DOUBLE, TransFieldTypeToAttrType(ft_double));
    ASSERT_EQ(AT_HASH_64, TransFieldTypeToAttrType(ft_hash_64));
    ASSERT_EQ(AT_HASH_128, TransFieldTypeToAttrType(ft_hash_128));
    ASSERT_EQ(AT_STRING, TransFieldTypeToAttrType(ft_string));
    ASSERT_EQ(AT_UNKNOWN, TransFieldTypeToAttrType(ft_byte1));
    ASSERT_EQ(AT_UNKNOWN, TransFieldTypeToAttrType(ft_fp8));
    ASSERT_EQ(AT_UNKNOWN, TransFieldTypeToAttrType(ft_unknown));
}
}} // namespace indexlib::config
