#include "swift/filter/EliminateFieldFilter.h"

#include <cstddef>
#include <string>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace swift::common;
namespace swift {
namespace filter {

class EliminateFieldFilterTest : public TESTBASE {};

TEST_F(EliminateFieldFilterTest, testGetFilterFields) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("CMD");
    string value1 = string("DEL");
    fieldGroupWriter.addProductionField(name1, value1, false);
    string name2 = string("field1");
    string value2 = string("abc");
    fieldGroupWriter.addProductionField(name2, value2, true);
    string name3 = string("field2");
    string value3 = string("def");
    fieldGroupWriter.addProductionField(name3, value3, false);
    string name4 = string("field3");
    string value4 = string("");
    fieldGroupWriter.addProductionField(name4, value4, false);
    string data = fieldGroupWriter.toString();

    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        StringView tmp1("CMD");
        StringView tmp2("not_exist_field");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);
        string result;
        fieldFilter.getFilteredFields("", result);
        EXPECT_EQ(string(""), result);
    }
    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        StringView tmp1("CMD");
        StringView tmp2("field1");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);
        string result;
        fieldFilter.getFilteredFields(data, result);
        EXPECT_TRUE(fieldGroupReader.fromConsumptionString(result));

        EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
        const Field *field1 = fieldGroupReader.getField(0);
        EXPECT_TRUE(field1->name.empty());
        string data;
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("DEL"), data);
        EXPECT_FALSE(field1->isUpdated);
        EXPECT_TRUE(field1->isExisted);

        const Field *field2 = fieldGroupReader.getField(1);
        EXPECT_TRUE(field2->name.empty());
        data.assign(field2->value.begin(), field2->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(field2->isUpdated);
        EXPECT_TRUE(field2->isExisted);
    }

    {

        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        StringView tmp1("field1");
        StringView tmp2("field3");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);
        string result;
        fieldFilter.getFilteredFields(data, result);
        EXPECT_TRUE(fieldGroupReader.fromConsumptionString(result));

        EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
        const Field *field1 = fieldGroupReader.getField(0);
        EXPECT_TRUE(field1->name.empty());
        string data;
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(field1->isUpdated);
        EXPECT_TRUE(field1->isExisted);

        const Field *field2 = fieldGroupReader.getField(1);
        EXPECT_TRUE(field2->name.empty());
        EXPECT_TRUE(field2->value.empty());
        data.assign(field2->value.begin(), field2->value.end());
        EXPECT_EQ(string(""), data);
        EXPECT_FALSE(field2->isUpdated);
        EXPECT_TRUE(field2->isExisted);
    }

    {

        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        StringView tmp1("CMD");
        StringView tmp2("field2");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);
        string result;
        fieldFilter.getFilteredFields(data, result);
        EXPECT_TRUE(fieldGroupReader.fromConsumptionString(result));

        EXPECT_EQ(string(""), result);
        EXPECT_TRUE(fieldGroupReader.fromConsumptionString(result));
        EXPECT_EQ((size_t)0, fieldGroupReader.getFieldSize());
    }
    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        StringView tmp1("CMD");
        StringView tmp2("not_exist_field");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        string result;
        fieldFilter.getFilteredFields(data, result);
        EXPECT_EQ(string(""), result);
    }
    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        StringView tmp1("field1");
        StringView tmp2("not_exist_field");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        string result;
        fieldFilter.getFilteredFields(data, result);

        EXPECT_TRUE(fieldGroupReader.fromConsumptionString(result));
        EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
        const Field *field1 = fieldGroupReader.getField(0);
        EXPECT_TRUE(field1->name.empty());
        string data;
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(field1->isExisted);
        EXPECT_TRUE(field1->isUpdated);

        const Field *field2 = fieldGroupReader.getField(1);
        EXPECT_TRUE(field2->name.empty());
        EXPECT_TRUE(field2->value.empty());
        EXPECT_FALSE(field2->isExisted);
        EXPECT_FALSE(field2->isUpdated);
    }
}

TEST_F(EliminateFieldFilterTest, testFilterFields) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("CMD");
    string value1 = string("DEL");
    fieldGroupWriter.addProductionField(name1, value1, false);
    string name2 = string("field1");
    string value2 = string("abc");
    fieldGroupWriter.addProductionField(name2, value2, true);
    string name3 = string("field2");
    string value3 = string("def");
    fieldGroupWriter.addProductionField(name3, value3, false);
    string name4 = string("field3");
    string value4 = string("");
    fieldGroupWriter.addProductionField(name4, value4, false);
    string data = fieldGroupWriter.toString();

    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(data);
        StringView tmp1("CMD");
        StringView tmp2("not_exist_field");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);
        string result;
        fieldGroupWriter.reset();
        fieldFilter.filterFields(fieldGroupReader, fieldGroupWriter);
        EXPECT_TRUE(fieldGroupWriter.empty());
    }

    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(data);
        StringView tmp1("CMD");
        StringView tmp2("field1");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        fieldGroupWriter.reset();
        fieldFilter.filterFields(fieldGroupReader, fieldGroupWriter);
        EXPECT_FALSE(fieldGroupWriter.empty());
        string result;
        fieldGroupWriter.toString(result);
        FieldGroupReader tmpFieldGroupReader;
        EXPECT_TRUE(tmpFieldGroupReader.fromConsumptionString(result));

        EXPECT_EQ((size_t)2, tmpFieldGroupReader.getFieldSize());
        const Field *field1 = tmpFieldGroupReader.getField(0);
        EXPECT_TRUE(field1->name.empty());
        string data;
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("DEL"), data);
        EXPECT_FALSE(field1->isUpdated);
        EXPECT_TRUE(field1->isExisted);

        const Field *field2 = tmpFieldGroupReader.getField(1);
        EXPECT_TRUE(field2->name.empty());
        data.assign(field2->value.begin(), field2->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(field2->isUpdated);
        EXPECT_TRUE(field2->isExisted);
    }

    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(data);
        StringView tmp1("field1");
        StringView tmp2("field3");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        fieldGroupWriter.reset();
        fieldFilter.filterFields(fieldGroupReader, fieldGroupWriter);
        EXPECT_FALSE(fieldGroupWriter.empty());
        string result;
        fieldGroupWriter.toString(result);
        FieldGroupReader tmpFieldGroupReader;
        EXPECT_TRUE(tmpFieldGroupReader.fromConsumptionString(result));
        EXPECT_EQ((size_t)2, tmpFieldGroupReader.getFieldSize());
        const Field *field1 = tmpFieldGroupReader.getField(0);
        EXPECT_TRUE(field1->name.empty());
        string data;
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(field1->isUpdated);
        EXPECT_TRUE(field1->isExisted);

        const Field *field2 = tmpFieldGroupReader.getField(1);
        EXPECT_TRUE(field2->name.empty());
        EXPECT_TRUE(field2->value.empty());
        data.assign(field2->value.begin(), field2->value.end());
        EXPECT_EQ(string(""), data);
        EXPECT_FALSE(field2->isUpdated);
        EXPECT_TRUE(field2->isExisted);
    }

    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(data);
        StringView tmp1("CMD");
        StringView tmp2("field2");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        fieldGroupWriter.reset();
        fieldFilter.filterFields(fieldGroupReader, fieldGroupWriter);
        EXPECT_TRUE(fieldGroupWriter.empty());
        string result;
        fieldGroupWriter.toString(result);
        FieldGroupReader tmpFieldGroupReader;
        EXPECT_TRUE(tmpFieldGroupReader.fromConsumptionString(result));
        EXPECT_EQ((size_t)0, tmpFieldGroupReader.getFieldSize());
    }
    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(data);
        StringView tmp1("CMD");
        StringView tmp2("not_exist_field");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        fieldGroupWriter.reset();
        fieldFilter.filterFields(fieldGroupReader, fieldGroupWriter);
        EXPECT_TRUE(fieldGroupWriter.empty());
        string result;
        fieldGroupWriter.toString(result);
        EXPECT_EQ(string(""), result);
    }
    {
        EliminateFieldFilter fieldFilter;
        FieldGroupReader fieldGroupReader;
        fieldGroupReader.fromProductionString(data);
        StringView tmp1("field1");
        StringView tmp2("not_exist_field");
        fieldFilter.addRequiredField(tmp1, 0);
        fieldFilter.addRequiredField(tmp2, 1);

        fieldGroupWriter.reset();
        fieldFilter.filterFields(fieldGroupReader, fieldGroupWriter);
        EXPECT_FALSE(fieldGroupWriter.empty());
        string result;
        fieldGroupWriter.toString(result);
        FieldGroupReader tmpFieldGroupReader;
        EXPECT_TRUE(tmpFieldGroupReader.fromConsumptionString(result));

        EXPECT_EQ((size_t)2, tmpFieldGroupReader.getFieldSize());
        const Field *field1 = tmpFieldGroupReader.getField(0);
        EXPECT_TRUE(field1->name.empty());
        string data;
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(field1->isExisted);
        EXPECT_TRUE(field1->isUpdated);

        const Field *field2 = tmpFieldGroupReader.getField(1);
        EXPECT_TRUE(field2->name.empty());
        EXPECT_TRUE(field2->value.empty());
        EXPECT_FALSE(field2->isExisted);
        EXPECT_FALSE(field2->isUpdated);
    }
}

} // namespace filter
} // namespace swift
