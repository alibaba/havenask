#include "swift/common/FieldGroupReader.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "swift/common/FieldGroupWriter.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
namespace swift {
namespace common {

class FieldGroupReaderTest : public TESTBASE {};

TEST_F(FieldGroupReaderTest, testSimpleProcess) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("CMD");
    string value1 = string("DEL");
    fieldGroupWriter.addProductionField(name1, value1, true);
    string name2 = string("data");
    string value2 = string("abc");
    fieldGroupWriter.addProductionField(name2, value2, false);
    string rawData;
    fieldGroupWriter.toString(rawData);

    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(rawData));
    EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
    string data;
    {
        const Field *field1 = fieldGroupReader.getField(0);
        data.assign(field1->name.begin(), field1->name.end());
        EXPECT_EQ(string("CMD"), data);
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("DEL"), data);
        EXPECT_TRUE(field1->isUpdated);
    }
    {
        const Field *field2 = fieldGroupReader.getField(1);
        data.assign(field2->name.begin(), field2->name.end());
        EXPECT_EQ(string("data"), data);
        data.assign(field2->value.begin(), field2->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(!field2->isUpdated);
    }
    {
        const Field *field1 = fieldGroupReader.getField("CMD");
        data.assign(field1->name.begin(), field1->name.end());
        EXPECT_EQ(string("CMD"), data);
        data.assign(field1->value.begin(), field1->value.end());
        EXPECT_EQ(string("DEL"), data);
        EXPECT_TRUE(field1->isUpdated);
    }
    {
        const Field *field2 = fieldGroupReader.getField("data");
        data.assign(field2->name.begin(), field2->name.end());
        EXPECT_EQ(string("data"), data);
        data.assign(field2->value.begin(), field2->value.end());
        EXPECT_EQ(string("abc"), data);
        EXPECT_TRUE(!field2->isUpdated);
    }
    {
        const Field *fieldNull = fieldGroupReader.getField("null");
        EXPECT_TRUE(fieldNull == NULL);
    }
}

TEST_F(FieldGroupReaderTest, testWriteReadLength) {
    // border test
    FieldGroupWriter fieldGroupWriter;
    for (uint32_t i = 0; i < 5; ++i) {
        uint32_t offset = 7 * i;
        fieldGroupWriter.writeLength((1LL << offset) - 1);
        fieldGroupWriter.writeLength((1LL << offset));
        fieldGroupWriter.writeLength((1LL << offset) + 1);
    }
    fieldGroupWriter.writeLength(std::numeric_limits<uint32_t>::max() - 1);
    fieldGroupWriter.writeLength(std::numeric_limits<uint32_t>::max());
    string data = fieldGroupWriter.toString();

    FieldGroupReader fieldGroupReader;
    fieldGroupReader.reset(data);
    uint32_t length = 0;
    for (uint32_t i = 0; i < 5; ++i) {
        uint32_t offset = 7 * i;
        EXPECT_TRUE(fieldGroupReader.readLength(length));
        EXPECT_EQ((uint32_t)((1LL << offset) - 1), length);
        EXPECT_TRUE(fieldGroupReader.readLength(length));
        EXPECT_EQ((uint32_t)(1LL << offset), length);
        EXPECT_TRUE(fieldGroupReader.readLength(length));
        EXPECT_EQ((uint32_t)((1LL << offset) + 1), length);
    }
    EXPECT_TRUE(fieldGroupReader.readLength(length));
    EXPECT_EQ((uint32_t)(std::numeric_limits<uint32_t>::max() - 1), length);
    EXPECT_TRUE(fieldGroupReader.readLength(length));
    EXPECT_EQ((uint32_t)(std::numeric_limits<uint32_t>::max()), length);
    EXPECT_TRUE(!fieldGroupReader.readLength(length));

    // random test
    fieldGroupWriter.reset();
    vector<uint32_t> randVec;
    srand(time(NULL));
    for (uint32_t i = 0; i < 10000; ++i) {
        uint32_t rd = rand() % std::numeric_limits<uint32_t>::max();
        randVec.push_back(rd);
        fieldGroupWriter.writeLength(rd);
    }

    data = fieldGroupWriter.toString();
    fieldGroupReader.reset(data);
    for (uint32_t i = 0; i < 10000; ++i) {
        EXPECT_TRUE(fieldGroupReader.readLength(length));
        EXPECT_EQ(randVec[i], length);
    }

    // exception test
    data = "1";
    (unsigned char &)data[0] = (unsigned char &)data[0] | 0x80;
    fieldGroupReader.reset(data);
    EXPECT_TRUE(!fieldGroupReader.readLength(length));
}

TEST_F(FieldGroupReaderTest, testExceptionRead) {
    FieldGroupWriter fieldGroupWriter;
    string tmp = "abcdef";
    StringView cs(tmp.c_str(), tmp.length());
    fieldGroupWriter.writeBytes(cs);
    string tmp1 = "1234";
    cs = tmp1;
    fieldGroupWriter.writeBytes(cs);
    string rawData = fieldGroupWriter.toString();

    FieldGroupReader fieldGroupReader;
    fieldGroupReader.reset(rawData);
    StringView cs1;
    EXPECT_TRUE(!fieldGroupReader.readBytes(tmp.length() + 100, cs1));
    EXPECT_EQ((uint32_t)0, fieldGroupReader._readCursor);
    EXPECT_TRUE(fieldGroupReader.readBytes(tmp.length(), cs1));
    EXPECT_EQ((uint32_t)tmp.length(), fieldGroupReader._readCursor);
    string data;
    data.assign(cs1.begin(), cs1.end());
    EXPECT_EQ(tmp, data);

    bool ret = false;
    ret = fieldGroupReader.readBytes(tmp1.length(), cs1);
    EXPECT_TRUE(ret);
    EXPECT_EQ((uint32_t)(tmp.length() + tmp1.length()), fieldGroupReader._readCursor);
    data.assign(cs1.begin(), cs1.end());
    EXPECT_EQ(tmp1, data);

    bool updated = false;
    EXPECT_TRUE(!fieldGroupReader.readBool(updated));
}

TEST_F(FieldGroupReaderTest, testFromProductionString) {
    string data;
    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));

    FieldGroupWriter fieldGroupWriter;
    string name = string("CMD");
    string value = string("DEL");
    fieldGroupWriter.writeLength(name.length());
    data = fieldGroupWriter.toString();
    EXPECT_TRUE(!fieldGroupReader.fromProductionString(data));
    StringView cs1(name.c_str(), name.length());
    fieldGroupWriter.writeBytes(cs1);
    data = fieldGroupWriter.toString();
    EXPECT_TRUE(!fieldGroupReader.fromProductionString(data));

    fieldGroupWriter.writeLength(value.length());
    data = fieldGroupWriter.toString();
    EXPECT_TRUE(!fieldGroupReader.fromProductionString(data));
    StringView cs2(value.c_str(), value.length());
    fieldGroupWriter.writeBytes(cs2);
    data = fieldGroupWriter.toString();
    EXPECT_TRUE(!fieldGroupReader.fromProductionString(data));

    fieldGroupWriter.writeBool(true);
    data = fieldGroupWriter.toString();
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));
}

TEST_F(FieldGroupReaderTest, testFromConsumeString) {
    FieldGroupReader reader;
    string data;
    EXPECT_TRUE(reader.fromConsumptionString(data));
    FieldGroupWriter writer;
    writer.addConsumptionField(NULL, true);
    data = writer.toString();
    EXPECT_TRUE(reader.fromConsumptionString(data));
    EXPECT_EQ((size_t)1, reader.getFieldSize());
    const Field *field = reader.getField(0);
    EXPECT_TRUE(!field->isExisted);
    EXPECT_TRUE(field->name.empty());
    EXPECT_TRUE(field->value.empty());
    EXPECT_TRUE(!field->isUpdated);

    string name = "abc";
    StringView cs(name.c_str(), name.length());
    writer.addConsumptionField(&cs, true);
    data = writer.toString();
    EXPECT_TRUE(reader.fromConsumptionString(data));
    EXPECT_EQ((size_t)2, reader.getFieldSize());
    field = reader.getField(1);
    EXPECT_TRUE(field->name.empty());
    EXPECT_TRUE(cs == field->value);
    EXPECT_TRUE(true == field->isUpdated);

    string value = "def";
    writer.writeBool(true);
    data = writer.toString();
    EXPECT_TRUE(!reader.fromConsumptionString(data));
    writer.writeLength(value.length());
    data = writer.toString();
    EXPECT_TRUE(!reader.fromConsumptionString(data));
    StringView cs1(value.c_str(), value.length());
    writer.writeBytes(cs1);
    data = writer.toString();
    EXPECT_TRUE(!reader.fromConsumptionString(data));
    writer.writeBool(false);
    data = writer.toString();
    EXPECT_TRUE(reader.fromConsumptionString(data));
    EXPECT_EQ((size_t)3, reader.getFieldSize());
    field = reader.getField(2);
    EXPECT_TRUE(field->name.empty());
    EXPECT_TRUE(cs1 == field->value);
    EXPECT_TRUE(false == field->isUpdated);
}

} // namespace common
} // namespace swift
