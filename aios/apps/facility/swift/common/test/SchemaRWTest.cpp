#include <fstream> // IWYU pragma: keep
#include <iostream>
#include <map>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "swift/common/FieldSchema.h"
#include "swift/common/SchemaReader.h"
#include "swift/common/SchemaWriter.h"
#include "swift/config/ResourceReader.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace swift {
namespace common {

class SchemaTest : public TESTBASE {
public:
    void setUp() {
        _workDir = GET_TEST_DATA_PATH() + "schema_topic/schema/";
        getFieldValues();
    }
    void tearDown() {}

private:
    void getFieldValues();
    string writeSchema(SchemaWriter &writer, const string &schemaStr);
    void readSchema(SchemaReader &reader, const string &result);
    string writeFieldGroup();
    void readFieldGroup(const string &result);

private:
    map<string, string> _fieldValues;
    vector<string> _fieldValVec;
    string _workDir;
};

string &ltrim(string &str, const string &chars = "\t\n\v\f\r ") {
    str.erase(0, str.find_first_not_of(chars));
    return str;
}

string &rtrim(string &str, const string &chars = "\t\n\v\f\r ") {
    str.erase(str.find_last_not_of(chars) + 1);
    return str;
}

string &trim(string &str, const string &chars = "\t\n\v\f\r ") { return ltrim(rtrim(str, chars), chars); }

void SchemaTest::getFieldValues() {
    string fileName(_workDir + "/data");
    ifstream myfile(fileName);
    string line;

    if (myfile.is_open()) {
        while (getline(myfile, line)) {
            const vector<string> &kv = StringUtil::split(line, "=");
            string key(kv[0]);
            trim(key);
            string value(kv[1]);
            string old1("[False]");
            string old2("[True]");
            string empty;
            StringUtil::replaceLast(value, old1, empty);
            StringUtil::replaceLast(value, old2, empty);
            trim(value);
            _fieldValues[key] = value;
            _fieldValVec.push_back(value);
        }
        myfile.close();
    }
    cout << "field count: " << _fieldValVec.size() << " " << _fieldValues.size() << endl;
}

void saveToFile(const string &fileName, const string &content) {
    ofstream myfile(fileName);
    if (myfile.is_open()) {
        myfile << content;
        myfile.close();
    }
}

TEST_F(SchemaTest, testNormal) {
    config::ResourceReader resReader(_workDir);
    string content;
    EXPECT_TRUE(resReader.getFileContent(content, "schema.json"));

    SchemaWriter writer;
    EXPECT_TRUE(writer.loadSchema(content));
    int32_t version = writer._schema.calcVersion();
    writer.setVersion(version);
    {
        EXPECT_TRUE(writer.setField("timestamp", "1591671634"));
        EXPECT_TRUE(writer.setField("price", "1600"));
        EXPECT_FALSE(writer.setField("not_exist", "xx"));
        EXPECT_TRUE(writer.setField("image_path", "d://pictures/show.jpeg"));
        EXPECT_TRUE(writer.setSubField("phone_series", "iphone", "iphone 7s"));
        EXPECT_TRUE(writer.setSubField("phone_series", "huawei", "huawei p10 pro"));
        EXPECT_TRUE(writer.setSubField("phone_series", "xiaomi", "k20 pro"));
        EXPECT_FALSE(writer.setSubField("not_exist", "not_exist", "world"));
        EXPECT_TRUE(writer.setField("CMD", "delete"));
        string result = writer.toStringWithHeader();
        cout << "result: " << result << endl;
        SchemaReader reader;
        map<string, SchemaWriterField> resultMap;
        EXPECT_TRUE(reader.loadSchema(content));

        SchemaHeader readHeader;
        const vector<SchemaReaderField> &fieldvec = reader.parseFromString(result.c_str(), result.size(), readHeader);
        for (const auto &field : fieldvec) {
            const StringView &key = field.key;
            const StringView &value = field.value;
            cout << key.to_string() << ": " << value.to_string();
            SchemaWriterField sf;
            sf.value = value.to_string();
            cout << "  {" << endl;
            for (const auto &sub : field.subFields) {
                cout << "    " << sub.key.to_string() << ": " << sub.value.to_string() << endl;
                sf.subFields[sub.key.to_string()] = new SchemaWriterField();
                sf.subFields[sub.key.to_string()]->value = sub.value.to_string();
            }
            cout << "  }" << endl;
            resultMap[key.to_string()] = sf;
            cout << endl;
        }

        EXPECT_EQ("delete", resultMap["CMD"].value);
        EXPECT_EQ("1591671634", resultMap["timestamp"].value);
        EXPECT_EQ("1600", resultMap["price"].value);
        EXPECT_EQ("d://pictures/show.jpeg", resultMap["image_path"].value);
        EXPECT_EQ("iphone 7s", resultMap["phone_series"].subFields["iphone"]->value);
        EXPECT_EQ("huawei p10 pro", resultMap["phone_series"].subFields["huawei"]->value);
        EXPECT_EQ("k20 pro", resultMap["phone_series"].subFields["xiaomi"]->value);

        EXPECT_TRUE(resultMap.find("not_exist") == resultMap.end());
        EXPECT_TRUE(resultMap["phone_series"].subFields.find("not_exist") == resultMap["phone_series"].subFields.end());

        int32_t vv = 0;
        EXPECT_TRUE(SchemaReader::readVersion(result.c_str(), vv));
        EXPECT_EQ(version, vv);
        cout << readHeader.meta.checkval << endl;
        EXPECT_EQ(writer._version, readHeader.version);
        EXPECT_EQ(0, readHeader.meta.reserve);

        for (auto &item : resultMap) {
            for (auto &sub : item.second.subFields) {
                DELETE_AND_SET_NULL(sub.second);
            }
        }
    }
    {
        writer.reset();
        EXPECT_TRUE(writer.setField("timestamp", "159167999", true));
        EXPECT_TRUE(writer.setField("price", "2000", false));
        EXPECT_TRUE(writer.setField("image_path", "", false));
        EXPECT_TRUE(writer.setSubField("phone_series", "huawei", "huawei p11", false));
        EXPECT_TRUE(writer.setSubField("phone_series", "xiaomi", "k30 pro", true));
        EXPECT_TRUE(writer.setField("CMD", "add"));
        string result = writer.toString();
        result.insert(0, sizeof(HeadMeta), 0);
        cout << "result: " << result << endl;
        SchemaReader reader;
        map<string, SchemaWriterField> resultMap;
        EXPECT_TRUE(reader.loadSchema(content));

        const vector<SchemaReaderField> &fieldvec = reader.parseFromString(result);
        for (const auto &field : fieldvec) {
            const StringView &key = field.key;
            const StringView &value = field.value;
            SchemaWriterField sf;
            sf.value = value.to_string();
            sf.isNone = field.isNone;
            for (const auto &sub : field.subFields) {
                sf.subFields[sub.key.to_string()] = new SchemaWriterField();
                sf.subFields[sub.key.to_string()]->value = sub.value.to_string();
                sf.subFields[sub.key.to_string()]->isNone = sub.isNone;
            }
            resultMap[key.to_string()] = sf;
        }

        EXPECT_FALSE(resultMap["CMD"].isNone);
        EXPECT_EQ("add", resultMap["CMD"].value);
        EXPECT_TRUE(resultMap["timestamp"].isNone);
        EXPECT_TRUE(resultMap["timestamp"].value.empty());
        EXPECT_FALSE(resultMap["price"].isNone);
        EXPECT_EQ("2000", resultMap["price"].value);
        EXPECT_FALSE(resultMap["image_path"].isNone);
        EXPECT_TRUE(resultMap["image_path"].value.empty());
        EXPECT_TRUE(resultMap["phone_series"].subFields["iphone"]->isNone);
        EXPECT_TRUE(resultMap["phone_series"].subFields["iphone"]->value.empty());
        EXPECT_FALSE(resultMap["phone_series"].subFields["huawei"]->isNone);
        EXPECT_EQ("huawei p11", resultMap["phone_series"].subFields["huawei"]->value);
        EXPECT_TRUE(resultMap["phone_series"].subFields["xiaomi"]->isNone);
        EXPECT_TRUE(resultMap["phone_series"].subFields["xiaomi"]->value.empty());

        for (auto &item : resultMap) {
            for (auto &sub : item.second.subFields) {
                DELETE_AND_SET_NULL(sub.second);
            }
        }
    }
}

string SchemaTest::writeSchema(SchemaWriter &writer, const string &schemaStr) {
    writer.reset();
    for (const auto &fv : _fieldValues) {
        if (!fv.second.empty()) {
            writer.setField(fv.first, fv.second);
        }
    }
    return writer.toString();
}

void SchemaTest::readSchema(SchemaReader &reader, const string &result) {
    const vector<SchemaReaderField> &fieldvec = reader.parseFromString(result);
    EXPECT_EQ(_fieldValues.size(), fieldvec.size());
    // ofstream dataOut(_workDir + "dataSchema");
    for (const auto &field : fieldvec) {
        const StringView &key = field.key;
        const StringView &value = field.value;
        // dataOut << key.toString() << " = " << value.toString() << endl;
        EXPECT_EQ(value.to_string(), _fieldValues[key.to_string()]);
    }
}

TEST_F(SchemaTest, testBigData) {
    config::ResourceReader resReader(_workDir);
    string content;
    EXPECT_TRUE(resReader.getFileContent(content, "schema_mainse.json"));
    SchemaWriter writer;
    EXPECT_TRUE(writer.loadSchema(content));
    string result = writeSchema(writer, content);
    result.insert(0, sizeof(HeadMeta), 0);
    uint64_t begin = TimeUtility::currentTime();
    // for (int i=0; i<1000000; i++) {
    writeSchema(writer, content);
    // }
    uint64_t step1 = TimeUtility::currentTime();

    // saveToFile(_workDir + "resultSchema", result);
    SchemaReader reader;
    EXPECT_TRUE(reader.loadSchema(content));
    uint64_t step2 = TimeUtility::currentTime();
    // for (int i=0; i<10000; i++) {
    readSchema(reader, result);
    // }
    uint64_t end = TimeUtility::currentTime();
    cout << "schema write: " << step1 - begin << ", read: " << end - step2 << ", parse schema: " << step2 - step1
         << endl;
}

string SchemaTest::writeFieldGroup() {
    FieldGroupWriter writer;
    for (const auto &kv : _fieldValues) {
        writer.addProductionField(kv.first, kv.second, false);
    }
    return writer.toString();
}

void SchemaTest::readFieldGroup(const string &result) {
    FieldGroupReader reader;
    EXPECT_TRUE(reader.fromProductionString(result));
    // size_t fieldCount = reader.getFieldSize();
    // size_t nonEmptyCount = 0;
    // ofstream dataOut(_workDir + "dataFilter");
    // for (size_t i = 0; i < fieldCount; ++i) {
    // const Field *field = reader.getField(i);
    // StringView name = field->name;
    // StringView value = field->value;
    // if (!value.empty()) {
    //     ++nonEmptyCount;
    // }
    // bool isUpdated = field->isUpdated;
    // dataOut << field->name << " = " << field->value << endl;//" " << field->isUpdated << endl;
    // }
    // cout << fieldCount << " " << nonEmptyCount << endl;
    // dataOut.close();
}

TEST_F(SchemaTest, testFieldGroup) {
    uint64_t begin = TimeUtility::currentTime();
    const string &result = writeFieldGroup();
    // for (int i=0; i<10000; i++) {
    writeFieldGroup();
    // }
    uint64_t step1 = TimeUtility::currentTime();
    // saveToFile(_workDir + "resultFilter", result);
    // for (int i=0; i<10000; i++) {
    readFieldGroup(result);
    // }
    uint64_t end = TimeUtility::currentTime();
    cout << "filter write: " << step1 - begin << ", read: " << end - step1 << endl;
}

}; // namespace common
}; // namespace swift
