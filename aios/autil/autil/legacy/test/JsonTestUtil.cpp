#include "autil/legacy/test/JsonTestUtil.h"

#include <algorithm>
#include <cstring>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <ostream>
#include <rapidjson/document.h>

#include "autil/mem_pool/Pool.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "unittest/unittest.h"

using namespace rapidjson;
using namespace std;

namespace autil {
namespace legacy {

bool JsonTestUtil::deepSortKeys(SimpleValue &value) {
    switch (value.GetType()) {
    case kObjectType:
        for (auto m = value.MemberBegin(); m != value.MemberEnd(); ++m) {
            if (!deepSortKeys(m->value)) {
                return false;
            }
        }
        std::sort(value.MemberBegin(), value.MemberEnd(), [](const auto &lhs, const auto &rhs) {
            return std::strcmp(lhs.name.GetString(), rhs.name.GetString()) < 0;
        });
        break;
    case kArrayType:
        for (auto it = value.Begin(); it != value.End(); ++it)
            if (!deepSortKeys(*it))
                return false;
        break;
    default:
        return true;
    }
    return true;
}

void JsonTestUtil::prettyDocument(SimpleDocument &document, std::string &result) {
    ASSERT_TRUE(deepSortKeys(document));
    StringBuffer buffer;
    PrettyWriter<StringBuffer> writer(buffer);
    document.Accept(writer);
    result = buffer.GetString();
}

void JsonTestUtil::checkJsonStringEqual(const std::string &expected, const std::string &actual) {
    autil::mem_pool::Pool pool;
    AutilPoolAllocator expectedAllocator(&pool);
    SimpleDocument expectedDoc(&expectedAllocator);
    expectedDoc.Parse(expected.c_str());
    ASSERT_FALSE(expectedDoc.HasParseError())
        << "errorCode:" << expectedDoc.GetParseError() << " offset:" << expectedDoc.GetErrorOffset() << std::endl
        << "str [" << expected << "]";

    AutilPoolAllocator actualAllocator(&pool);
    SimpleDocument actualDoc(&actualAllocator);
    actualDoc.Parse(actual.c_str());
    ASSERT_FALSE(actualDoc.HasParseError())
        << "errorCode:" << actualDoc.GetParseError() << " offset:" << actualDoc.GetErrorOffset() << std::endl
        << "str [" << actual << "]";

    if (actualDoc != expectedDoc) {
        string expectedStr, actualStr;
        ASSERT_NO_FATAL_FAILURE(prettyDocument(expectedDoc, expectedStr));
        ASSERT_NO_FATAL_FAILURE(prettyDocument(actualDoc, actualStr));
        ASSERT_EQ(expectedStr, actualStr) << "[expected]" << std::endl
                                          << expectedStr << std::endl
                                          << "[actual]" << std::endl
                                          << actualStr << std::endl;
    }
}

} // namespace legacy
} // namespace autil
