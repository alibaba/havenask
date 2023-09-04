#include "suez/sdk/JsonNodeRef.h"

#include <algorithm>

#include "autil/legacy/jsonizable.h"
#include "unittest/unittest.h"
using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;

namespace suez {

class JsonNodeRefTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    Any s2d(const string &str) {
        string tempStr = str;
        replace(tempStr.begin(), tempStr.end(), '\'', '"');
        return json::ParseJson(tempStr);
    }
    void assertReadTarget(JsonNodeRef &target, const string &nodePath, const Any &expectValue);

protected:
    JsonNodeRef::JsonMap _node;
    JsonNodeRef::JsonMap _other;
};

void JsonNodeRefTest::setUp() {
    _node = JsonNodeRef::JsonMap();
    _other = JsonNodeRef::JsonMap();
}

void JsonNodeRefTest::tearDown() {}

#define INIT_NODE(node, jsonStr) ({ node = AnyCast<JsonNodeRef::JsonMap>(s2d(jsonStr)); })

TEST_F(JsonNodeRefTest, testCreateRecursive) {
    INIT_NODE(_node, "{'a':{'b':{'c': 'd'}}}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.create("a.b.e.e", s2d("'f'"), true)) << target.getLastError();
    INIT_NODE(_other, "{'a':{'b':{'c': 'd', 'e':{'e':'f'}}}}");
    JsonNodeRef other(_other);
    EXPECT_EQ(FastToJsonString(_node), FastToJsonString(_other));
}

TEST_F(JsonNodeRefTest, testCreateSucc) {
    INIT_NODE(_node, "{'a':'b'}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.create("c", s2d("'d'"))) << target.getLastError();
    ASSERT_TRUE(target.create("e", s2d("'d'"))) << target.getLastError();
    INIT_NODE(_other, "{'a':'b','c':'d','e':'d'}");
    JsonNodeRef other(_other);
    ASSERT_TRUE(target == other);
}

TEST_F(JsonNodeRefTest, testCreateExist) {
    INIT_NODE(_node, "{'a':'b'}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.create("c", s2d("'d'"))) << target.getLastError();
    ASSERT_TRUE(target.create("c", s2d("'e'"))) << target.getLastError();
    INIT_NODE(_other, "{'a':'b','c':'e'}");
    JsonNodeRef other(_other);
    ASSERT_TRUE(target == other);
}

TEST_F(JsonNodeRefTest, testCreateCompValue) {
    INIT_NODE(_node, "{'a':'b'}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.create("c", s2d("['d', 'e']"))) << target.getLastError();
    ASSERT_TRUE(target.create("d", s2d("{'d':'e'}"))) << target.getLastError();
    INIT_NODE(_other, "{'a':'b','c':['d', 'e'], 'd' : {'d':'e'}}");
    JsonNodeRef other(_other);
    ASSERT_TRUE(target == other);
}

TEST_F(JsonNodeRefTest, testCreateRootFailed) {
    JsonNodeRef target(_node);
    ASSERT_FALSE(target.create("", s2d("'d'")));
    ASSERT_EQ(string("create with empty node path!"), target.getLastError());
}

TEST_F(JsonNodeRefTest, testCreateNotExistParentPath) {
    JsonNodeRef target(_node);
    ASSERT_FALSE(target.create("a.b", s2d("'d'")));
    ASSERT_EQ(string("create a.b failed, path node[a] not exist!"), target.getLastError());
}

TEST_F(JsonNodeRefTest, testUpdateSucc) {
    INIT_NODE(_node, "{'a':'b'}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.update("a", s2d("'e'"))) << target.getLastError();
    INIT_NODE(_other, "{'a':'e'}");
    JsonNodeRef other(_other);
    ASSERT_TRUE(target == other);
}

TEST_F(JsonNodeRefTest, testUpdateNotExist) {
    INIT_NODE(_node, "{'a':'b'}");
    JsonNodeRef target(_node);
    ASSERT_FALSE(target.update("b", s2d("'e'"))) << target.getLastError();
}

void JsonNodeRefTest::assertReadTarget(JsonNodeRef &target, const string &nodePath, const Any &expectValue) {
    autil::legacy::Any *any = target.read(nodePath);
    ASSERT_TRUE(any) << target.getLastError();
    ASSERT_EQ(FastToJsonString(expectValue), FastToJsonString(*any));
}

TEST_F(JsonNodeRefTest, testReadSucc) {
    INIT_NODE(_node, "{'a':['b', 'c'], 'd' : {'d1':'d2'}}");
    JsonNodeRef target(_node);
    assertReadTarget(target, "a", s2d("['b', 'c']"));
    assertReadTarget(target, "d.d1", s2d("'d2'"));
}

TEST_F(JsonNodeRefTest, testReadNotExistPath) {
    INIT_NODE(_node, "{'a':['b', 'c'], 'd' : {'d1':'d2'}}");
    JsonNodeRef target(_node);
    ASSERT_FALSE(target.read("c"));
    EXPECT_EQ("read node[c] not exist!", target.getLastError());
}
TEST_F(JsonNodeRefTest, testDelSucc) {
    INIT_NODE(_node, "{'a':['b', 'c'], 'd' : {'d1':'d2'}}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.del("a")) << target.getLastError();
    INIT_NODE(_other, "{'d' : {'d1':'d2'}}");
    JsonNodeRef other(_other);
    ASSERT_TRUE(target == _other);
}

TEST_F(JsonNodeRefTest, testDelMultiPath) {
    INIT_NODE(_node, "{'a':['b', 'c'], 'd' : {'d1':'d2','d3':'d4'}}");
    JsonNodeRef target(_node);
    ASSERT_TRUE(target.del("d.d1")) << target.getLastError();
    INIT_NODE(_other, "{'a':['b', 'c'], 'd' : {'d3':'d4'}}");
    JsonNodeRef other(_other);
    ASSERT_TRUE(target == other);
}

TEST_F(JsonNodeRefTest, testDelNotExist) {
    INIT_NODE(_node, "{'a':['b', 'c'], 'd' : {'d1':'d2','d3':'d4'}}");
    JsonNodeRef target(_node);
    ASSERT_FALSE(target.del("d.d1.d3.d4"));
    ASSERT_EQ(string("delete d.d1.d3.d4 failed, path node[d1] is not a map!"), target.getLastError());
}

} // namespace suez
