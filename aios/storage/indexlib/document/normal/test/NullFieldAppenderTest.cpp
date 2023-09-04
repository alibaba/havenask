#include "indexlib/document/normal/NullFieldAppender.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2 { namespace document {

class NullFieldAppenderTest : public TESTBASE
{
};

TEST_F(NullFieldAppenderTest, TestSimpleProcess)
{
    vector<shared_ptr<FieldConfig>> fieldConfigs;
    fieldConfigs.push_back(make_shared<FieldConfig>("string1", ft_string, false));
    fieldConfigs.push_back(make_shared<FieldConfig>("long1", ft_uint32, false));
    fieldConfigs.push_back(make_shared<FieldConfig>("body", ft_text, false));

    NullFieldAppender appender;
    shared_ptr<RawDocument> rawDoc(new DefaultRawDocument);
    rawDoc->setDocOperateType(ADD_DOC);

    ASSERT_FALSE(appender.Init(fieldConfigs));

    fieldConfigs[1]->SetEnableNullField(true);
    fieldConfigs[2]->SetEnableNullField(true);
    ASSERT_TRUE(appender.Init(fieldConfigs));
    appender.Append(rawDoc);
    ASSERT_TRUE(rawDoc->exist("long1"));
    ASSERT_EQ(string("__NULL__"), rawDoc->getField("long1"));

    ASSERT_TRUE(rawDoc->exist("body"));
    ASSERT_EQ(string("__NULL__"), rawDoc->getField("body"));

    ASSERT_FALSE(rawDoc->exist("string1"));
}
}} // namespace indexlibv2::document
