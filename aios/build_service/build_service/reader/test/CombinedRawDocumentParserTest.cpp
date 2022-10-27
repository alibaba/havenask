#include "build_service/test/unittest.h"
#include "build_service/reader/CombinedRawDocumentParser.h"
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/document/RawDocumentHashMapManager.h"
#include "build_service/config/CLIOptionNames.h"
#include <swift/common/FieldGroupWriter.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;
BS_NAMESPACE_USE(proto);
BS_NAMESPACE_USE(config);
BS_NAMESPACE_USE(document);
SWIFT_USE_NAMESPACE(common);
IE_NAMESPACE_USE(document);

namespace build_service {
namespace reader {

class CombinedRawDocumentParserTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    std::string generateStandardDocString(
            const std::string &fieldSep,
            const std::string &keyValueSep,
            const KeyValueMap &kvMap);

    std::string generateFieldFilterDocString(const KeyValueMap& kvMap);
private:
    RawDocumentHashMapManagerPtr _hashMapManager;    
};

void CombinedRawDocumentParserTest::setUp() {
}

void CombinedRawDocumentParserTest::tearDown() {
}

string CombinedRawDocumentParserTest::generateStandardDocString(
        const string &fieldSep, const string &keyValueSep,
        const KeyValueMap &kvMap)
{
    string docStr;
    size_t i = 0;
    for (KeyValueMap::const_iterator it = kvMap.begin();
         it != kvMap.end(), i < kvMap.size(); it++, i++)
    {
        bool insertEmpty = rand() % 2 == 0;
        if (insertEmpty) {
            docStr += ' ';
        }
        docStr += it->first;
        if (insertEmpty) {
            docStr += '\n';
        }
        docStr += keyValueSep;
        docStr += it->second;
        if (i+1 != kvMap.size()) {
            docStr += fieldSep;
        }
    }
    return docStr;
}

string CombinedRawDocumentParserTest::generateFieldFilterDocString(
        const KeyValueMap& kvMap)
{
    FieldGroupWriter writer;
    for (const auto& kv : kvMap) {
        writer.addProductionField(kv.first, kv.second);
    }
    return writer.toString();
}

TEST_F(CombinedRawDocumentParserTest, testSimple) {
    string dsStr = R"(
    {
                "src" : "swift",
                "type" : "swift",
                "swift_root" : "zfs://xxx-swift/xxx-swift-service",
                "parser_config" : [
                    {
                        "type" : "swift_field_filter",
                        "parameters" : {}
                    },
                    {
                        "type" : "customized",
                        "parameters" : {
                            "field_separator" : ";",
                            "kv_separator" : "="                            
                        }
                    }        
                ]
     })";
    /// """
    KeyValueMap kvMap;
    kvMap[DATA_DESCRIPTION_KEY] = dsStr;

    ParserCreator creator;
    shared_ptr<CombinedRawDocumentParser> parser(
            ASSERT_CAST_AND_RETURN(CombinedRawDocumentParser,
                    creator.create(kvMap)));

    KeyValueMap fieldMap;
    fieldMap["f1"] = "v1";
    fieldMap["f2"] = "v2";
    fieldMap["f3"] = "v3";
    fieldMap["f4"] = "v4";

    string doc1 = generateStandardDocString(";", "=", fieldMap);
    string doc2 = generateFieldFilterDocString(fieldMap);
    DefaultRawDocument rawDocument(_hashMapManager);
    EXPECT_TRUE(parser->parse(doc1, rawDocument));
    for (const auto& kv : fieldMap) {
        EXPECT_EQ(kv.second, rawDocument.getField(kv.first));
    }
    rawDocument.clear();
    EXPECT_TRUE(parser->parse(doc2, rawDocument));
    for (const auto& kv : fieldMap) {
        EXPECT_EQ(kv.second, rawDocument.getField(kv.first)); 
    }    
}

TEST_F(CombinedRawDocumentParserTest, testParseWithFailedDoc) {
    string dsStr = R"(
    {
                "src" : "swift",
                "type" : "swift",
                "swift_root" : "zfs://xxx-swift/xxx-swift-service",
                "parser_config" : [
                    {
                        "type" : "swift_field_filter",
                        "parameters" : {}
                    },
                    {
                        "type" : "customized",
                        "parameters" : {
                            "field_separator" : ";",
                            "kv_separator" : "="                            
                        }
                    }        
                ]
     })";
    /// """
    KeyValueMap kvMap;
    kvMap[DATA_DESCRIPTION_KEY] = dsStr;

    ParserCreator creator;
    shared_ptr<CombinedRawDocumentParser> parser(
            ASSERT_CAST_AND_RETURN(CombinedRawDocumentParser,
                    creator.create(kvMap)));

    KeyValueMap fieldMap;
    fieldMap["f1"] = "v1";
    fieldMap["f2"] = "v2";
    fieldMap["f3"] = "v3";
    fieldMap["f4"] = "v4";

    auto innerCheckParseResult = [&fieldMap, &parser, this] (
            const string& docStr, bool expectRet, size_t expectParserIdx)
    {
        EXPECT_EQ(expectParserIdx, parser->_lastParser);
        DefaultRawDocument rawDoc(_hashMapManager);
        ASSERT_EQ(expectRet, parser->parse(docStr, rawDoc));
        if (expectRet == true) {
            EXPECT_EQ(fieldMap.size() + 1, rawDoc.getFieldCount());
            for (const auto& kv : fieldMap) {
                EXPECT_EQ(kv.second, rawDoc.getField(kv.first));                 
            }
        }
    };
    {
        string docStr = generateFieldFilterDocString(fieldMap);
        innerCheckParseResult(docStr, true, 0);        
    }
    fieldMap.clear();
    fieldMap["f5"] = "v5";
    fieldMap["f6"] = "v6";
    fieldMap["f7"] = "v7";
    {
        string docStr = generateFieldFilterDocString(fieldMap);
        innerCheckParseResult(docStr, true, 0);         
    }
    fieldMap.clear();
    fieldMap["f8"] = "v8";
    fieldMap["f9"] = "v9";
    {
        string docStr = generateStandardDocString(";", "=", fieldMap);
        innerCheckParseResult(docStr, true, 0); 
    }
    fieldMap.clear();
    fieldMap["f10"] = "v10";
    fieldMap["f11"] = "v11";    
    {
        string docStr = generateStandardDocString(";", "=", fieldMap);
        innerCheckParseResult(docStr, true, 1); 
    }
    {
        string errorDoc = generateStandardDocString("%", "---", fieldMap);
        innerCheckParseResult(errorDoc, false, 1); 
    }
    fieldMap.clear();
    fieldMap["f12"] = "v12";
    fieldMap["f13"] = "v13"; 
    {
        string docStr = generateFieldFilterDocString(fieldMap);
        innerCheckParseResult(docStr, true, 1); 
    }
    {
        string docStr = generateFieldFilterDocString(fieldMap);
        innerCheckParseResult(docStr, true, 0); 
    }        
}

}
}
