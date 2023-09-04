#include "indexlib/test/test/document_parser_unittest.h"

#include "indexlib/test/document_parser.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocumentParserTest);

DocumentParserTest::DocumentParserTest() {}

DocumentParserTest::~DocumentParserTest() {}

void DocumentParserTest::CaseSetUp() {}

void DocumentParserTest::CaseTearDown() {}

void DocumentParserTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string docString = "cmd=add,pk=2,attr=2,ts=2,vs=2";
    RawDocumentPtr doc = DocumentParser::Parse(docString);
}
}} // namespace indexlib::test
