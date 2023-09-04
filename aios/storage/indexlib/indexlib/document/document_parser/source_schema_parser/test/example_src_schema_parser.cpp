#include "indexlib/document/document_parser/source_schema_parser/test/example_src_schema_parser.h"

#include "autil/ConstString.h"
#include "autil/StringTokenizer.h"
#include "indexlib/document/raw_document.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, ExampleSrcSchemaParser);

ExampleSrcSchemaParser::ExampleSrcSchemaParser() {}

ExampleSrcSchemaParser::~ExampleSrcSchemaParser() {}

bool ExampleSrcSchemaParser::Init(const KeyValueMap& initParam)
{
    auto iter = initParam.find("target_field");
    if (iter == initParam.end()) {
        return false;
    }
    mTargetField = iter->second;
    iter = initParam.find("separator");
    mSeparator = iter->second;
    return true;
}

bool ExampleSrcSchemaParser::CreateDocSrcSchema(const RawDocumentPtr& rawDoc, vector<string>& sourceSchema) const
{
    string value = rawDoc->getField(mTargetField);
    if (value.empty()) {
        return false;
    }
    sourceSchema = StringTokenizer::tokenize(StringView(value), mSeparator);
    return true;
}

SourceSchemaParser* ExampleSrcSchemaParserFactory::CreateSourceSchemaParser() { return new ExampleSrcSchemaParser(); }

extern "C" plugin::ModuleFactory* createSourceSchemaParserFactory() { return new ExampleSrcSchemaParserFactory; }

extern "C" void destroySourceSchemaParserFactory(plugin::ModuleFactory* factory) { factory->destroy(); }
}} // namespace indexlib::document
