#ifndef __INDEXLIB_EXAMPLE_SRC_SCHEMA_PARSER_H
#define __INDEXLIB_EXAMPLE_SRC_SCHEMA_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class ExampleSrcSchemaParser : public SourceSchemaParser
{
public:
    ExampleSrcSchemaParser();
    ~ExampleSrcSchemaParser();

public:
    bool Init(const KeyValueMap& initParam) override;
    bool CreateDocSrcSchema(const std::shared_ptr<indexlibv2::document::RawDocument>& rawDoc,
                            std::vector<std::string>& sourceSchema) const override;

private:
    std::string mTargetField;
    std::string mSeparator;

private:
    IE_LOG_DECLARE();
};

class ExampleSrcSchemaParserFactory : public SourceSchemaParserFactory
{
public:
    ExampleSrcSchemaParserFactory() {}
    ~ExampleSrcSchemaParserFactory() override {}

public:
    void destroy() override { delete this; }

public:
    SourceSchemaParser* CreateSourceSchemaParser() override;

public:
    const static std::string FACTORY_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExampleSrcSchemaParser);
}} // namespace indexlib::document

#endif //__INDEXLIB_EXAMPLE_SRC_SCHEMA_PARSER_H
