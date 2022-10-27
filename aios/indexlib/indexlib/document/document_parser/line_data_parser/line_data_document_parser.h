#ifndef __INDEXLIB_LINE_DATA_DOCUMENT_PARSER_H
#define __INDEXLIB_LINE_DATA_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser.h"

IE_NAMESPACE_BEGIN(document);

class LineDataDocumentParser : public DocumentParser
{
public:
    LineDataDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~LineDataDocumentParser();
    
public:
    bool DoInit() override;
    DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) override;
    DocumentPtr Parse(const autil::ConstString& serializedData) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineDataDocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_LINE_DATA_DOCUMENT_PARSER_H
