#ifndef __INDEXLIB_NORMAL_DOCUMENT_PARSER_H
#define __INDEXLIB_NORMAL_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/document_parser/normal_parser/single_document_parser.h"
#include "indexlib/misc/monitor.h"

IE_NAMESPACE_BEGIN(document);

// TODO: add test case
class NormalDocumentParser : public DocumentParser
{
public:
    NormalDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~NormalDocumentParser();
    
public:
    bool DoInit() override;
    DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) override;
    DocumentPtr Parse(const autil::ConstString& serializedData) override;

private:
    bool IsEmptyUpdate(const NormalDocumentPtr& doc);
    bool InitFromDocumentParam();
    
private:
    SingleDocumentParserPtr mMainParser;
    SingleDocumentParserPtr mSubParser;
    std::vector<DocumentRewriterPtr> mDocRewriters;
    IE_DECLARE_METRIC(UselessUpdateQps);
    IE_NAMESPACE(util)::AccumulativeCounterPtr mAttributeConvertErrorCounter;

public:
    static std::string ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalDocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_NORMAL_DOCUMENT_PARSER_H
