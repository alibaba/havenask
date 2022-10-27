#ifndef __INDEXLIB_RAW_DOCUMENT_PARSER_H
#define __INDEXLIB_RAW_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/raw_document.h"

IE_NAMESPACE_BEGIN(document);

class RawDocumentParser
{
public:
    struct Message {
        std::string data;
        int64_t timestamp = 0;
    };
public:
    RawDocumentParser() {}
    virtual ~RawDocumentParser() {}
    
private:
    RawDocumentParser(const RawDocumentParser &);
    RawDocumentParser& operator=(const RawDocumentParser &);
    
public:
    virtual bool parse(const std::string& docString, document::RawDocument& rawDoc) = 0;
    virtual bool parseMultiMessage(const std::vector<Message>& msgs, document::RawDocument& rawDoc) { return false; }
};

DEFINE_SHARED_PTR(RawDocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_RAW_DOCUMENT_PARSER_H
