#ifndef __REWRITE_TOKEN_DOCUMENT_PROCESSOR_H
#define __REWRITE_TOKEN_DOCUMENT_PROCESSOR_H

#include <ha3/common.h>
#include <build_service/processor/DocumentProcessor.h>
#include <build_service/config/ResourceReader.h>
#include <tr1/memory>

namespace build_service {
namespace processor {

#define TOKEN_REWRITE_LIST "token_rewrite_list"
#define TOKEN_REWRITE_SEPARAROR ","
#define TOKEN_REWRITE_KEY_VALUE_SEPARATOR ":"

class RewriteTokenDocumentProcessor : public DocumentProcessor
{
    typedef std::map<std::string, std::string> TokenRewriteMap;
public:
    RewriteTokenDocumentProcessor();
    ~RewriteTokenDocumentProcessor() {}
public:
    /* override */ bool process(const document::ExtendDocumentPtr& document);
    /* override */ void destroy();
    RewriteTokenDocumentProcessor* clone() {return new RewriteTokenDocumentProcessor(*this);}
    /* override */ bool init(const KeyValueMap &kvMap, 
                             const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr, 
                             config::ResourceReader * resourceReader);
                         
    /* override */ std::string getDocumentProcessorName() const {return "RewriteTokenDocumentProcessor";}

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;
    TokenRewriteMap _tokenRewriteMap;
private:
    bool initTokenRewriteMap(const KeyValueMap &kvMap,
                             config::ResourceReader * resourceReader);
    bool constructTokenRewriteMap(const std::string &content);
private:
    BS_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessor> RewriteTokenDocumentProcessorPtr;

};
};

#endif //__BUILD_DOCUMENT_PROCESSOR_H
