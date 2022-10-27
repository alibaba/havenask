#include "QrsTokenRewriteProcessor.h"
#include <ha3/util/FileUtil.h>
#include <autil/StringUtil.h>
#include "RewriteQueryTermVisitor.h"
#include <autil/StringTokenizer.h>

using namespace std;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QrsTokenRewriteProcessor);
QrsTokenRewriteProcessor::QrsTokenRewriteProcessor() 
{ 
}

QrsTokenRewriteProcessor::~QrsTokenRewriteProcessor() { 
}

QrsProcessorPtr QrsTokenRewriteProcessor::clone() {
    QrsProcessorPtr ptr(new QrsTokenRewriteProcessor(*this));
    return ptr;
}

void QrsTokenRewriteProcessor::process(RequestPtr &requestPtr, 
                                 ResultPtr &resultPtr) 
{
    rewriteQueryToken(requestPtr);
    /* call QrsProcessor::process() function to search. */
    QrsProcessor::process(requestPtr, resultPtr);
}

void QrsTokenRewriteProcessor::rewriteQueryToken(RequestPtr &requestPtr){
    HA3_LOG(DEBUG, "QrsTokenRewriteProcessor::rewriteQueryToken");
    RewriteQueryTermVisitor termVisitor(_tokenRewriteMap);
    assert(requestPtr->getQueryClause());
    Query *query = requestPtr->getQueryClause()->getRootQuery();
    assert(query);
    query->accept(&termVisitor);
 
    RewriteQueryTermVisitor childTermVisitor(_tokenRewriteMap);
    childTermVisitor.visitAdvancedQuery(query);
}

bool QrsTokenRewriteProcessor::init(const KeyValueMap &keyValues,
                                    config::ResourceReader * resourceReader) 
{
    initTokenRewriteMap(keyValues,resourceReader);
    return true;
}

bool QrsTokenRewriteProcessor::initTokenRewriteMap(const KeyValueMap &kvMap, 
                             config::ResourceReader *resourceReader)
 {
     if (NULL == resourceReader) {
         return false;
     }
     string content;
     KeyValueMap::const_iterator it = kvMap.find(TOKEN_REWRITE_LIST);
     if (it != kvMap.end()) {
         if (resourceReader->getFileContent(content, it->second)) {
             return constructTokenRewriteMap(content);
         } else {
             HA3_LOG(ERROR,"Can not read resource file:%s",it->second.c_str());
             return false;
         }
     } else {
         HA3_LOG(ERROR,"Can not find the file!");
     }
    return true;
}

bool QrsTokenRewriteProcessor::constructTokenRewriteMap(const std::string &content)
{
     autil::StringTokenizer stringTokenizer(content,
           TOKEN_REWRITE_SEPARAROR, 
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
     for(size_t i=0; i<stringTokenizer.getNumTokens();i++)
     {
         autil::StringTokenizer strTokenizer(stringTokenizer[i],
            TOKEN_REWRITE_KEY_VALUE_SEPARATOR, 
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
         if(strTokenizer.getNumTokens() != (size_t)2){
             HA3_LOG(ERROR,"Can not parse key-value correctly!");
             return false;
         }
         _tokenRewriteMap.insert(make_pair(strTokenizer[0],
                         strTokenizer[1]));
     }
     return true;
}

END_HA3_NAMESPACE(qrs);

