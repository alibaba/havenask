#ifndef ISEARCH_QRSPROCESSOR_TOKEN_REWRITE_H
#define ISEARCH_QRSPROCESSOR_TOKEN_REWRITE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/common/Request.h>
#include "RewriteQueryTermVisitor.h"

BEGIN_HA3_NAMESPACE(qrs);
#define TOKEN_REWRITE_LIST "token_rewrite_list"
#define TOKEN_REWRITE_SEPARAROR ","
#define TOKEN_REWRITE_KEY_VALUE_SEPARATOR ":"

class QrsTokenRewriteProcessor : public QrsProcessor
{
    typedef std::map<std::string, std::string> TokenRewriteMap;
public:
    QrsTokenRewriteProcessor();
    virtual ~QrsTokenRewriteProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, common::ResultPtr &resultPtr);
    virtual bool init(const KeyValueMap &keyValues, config::ResourceReader * resourceReader);
    /** attention: you should assure the cloned 'QrsProcessor' is thread-safe. */
    virtual QrsProcessorPtr clone();

public:
    std::string getName() const {
        return "QrsTokenRewriteProcessor";
    }
private:
    void rewriteQueryToken(common::RequestPtr &requestPtr);
private:
    TokenRewriteMap _tokenRewriteMap;
private:
    bool initTokenRewriteMap(const KeyValueMap &kvMap,
                            config::ResourceReader * resourceReader);
    bool constructTokenRewriteMap(const std::string &content);
private:
     HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSPROCESSORSAMPLE_H
