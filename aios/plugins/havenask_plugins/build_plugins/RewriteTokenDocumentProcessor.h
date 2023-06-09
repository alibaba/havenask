/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <build_service/processor/DocumentProcessor.h>
#include <build_service/config/ResourceReader.h>
#include <tr1/memory>

namespace pluginplatform {
namespace processor_plugins {

#define TOKEN_REWRITE_LIST "token_rewrite_list"
#define TOKEN_REWRITE_SEPARAROR ","
#define TOKEN_REWRITE_KEY_VALUE_SEPARATOR ":"

class RewriteTokenDocumentProcessor : public build_service::processor::DocumentProcessor
{
    typedef std::map<std::string, std::string> TokenRewriteMap;
public:
    RewriteTokenDocumentProcessor();
    ~RewriteTokenDocumentProcessor() {}
public:
    /* override */ bool process(const build_service::document::ExtendDocumentPtr& document);
    /* override */ void destroy();
    RewriteTokenDocumentProcessor* clone() {return new RewriteTokenDocumentProcessor(*this);}
    /* override */ bool init(const build_service::KeyValueMap &kvMap, 
                             const indexlib::config::IndexPartitionSchemaPtr &schemaPtr, 
                             build_service::config::ResourceReader * resourceReader);
                         
    /* override */ std::string getDocumentProcessorName() const {return "RewriteTokenDocumentProcessor";}

private:
    indexlib::config::IndexPartitionSchemaPtr _schemaPtr;
    TokenRewriteMap _tokenRewriteMap;
private:
    bool initTokenRewriteMap(const build_service::KeyValueMap &kvMap,
                             build_service::config::ResourceReader * resourceReader);
    bool constructTokenRewriteMap(const std::string &content);
private:
    BS_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessor> RewriteTokenDocumentProcessorPtr;

}}
