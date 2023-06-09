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
#include "RewriteTokenDocumentProcessor.h"
#include <indexlib/document/index_document/normal_document/field_builder.h>
#include <build_service/processor/TokenizeDocumentProcessor.h>
#include <autil/StringTokenizer.h>
using namespace std;

using namespace indexlib::config;
using namespace indexlib::document;

using namespace build_service::document;

namespace pluginplatform {
namespace processor_plugins {

BS_LOG_SETUP(processor_plugins, RewriteTokenDocumentProcessor);

RewriteTokenDocumentProcessor::RewriteTokenDocumentProcessor()
{
}

bool RewriteTokenDocumentProcessor::init(const build_service::KeyValueMap &kvMap, 
        const IndexPartitionSchemaPtr &schemaPtr,
        build_service::config::ResourceReader *resourceReader)
{
    BS_LOG(DEBUG, "RewriteTokenDocumentProcessor::init");
    _schemaPtr = schemaPtr;
    return initTokenRewriteMap(kvMap,resourceReader);
}

bool RewriteTokenDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    BS_LOG(DEBUG, "RewriteTokenDocumentProcessor::process");
    FieldSchemaPtr fieldSchema = _schemaPtr->GetFieldSchema();
    TokenizeDocumentPtr tokenizeDocument = document->getTokenizeDocument(); 
    assert(tokenizeDocument.get());
    assert(fieldSchema.get());

    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        TokenizeFieldPtr tokenizeFieldPtr = tokenizeDocument->getField(fieldId);
        if (!tokenizeFieldPtr.get()) {
            BS_LOG(WARN, "tokenizeFieldPtr == NULL");
            continue;
        }
        TokenizeField::Iterator sectionIt = tokenizeFieldPtr->createIterator();
        while(!sectionIt.isEnd()) {
            TokenizeSection::Iterator tokenIt = (*sectionIt)->createIterator();
            while(*tokenIt) {
                string word = string((*tokenIt)->getText().c_str());
                TokenRewriteMap::const_iterator rewriteIt = _tokenRewriteMap.find(word);
                if (rewriteIt != _tokenRewriteMap.end()) {
                    (*tokenIt)->setText(rewriteIt->second);
                    (*tokenIt)->setNormalizedText(rewriteIt->second);
                }
                tokenIt.next();
            }
            sectionIt.next();
        }
    }

    return true;
}

void RewriteTokenDocumentProcessor::destroy() 
{
    delete this;
}

bool RewriteTokenDocumentProcessor::initTokenRewriteMap(const build_service::KeyValueMap &kvMap, 
                             build_service::config::ResourceReader *resourceReader)
 {
     if (NULL == resourceReader) {
         return false;
     }
     string content;
     build_service::KeyValueMap::const_iterator it = kvMap.find(TOKEN_REWRITE_LIST);
     if (it != kvMap.end()) {
         if (resourceReader->getFileContent(content, it->second)) {
             return constructTokenRewriteMap(content);
         } else {
             BS_LOG(ERROR,"Can not read resource file:%s",it->second.c_str());
             return false;
         }
     } else {
         BS_LOG(ERROR,"Can not find the file!");
     }
    return true;
}

bool RewriteTokenDocumentProcessor::constructTokenRewriteMap(const std::string &content)
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
             BS_LOG(ERROR,"Can not parse key-value correctly!");
             return false;
         }
         _tokenRewriteMap.insert(make_pair(strTokenizer[0],
                         strTokenizer[1]));
     }
     return true;
}
}}
