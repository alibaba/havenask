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
#pragma once

#include <set>

#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace build_service { namespace processor {
class TokenCombinedDocumentProcessor : public DocumentProcessor
{
public:
    TokenCombinedDocumentProcessor();
    TokenCombinedDocumentProcessor(const TokenCombinedDocumentProcessor& other);
    ~TokenCombinedDocumentProcessor() override;

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam& param) override;

    std::string getDocumentProcessorName() const override { return "TokenCombinedDocumentProcessor"; }

public: // for test
    void setPrefixStopwordSet(const std::set<std::string>& prefixTokenSet);

private:
    void processField(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                      const document::TokenizeDocumentPtr& tokenizeDocument);
    void fillStopTokenSet(const KeyValueMap& kvMap, const std::string& stopTokenIdentifier,
                          std::set<std::string>& stopTokenSet);
    void processSection(document::TokenizeSection* section) const;
    void combinePreToken(document::TokenizeSection::Iterator& preIt, document::TokenizeSection::Iterator& curIt,
                         document::TokenizeSection* section) const;
    void combineNextToken(document::TokenizeSection::Iterator& curIt, document::TokenizeSection::Iterator& nextIt,
                          document::TokenizeSection* section) const;

public:
    static const std::string PREFIX_IDENTIFIER;
    static const std::string SUFFIX_IDENTIFIER;
    static const std::string INFIX_IDENTIFIER;

    static const std::string PROCESSOR_NAME;

    std::set<std::string> _prefixTokenSet;
    std::set<std::string> _suffixTokenSet;
    std::set<std::string> _infixTokenSet;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;

    BS_LOG_DECLARE();
};

}} // namespace build_service::processor
