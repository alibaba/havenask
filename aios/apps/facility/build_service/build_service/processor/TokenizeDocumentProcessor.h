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

#include "build_service/analyzer/Token.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/normal/TokenizeDocumentConvertor.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::analyzer {
class Analyzer;
}

namespace build_service { namespace analyzer {
class AnalyzerFactory;
}} // namespace build_service::analyzer

namespace build_service { namespace processor {
class TokenizeDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;

public:
    TokenizeDocumentProcessor();
    TokenizeDocumentProcessor(const TokenizeDocumentProcessor& other);
    ~TokenizeDocumentProcessor();

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;
    DocumentProcessor* clone() override;

    bool init(const DocProcessorInitParam& param) override;

    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

    const std::shared_ptr<indexlibv2::document::TokenizeDocumentConvertor>& TEST_getImpl() const { return _impl; }

private:
    std::shared_ptr<indexlibv2::document::TokenizeDocumentConvertor> _impl;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TokenizeDocumentProcessor);

}} // namespace build_service::processor
