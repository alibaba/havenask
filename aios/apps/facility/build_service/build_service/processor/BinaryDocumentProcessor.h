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
#ifndef ISEARCH_BS_BINARYDOCUMENTPROCESSOR_H
#define ISEARCH_BS_BINARYDOCUMENTPROCESSOR_H

#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace processor {

class BinaryDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string BINARY_FIELD_NAMES;
    static const std::string FIELD_NAME_SEP;

public:
    BinaryDocumentProcessor();
    ~BinaryDocumentProcessor();

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam& param) override;

    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    void processBinaryField(const document::RawDocumentPtr& rawDoc, const std::string& fieldName);
    bool base64Decode(const std::string& input, std::string& output);

private:
    typedef std::vector<std::string> FieldNameVector;
    FieldNameVector _binaryFieldNames;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BinaryDocumentProcessor);

}} // namespace build_service::processor

#endif // ISEARCH_BS_BINARYDOCUMENTPROCESSOR_H
