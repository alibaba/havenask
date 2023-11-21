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

#include <iconv.h>
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class EncodingConvertDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string SRC_ENCODING;
    static const std::string DST_ENCODING;
    static const std::string TO_CONVERT_FIELDS;
    static const std::string IGNORE_ERROR;
    static const std::string FIELD_NAME_SEP;

public:
    EncodingConvertDocumentProcessor();
    EncodingConvertDocumentProcessor(const EncodingConvertDocumentProcessor& other);
    ~EncodingConvertDocumentProcessor();

private:
    EncodingConvertDocumentProcessor& operator=(const EncodingConvertDocumentProcessor&);

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam& param) override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    bool initIconv();
    void processOneField(const document::RawDocumentPtr& rawDoc, const std::string& fieldName);

private:
    typedef std::vector<std::string> FieldNameVector;
    FieldNameVector _toConvertFieldNames;
    std::string _srcEncoding;
    std::string _dstEncoding;
    iconv_t _iconvDesc;

private:
    static const size_t MAX_BUF_SIZE = 128 * 1024;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EncodingConvertDocumentProcessor);

}} // namespace build_service::processor
