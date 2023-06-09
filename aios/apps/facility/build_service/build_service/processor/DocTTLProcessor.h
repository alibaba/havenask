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
#ifndef ISEARCH_BS_DOCTTLPROCESSOR_H
#define ISEARCH_BS_DOCTTLPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class DocTTLProcessor : public DocumentProcessor
{
public:
    DocTTLProcessor();
    ~DocTTLProcessor();

public:
    virtual bool init(const DocProcessorInitParam& param);
    virtual bool process(const document::ExtendDocumentPtr& document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs);
    virtual DocumentProcessor* clone();
    virtual std::string getDocumentProcessorName() const { return "DocTTLProcessor"; }

private:
    uint32_t _ttlTime;
    std::string _ttlFieldName;

public:
    static const std::string PROCESSOR_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocTTLProcessor);

}} // namespace build_service::processor

#endif // ISEARCH_BS_DOCTTLPROCESSOR_H
