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
#include "build_service/processor/LineDataDocumentProcessor.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/attribute/Common.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

using namespace indexlib::config;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, LineDataDocumentProcessor);

const string LineDataDocumentProcessor::PROCESSOR_NAME = "LineDataDocumentProcessor";

LineDataDocumentProcessor::LineDataDocumentProcessor() {}

LineDataDocumentProcessor::~LineDataDocumentProcessor() {}

LineDataDocumentProcessor::LineDataDocumentProcessor(const LineDataDocumentProcessor& other)
    : DocumentProcessor(other)
    , _schema(other._schema)
    , _fieldAttrName(other._fieldAttrName)
    , _hasAttrs(other._hasAttrs)
{
}

bool LineDataDocumentProcessor::init(const DocProcessorInitParam& param)
{
    _schema = param.schema;
    auto legacySchema = _schema->GetLegacySchema();
    auto fieldCount = legacySchema->GetFieldCount();
    _fieldAttrName.resize(fieldCount);
    auto attrSchema = legacySchema->GetAttributeSchema();
    if (!attrSchema) {
        BS_LOG(ERROR, "attribute schema is null");
        return false;
    }
    auto attrConfigs = attrSchema->GetAttributeConfigs();
    _hasAttrs = (attrConfigs.size() > 0);
    if (!_hasAttrs) {
        BS_LOG(ERROR, "attribute schema is null");
        return false;
    }
    for (const auto& attrConfig : attrConfigs) {
        auto attrName = attrConfig->GetAttrName();
        auto fieldConfigs = attrConfig->GetFieldConfigs();
        for (const auto& fieldConfig : fieldConfigs) {
            fieldid_t fieldId = fieldConfig->GetFieldId();
            assert(fieldId >= 0 && static_cast<size_t>(fieldId <= _fieldAttrName.size()));
            _fieldAttrName[fieldId] = attrName;
        }
    }
    return true;
}

bool LineDataDocumentProcessor::process(const document::ExtendDocumentPtr& document)
{
    if (!_hasAttrs) {
        BS_LOG(ERROR, "attribute schema is null");
        return false;
    }

    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    const auto& fieldConfigs = _schema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        assert(fieldId >= 0 && static_cast<size_t>(fieldId) <= _fieldAttrName.size());
        const auto& attrName = _fieldAttrName[fieldId];
        if (attrName.empty()) {
            continue;
        }
        if (rawDoc->exist(attrName)) {
            classifiedDoc->setAttributeField(fieldId, rawDoc->getField(attrName));
        }
    }
    return true;
}

void LineDataDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

}} // namespace build_service::processor
