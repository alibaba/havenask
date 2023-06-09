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
#include "build_service/processor/SourceFieldExtractorProcessor.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace build_service::document;

namespace build_service::processor {

BS_LOG_SETUP(processor, SourceFieldExtractorProcessor);

SourceFieldExtractorProcessor::SourceFieldExtractorProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

bool SourceFieldExtractorProcessor::init(const DocProcessorInitParam& param)
{
    if (!param.schema) {
        BS_LOG(ERROR, "schema is null");
        return false;
    }

    auto fieldConfigs = param.schema->GetFieldConfigs();

    _fieldName2Param.reset(new SourceFieldParams);
    // cache user defined param
    std::string allSourceFields;
    std::map<std::string, std::pair<std::string, std::string>> sourceField2Separators;
    for (const auto& fieldConfig : fieldConfigs) {
        if (!util::SourceFieldExtractorUtil::needExtractSourceField(fieldConfig)) {
            // normal field
            continue;
        }
        auto param = std::make_shared<util::SourceFieldExtractorUtil::SourceFieldParam>();
        if (!util::SourceFieldExtractorUtil::getSourceFieldParam(fieldConfig, param)) {
            BS_LOG(ERROR, "get source field param failed.");
            return false;
        }
        if (!util::SourceFieldExtractorUtil::checkUnifiedSeparator(*param, sourceField2Separators)) {
            BS_LOG(ERROR, "check failed.");
            return false;
        }
        auto fieldName = fieldConfig->GetFieldName();
        (*_fieldName2Param)[fieldName] = param;
        allSourceFields += fieldName + ", ";
    }

    if (param.metricProvider) {
        _sourceFieldQpsMetric =
            DECLARE_METRIC(param.metricProvider, "basic/sourceFieldExtractorQps", kmonitor::QPS, "count");
    }

    BS_LOG(INFO, "init source field extractor processor success, all source fields[%s]", allSourceFields.c_str());
    return true;
}

// the format of userDefinedParam like this:
// "user_defined_param" : {
//     "source_field" : {
//         "field_name" : "attributes",
//         "key" : "demo",
//         "kv_pair_separator" : ";",
//         "kv_separator" : ":"
//     }
//  }
bool SourceFieldExtractorProcessor::process(const std::shared_ptr<ExtendDocument>& document)
{
    util::SourceFieldMap sourceField2ParsedInfo;
    const auto& rawDoc = document->getRawDocument();
    for (const auto& [fieldName, param] : *_fieldName2Param) {
        const std::string& field = param->fieldName;
        auto it = sourceField2ParsedInfo.find(field);
        if (it == sourceField2ParsedInfo.end()) {
            auto value = rawDoc->getField(field);
            if (value.empty()) {
                BS_INTERVAL_LOG2(60, WARN, "source field[%s] not exist in raw doc.", field.c_str());
                continue;
            }
            util::SourceFieldParsedInfo parsedInfo;
            parsedInfo.first = std::move(value);
            auto ret = util::SourceFieldExtractorUtil::parseRawString(*param, parsedInfo);
            if (!ret) {
                BS_INTERVAL_LOG2(30, WARN, "parse source field value failed, fieldName[%s]", fieldName.c_str());
                continue;
            }
            it = sourceField2ParsedInfo.insert(it, std::make_pair(field, std::move(parsedInfo)));
        }
        std::string fieldValue;
        if (!util::SourceFieldExtractorUtil::getValue(it->second, *param, fieldValue)) {
            BS_INTERVAL_LOG2(60, WARN, "key[%s] not exist in source field.", param->key.c_str());
            continue;
        }
        rawDoc->setField(fieldName, fieldValue);

        if (_sourceFieldQpsMetric != nullptr) {
            INCREASE_QPS(_sourceFieldQpsMetric);
        }
    }
    return true;
}

void SourceFieldExtractorProcessor::batchProcess(const std::vector<std::shared_ptr<ExtendDocument>>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

SourceFieldExtractorProcessor* SourceFieldExtractorProcessor::clone()
{
    return new SourceFieldExtractorProcessor(*this);
}

void SourceFieldExtractorProcessor::destroy() { delete this; }

} // namespace build_service::processor
