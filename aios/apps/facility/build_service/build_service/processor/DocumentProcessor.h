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

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/plugin/PooledObject.h"
#include "build_service/util/Log.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/common.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service { namespace config {
class ResourceReader;
}} // namespace build_service::config

namespace build_service { namespace analyzer {
class AnalyzerFactory;
BS_TYPEDEF_PTR(AnalyzerFactory);
}} // namespace build_service::analyzer

namespace indexlib { namespace util {
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace processor {

struct DocProcessorInitParam {
    KeyValueMap parameters;
    indexlib::config::IndexPartitionSchemaPtr schemaPtr;
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema;
    config::ResourceReader* resourceReader = nullptr;
    analyzer::AnalyzerFactory* analyzerFactory = nullptr;
    std::vector<std::string> clusterNames;
    indexlib::util::MetricProviderPtr metricProvider;
    indexlib::util::CounterMapPtr counterMap;
    bool processForSubDoc = false;
};

class DocumentProcessor
{
public:
    DocumentProcessor(int docOperateFlag = ADD_DOC) : _docOperateFlag(docOperateFlag) {}
    virtual ~DocumentProcessor() {}

public:
    // only return false when process failed
    virtual bool process(const document::ExtendDocumentPtr& document) = 0;

    // user should use self-implement batchProcess for performance
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs)
    {
        for (size_t i = 0; i < docs.size(); i++) {
            bool ret = false;
            if ((i + 1) == docs.size()) {
                ret = process(docs[i]);
            } else {
                DocumentProcessor* cloneDocProcessor = allocate();
                if (cloneDocProcessor) {
                    ret = cloneDocProcessor->process(docs[i]);
                    cloneDocProcessor->deallocate();
                }
            }
            if (!ret) {
                docs[i]->setWarningFlag(document::ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
            }
        }
    }

    virtual void destroy() { delete this; }
    virtual DocumentProcessor* clone() = 0;
    virtual bool init(const DocProcessorInitParam& param)
    {
        return init(param.parameters, param.schemaPtr, param.resourceReader);
    }
    virtual std::string getDocumentProcessorName() const { return "DocumentProcessor"; }

public:
    bool needProcess(DocOperateType docOperateType) const { return (_docOperateFlag & docOperateType) != 0; }

public:
    // compatiblity for old implement
    virtual bool init(const KeyValueMap& kvMap, const indexlib::config::IndexPartitionSchemaPtr& schemaPtr,
                      config::ResourceReader* resourceReader)
    {
        return true;
    }

public:
    // for ha3
    virtual DocumentProcessor* allocate() { return clone(); }
    virtual void deallocate() { destroy(); }

protected:
    int _docOperateFlag;
};

typedef std::shared_ptr<DocumentProcessor> DocumentProcessorPtr;
typedef plugin::PooledObject<DocumentProcessor> PooledDocumentProcessor;

}} // namespace build_service::processor
