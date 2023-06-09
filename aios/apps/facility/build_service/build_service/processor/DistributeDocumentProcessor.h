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
#ifndef __DISTRIBUTE_DOCUMENT_PROCESSOR_H
#define __DISTRIBUTE_DOCUMENT_PROCESSOR_H

#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class DistributeDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string DISTRIBUTE_FIELD_NAMES;
    static const std::string DISTRIBUTE_RULE;
    static const std::string DEFAULT_CLUSTERS;
    static const std::string OTHER_CLUSTERS;
    static const std::string EMPTY_CLUSTERS;
    static const std::string ALL_CLUSTER;
    static const std::string FIELD_NAME_SEP;
    static const std::string KEY_VALUE_SEP;
    static const std::string CLUSTERS_SEP;

public:
    DistributeDocumentProcessor() : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC) {}
    ~DistributeDocumentProcessor()
    {
        // do nothing
    }

    bool init(const DocProcessorInitParam& param) override;
    bool process(const document::ExtendDocumentPtr& document) override;
    void destroy() override;
    DistributeDocumentProcessor* clone() override { return new DistributeDocumentProcessor(*this); }
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;

private:
    std::string _distributeFieldName;
    std::map<std::string, std::vector<std::string>> _rule;
    std::vector<std::string> _otherClusters;
    std::vector<std::string> _emptyClusters;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::processor

#endif
