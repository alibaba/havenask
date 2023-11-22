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
#include "build_service/processor/HashDocumentProcessor.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <set>

#include "alog/Logger.h"
#include "autil/HashFuncFactory.h"
#include "build_service/config/HashMode.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/document/RawDocument.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/region_schema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

using namespace indexlib::config;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, HashDocumentProcessor);

const string HashDocumentProcessor::PROCESSOR_NAME = "HashDocumentProcessor";

HashDocumentProcessor::HashDocumentProcessor() : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

HashDocumentProcessor::~HashDocumentProcessor() {}

bool HashDocumentProcessor::init(const DocProcessorInitParam& param)
{
    ResourceReader* resourceReader = param.resourceReader;
    if (!param.schema) {
        BS_LOG(ERROR, "schema is null");
        return false;
    }
    const auto& legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(param.schema);
    if (legacySchemaAdapter) {
        _schema = legacySchemaAdapter->GetLegacySchema();
        _regionCount = _schema->GetRegionCount();
    }
    _clusterNames = param.clusterNames;
    // notice: if configurate multi cluster, they must share the same schema
    for (const auto& clusterName : param.clusterNames) {
        string relativePath = ResourceReader::getClusterConfRelativePath(clusterName);
        HashModeConfig hashModeConfig;
        if (!hashModeConfig.init(clusterName, resourceReader, param.schema)) {
            BS_LOG(ERROR, "create hashModeConfig failed");
            return false;
        }
        ClusterHashMeta meta;
        if (!createClusterHashMeta(hashModeConfig, meta)) {
            BS_LOG(ERROR, "create clusterHashMeta failed");
            return false;
        }
        _clusterHashMetas.push_back(meta);
    }

    if (!param.processForSubDoc) {
        _hashFieldErrorCounter = GET_ACC_COUNTER(param.counterMap, processor.hashFieldEmptyError);
        if (!_hashFieldErrorCounter) {
            BS_LOG(WARN, "create attributeConvertErrorCounter failed");
        }
    }
    return true;
}
bool HashDocumentProcessor::addHashInfo(const HashModeConfig& hashModeConfig, const std::string& regionName,
                                        ClusterHashMeta& clusterHashMeta)
{
    HashMode hashMode;
    if (!hashModeConfig.getHashMode(regionName, hashMode)) {
        BS_LOG(ERROR, "get HashMode failed for region [%s]", regionName.c_str());
        return false;
    }
    auto hashFuncPtr = HashFuncFactory::createHashFunc(hashMode._hashFunction, hashMode._hashParams);
    if (!hashFuncPtr) {
        string errorMsg = "invalid hash function[" + hashMode._hashFunction + "] for cluster[" +
                          hashModeConfig.getClusterName() + "] region_name[" + regionName + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    clusterHashMeta.appendHashInfo(hashFuncPtr, hashMode._hashFields);
    return true;
}

bool HashDocumentProcessor::createClusterHashMeta(const HashModeConfig& hashModeConfig,
                                                  ClusterHashMeta& clusterHashMeta)
{
    if (_schema) {
        for (size_t regionId = 0; regionId < _schema->GetRegionCount(); regionId++) {
            string regionName = _schema->GetRegionSchema(regionId)->GetRegionName();
            if (!addHashInfo(hashModeConfig, regionName, clusterHashMeta)) {
                return false;
            }
        }
    } else {
        if (!addHashInfo(hashModeConfig, indexlib::DEFAULT_REGIONNAME, clusterHashMeta)) {
            return false;
        }
    }
    clusterHashMeta.clusterName = hashModeConfig.getClusterName();
    return true;
}

void HashDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool HashDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    const ProcessedDocumentPtr& processedDocPtr = document->getProcessedDocument();
    const RawDocumentPtr& rawDocPtr = document->getRawDocument();
    // TODO add validate code in region_processor
    regionid_t regionId = document->getRegionId();
    if (regionId < 0 || regionId >= _regionCount) {
        BS_LOG(ERROR, "invalid regionId [%d]", regionId);
        return false;
    }
    vector<string> hashFieldValues;
    for (auto& clusterHashMeta : _clusterHashMetas) {
        hashFieldValues.clear();
        HashFunctionBasePtr hashFuncBase = clusterHashMeta.getHashFunc(regionId);
        const set<string>& canEmptyFields = hashFuncBase->getCanEmptyFields();
        const vector<string>& hashFields = clusterHashMeta.getHashField(regionId);
        bool hasEmptyHashField = false;
        hashFieldValues.reserve(hashFields.size());
        for (size_t i = 0; i < hashFields.size(); i++) {
            const string& value = rawDocPtr->getField(hashFields[i]);
            if (value == EMPTY_STRING && canEmptyFields.count(hashFields[i]) == 0) {
                hasEmptyHashField = true;
            }
            hashFieldValues.push_back(value);
        }
        if (hasEmptyHashField) {
            document->setWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY);
            if (_hashFieldErrorCounter) {
                _hashFieldErrorCounter->Increase(1);
            }
        }
        ProcessedDocument::DocClusterMeta meta;
        meta.clusterName = clusterHashMeta.clusterName;
        meta.hashId = hashFuncBase->getHashId(hashFieldValues);
        processedDocPtr->addDocClusterMeta(meta);
    }
    return true;
}

void HashDocumentProcessor::destroy() { delete this; }

DocumentProcessor* HashDocumentProcessor::clone() { return new HashDocumentProcessor(*this); }

}} // namespace build_service::processor
