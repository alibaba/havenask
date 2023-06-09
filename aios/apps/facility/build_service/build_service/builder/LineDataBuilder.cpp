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
#include "build_service/builder/LineDataBuilder.h"

#include "build_service/config/ConfigDefine.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::util;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, LineDataBuilder);

LineDataBuilder::LineDataBuilder(const string& indexPath, const indexlib::config::IndexPartitionSchemaPtr& schema,
                                 fslib::fs::FileLock* fileLock, const std::string& epochId)
    : Builder(indexlib::partition::IndexBuilderPtr(), fileLock, proto::BuildId())
    , _indexPath(indexPath)
    , _schema(schema)
    , _counterMap(new indexlib::util::CounterMap())
    , _epochId(epochId)
{
}

LineDataBuilder::~LineDataBuilder() {}

bool LineDataBuilder::init(const BuilderConfig& builderConfig, indexlib::util::MetricProviderPtr metricProvider)
{
    if (!Builder::init(builderConfig, metricProvider)) {
        return false;
    }
    _totalDocCountCounter = GET_ACC_COUNTER(_counterMap, totalDocCount);
    if (!_totalDocCountCounter) {
        BS_LOG(ERROR, "can not get totalDocCountCounter form counterMap");
    }
    if (!_schema->GetValueFromUserDefinedParam(BS_FIELD_SEPARATOR, _fieldSeperator)) {
        BS_LOG(ERROR, "[%s] not found in schema", BS_FIELD_SEPARATOR.c_str());
        return false;
    }
    auto& param = _schema->GetUserDefinedParam();
    param[BS_FILE_NAME] = autil::legacy::Any(string(BS_LINE_DATA));
    FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(_indexPath, _epochId));
    if (!storeSchema(_indexPath, _schema, fenceContext.get())) {
        return false;
    }
    if (!_writer) {
        _writer.reset(new BufferedFileWriter);
    }
    string filePath = fslib::fs::FileSystem::joinFilePath(_indexPath, BS_LINE_DATA);
    if (auto ec = _writer->Open(filePath, filePath).Code(); ec != FSEC_OK) {
        BS_LOG(ERROR, "writer open [%s] failed, ec [%d]", filePath.c_str(), ec);
        return false;
    }
    return true;
}

bool LineDataBuilder::build(const indexlib::document::DocumentPtr& doc)
{
    bool buildSuccess = true;
    try {
        string oneLineDoc;
        if (prepareOne(doc, oneLineDoc)) {
            _writer->Write(oneLineDoc.c_str(), oneLineDoc.size()).GetOrThrow();
            ;
        } else {
            buildSuccess = false;
        }
    } catch (const exception& e) {
        BS_LOG(ERROR, "exception happend [%s]", e.what());
        setFatalError();
        buildSuccess = false;
    } catch (...) {
        BS_LOG(ERROR, "unknown exception happend");
        setFatalError();
        buildSuccess = false;
    }
    if (buildSuccess && _totalDocCountCounter) {
        _totalDocCountCounter->Increase(1);
    }
    _builderMetrics.reportMetrics(buildSuccess, doc->GetDocOperateType());
    return buildSuccess;
}

void LineDataBuilder::stop(int64_t stopTimestamp)
{
    auto ret = _writer->Close();
    if (!ret.OK()) {
        BS_LOG(ERROR, "Close writer failed, ec[%d]", ret.Code());
        setFatalError();
    }
}

bool LineDataBuilder::prepareOne(const indexlib::document::DocumentPtr& document, string& oneLineDoc)
{
    oneLineDoc.clear();
    const AttributeSchemaPtr& attrSchema = _schema->GetAttributeSchema();
    if (!attrSchema) {
        BS_LOG(ERROR, "attribute schema is null");
        return false;
    }

    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    auto attrDoc = doc->GetAttributeDocument();
    if (!attrDoc) {
        BS_LOG(ERROR, "attribute document is null");
        return false;
    }
    uint32_t count = attrSchema->GetAttributeCount();
    for (uint32_t fieldCounter = 0; fieldCounter < count; ++fieldCounter) {
        const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(fieldCounter);
        fieldid_t fieldId = attrConfig->GetFieldId();
        const StringView& field = attrDoc->GetField(fieldId);
        // TODO, get seperator from schema
        if (fieldCounter != 0) {
            oneLineDoc.append(_fieldSeperator);
        }
        oneLineDoc.append(field.data(), field.size());
    }
    oneLineDoc.append(1, '\n');
    return true;
}

bool LineDataBuilder::storeSchema(const string& indexPath, const IndexPartitionSchemaPtr& schema,
                                  FenceContext* fenceContext)
{
    string schemaPath = indexlib::util::PathUtil::JoinPath(
        indexPath, indexlib::index_base::Version::GetSchemaFileName(schema->GetSchemaVersionId()));
    try {
        auto loadedSchema = indexlib::config::IndexPartitionSchema::Load(schemaPath);
        if (loadedSchema) {
            BS_LOG(INFO, "index schema already exist");
            return true;
        }
    } catch (const exception& e) {
        BS_LOG(ERROR, "store schema to [%s] failed, exception [%s]", indexPath.c_str(), e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "store schema to [%s] failed, unknown expcetion", indexPath.c_str());
        return false;
    }

    // store if not exist
    try {
        indexlib::config::IndexPartitionSchema::Store(schema, schemaPath, fenceContext);
    } catch (const exception& e) {
        BS_LOG(ERROR, "store schema to [%s] failed, exception [%s]", indexPath.c_str(), e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "store schema to [%s] failed, unknown expcetion", indexPath.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::builder
