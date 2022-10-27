#include "build_service/builder/LineDataBuilder.h"
#include "build_service/config/ConfigDefine.h"
#include <fslib/fs/FileSystem.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <indexlib/util/counter/accumulative_counter.h>

using namespace std;
using namespace autil;
using namespace build_service::config;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace builder {
BS_LOG_SETUP(builder, LineDataBuilder);

LineDataBuilder::LineDataBuilder(
        const string &indexPath,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
        fslib::fs::FileLock *fileLock)
    : Builder(IE_NAMESPACE(partition)::IndexBuilderPtr(), fileLock)
    , _indexPath(indexPath)
    , _schema(schema)
    , _counterMap(new IE_NAMESPACE(util)::CounterMap())
{
}

LineDataBuilder::~LineDataBuilder() {
}

bool LineDataBuilder::init(const BuilderConfig &builderConfig,
                          IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
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
    auto &param = _schema->GetUserDefinedParam();
    param[BS_FILE_NAME] = autil::legacy::Any(string(BS_LINE_DATA));
    if (!storeSchema(_indexPath, _schema)) {
        return false;
    }
    try {
        if (!_writer) {
            _writer.reset(new BufferedFileWriter);
        }
        _writer->Open(fslib::fs::FileSystem::joinFilePath(_indexPath, BS_LINE_DATA));
    } catch (const exception &e) {
        BS_LOG(ERROR, "exception happend [%s]", e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "unknown exception happened");
        return false;
    }
    return true;
}

bool LineDataBuilder::build(const IE_NAMESPACE(document)::DocumentPtr &doc) {
    bool buildSuccess = true;
    try {
        string oneLineDoc;
        if (prepareOne(doc, oneLineDoc)) {
            _writer->Write(oneLineDoc.c_str(), oneLineDoc.size());
        } else {
            buildSuccess = false;
        }
    } catch (const exception &e) {
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

void LineDataBuilder::stop(int64_t stopTimestamp) {
    try {
        _writer->Close();
    } catch (const exception &e) {
        BS_LOG(ERROR, "exception happend [%s]", e.what());
        setFatalError();
    } catch (...) {
        BS_LOG(ERROR, "unknown exception happend");
        setFatalError();
    }
}

bool LineDataBuilder::prepareOne(const IE_NAMESPACE(document)::DocumentPtr &document,
                                string &oneLineDoc)
{
    oneLineDoc.clear();
    const AttributeSchemaPtr &attrSchema = _schema->GetAttributeSchema();
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
        const AttributeConfigPtr& attrConfig =
            attrSchema->GetAttributeConfig(fieldCounter);
        fieldid_t fieldId = attrConfig->GetFieldId();
        const ConstString& field = attrDoc->GetField(fieldId);
        // TODO, get seperator from schema
        if (fieldCounter != 0) {
            oneLineDoc.append(_fieldSeperator);
        }
        oneLineDoc.append(field.c_str(), field.size());
    }
    oneLineDoc.append(1, '\n');
    return true;
}


bool LineDataBuilder::storeSchema(const string &indexPath,
                                 const IndexPartitionSchemaPtr &schema)
{
    try {
        auto loadedSchema = IE_NAMESPACE(index_base)::SchemaAdapter::LoadSchema(
                indexPath, schema->GetSchemaVersionId());
        if (loadedSchema) {
            BS_LOG(INFO, "index schema already exist");
            return true;
        }
    } catch (const exception &e) {
        BS_LOG(ERROR, "store schema to [%s] failed, exception [%s]",
               indexPath.c_str(), e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "store schema to [%s] failed, unknown expcetion",
               indexPath.c_str());
        return false;
    }

    // store if not exist
    try {
        IE_NAMESPACE(index_base)::SchemaAdapter::StoreSchema(indexPath, schema);
    } catch (const exception &e) {
        BS_LOG(ERROR, "store schema to [%s] failed, exception [%s]",
               indexPath.c_str(), e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "store schema to [%s] failed, unknown expcetion",
               indexPath.c_str());
        return false;
    }
    return true;
}

}
}
