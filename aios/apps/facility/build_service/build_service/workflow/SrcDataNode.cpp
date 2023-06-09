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
#include "build_service/workflow/SrcDataNode.h"

#include "autil/memory.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::common;
using namespace build_service::proto;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::partition;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, SrcDataNode);

const int64_t SrcDataNode::reopenDetectInterval = 30 * 1000 * 1000; // 30s
const int64_t SrcDataNode::openRetryCount = 1;

SrcDataNode::SrcDataNode(const config::SrcNodeConfig& config, indexlib::util::MetricProviderPtr metricProvider)
    : _config(config)
    , _preserverFieldId(INVALID_FIELDID)
    , _metricProvider(metricProvider)
    , _hasGenerateLoadConfig(false)
    , _useIndexIdx(0)
{
}

SrcDataNode::~SrcDataNode()
{
    _reopenThread.reset();
    _currentResource.reset();
    _targetResource.reset();
}

vector<string> SrcDataNode::getIndexRootFromResource(const ResourceKeeperPtr& keeper) const noexcept
{
    vector<string> ret;
    string path;
    string backupPath;
    if (keeper) {
        keeper->getParam(RESOURCE_INDEX_ROOT, &path);
        keeper->getParam(RESOURCE_BACKUP_INDEX_ROOT, &backupPath);
    }
    ret.push_back(path);
    if (!backupPath.empty()) {
        ret.push_back(backupPath);
    }
    return ret;
}

versionid_t SrcDataNode::getIndexVersion(const ResourceKeeperPtr& keeper) const noexcept
{
    versionid_t ret = INVALID_VERSION;
    string versionStr;
    if (keeper && keeper->getParam(INDEX_VERSION, &versionStr) && StringUtil::numberFromString(versionStr, ret)) {
        return ret;
    }
    return INVALID_VERSION;
}

string SrcDataNode::getIndexCluster(const ResourceKeeperPtr& keeper) const noexcept
{
    string ret;
    if (keeper) {
        keeper->getParam(INDEX_CLUSTER, &ret);
    }
    return ret;
}

bool SrcDataNode::init(const config::ResourceReaderPtr& configReader, const proto::PartitionId& pid,
                       const ResourceKeeperPtr& checkpointKeeper) noexcept
{
    {
        ScopedLock lock(_targetMutex);
        _targetResource = checkpointKeeper;
        if (!_targetResource || getIndexVersion(_targetResource) == INVALID_VERSION) {
            BS_LOG(ERROR, "no target resource or version is INVALID VERSION");
            return false;
        }
    }

    vector<string> indexRoots = getIndexRootFromResource(_targetResource);
    bool success = false;
    _useIndexIdx = 0;
    for (const string& indexRoot : indexRoots) {
        proto::PartitionId indexPid;
        string localIndexRoot;
        bool thisDone = true;
        if (!calculateIndexPid(configReader, pid, indexRoot, indexPid) ||
            !prepareLocalWorkDir(indexPid, localIndexRoot) ||
            !rewriteSchemaAndLoadconfig(configReader, localIndexRoot) || !loadFull(localIndexRoot) ||
            !prepareIndexBuilder(indexPid)) {
            thisDone = false;
        }
        if (thisDone) {
            success = true;
            break;
        }
        _useIndexIdx++;
    }
    if (!success) {
        BS_LOG(ERROR, "src init failed with resource [%s]", ToJsonString(*checkpointKeeper).c_str());
        return false;
    }
    _reopenThread = autil::LoopThread::createLoopThread(bind(&SrcDataNode::reopenLoop, this), reopenDetectInterval);
    if (!_reopenThread) {
        return false;
    }
    registerMetric();
    return true;
}

void SrcDataNode::registerMetric() noexcept
{
    if (!_metricProvider) {
        return;
    }
    _targetVersonMetric = DECLARE_METRIC(_metricProvider, "basic/srcTargetVersion", kmonitor::STATUS, "count");
    _currentVersionMetric = DECLARE_METRIC(_metricProvider, "basic/srcCurrentVersion", kmonitor::STATUS, "count");
    _useIndexIdxMetric = DECLARE_METRIC(_metricProvider, "basic/srcUseIndexIdx", kmonitor::STATUS, "count");
}

string SrcDataNode::getSrcClusterName() noexcept
{
    ScopedLock lock(_targetMutex);
    assert(_targetResource);
    return getIndexCluster(_targetResource);
}

bool SrcDataNode::calculateIndexPid(const config::ResourceReaderPtr& configReader, const proto::PartitionId& pid,
                                    const string& indexRoot, proto::PartitionId& indexPid) noexcept
{
    BuildRuleConfig buildRuleConfig;
    string srcClusterName = getSrcClusterName();
    if (!configReader->getClusterConfigWithJsonPath(srcClusterName, "cluster_config.builder_rule_config",
                                                    buildRuleConfig)) {
        return false;
    }
    uint32_t indexPartitionCount = buildRuleConfig.partitionCount;
    vector<Range> ranges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, indexPartitionCount);
    const auto& range = pid.range();
    for (auto& it : ranges) {
        if (it.from() <= range.from() && it.to() >= range.to()) {
            indexPid = pid;
            indexPid.clear_clusternames();
            *indexPid.add_clusternames() = srcClusterName;
            *indexPid.mutable_range() = it;
            ScopedLock lock(_targetMutex);
            _remoteIndexRoot = util::IndexPathConstructor::constructIndexPath(
                indexRoot, getIndexCluster(_targetResource), StringUtil::toString(pid.buildid().generationid()),
                StringUtil::toString(indexPid.range().from()), StringUtil::toString(indexPid.range().to()));
            if (_config.options.GetOnlineConfig().NeedSyncRealtimeIndex()) {
                string remoteRealtimeDir = string("src_realtime_index_") + _config.remoteRealtimeRootSuffix;
                string remoteRtIndex = util::IndexPathConstructor::constructSyncRtIndexPath(
                    indexRoot, getIndexCluster(_targetResource), StringUtil::toString(pid.buildid().generationid()),
                    StringUtil::toString(pid.range().from()), StringUtil::toString(pid.range().to()),
                    remoteRealtimeDir);
                _config.options.GetOnlineConfig().SetRealtimeIndexSyncDir(remoteRtIndex);
            }
            return true;
        }
    }
    BS_LOG(ERROR, "fail to calculate index partition range with pid [%s] partition count [%u]",
           pid.ShortDebugString().c_str(), indexPartitionCount);
    return false;
}

bool SrcDataNode::prepareIndexBuilder(const proto::PartitionId& indexPid) noexcept
{
    int64_t buildTotalMem = _config.options.GetBuildConfig(true).buildTotalMemory;
    QuotaControlPtr memQuotaControl(new QuotaControl(buildTotalMem * 1024 * 1024));
    indexlib::PartitionRange range(indexPid.range().from(), indexPid.range().to());
    _indexBuilder.reset(new IndexBuilder(_partition, memQuotaControl, _metricProvider, range));
    if (!_indexBuilder->Init()) {
        _indexBuilder.reset();
        BS_LOG(ERROR, "failed to init index xbuilder");
        return false;
    }
    return true;
}

std::string SrcDataNode::getLocalWorkDir() const { return PathDefine::getLocalDataPath(); }

bool SrcDataNode::prepareLocalWorkDir(const proto::PartitionId& indexPid, string& localIndexRoot) noexcept
{
    localIndexRoot = getLocalWorkDir();
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(localIndexRoot, exist)) {
        BS_LOG(ERROR, "fail to check  exist [%s]", localIndexRoot.c_str());
        return false;
    }
    return exist;
}

bool SrcDataNode::loadFull(const string& localPartitionRoot) noexcept
{
    IndexPartitionResourcePtr indexResource;

    string remoteIndexPath = getRemoteIndexPath();
    IndexPartitionPtr indexPartition;
    bool openSuccess = false;
    auto latestVersion = getLatestVersion();
    for (int64_t i = 0; i <= openRetryCount; ++i) {
        PartitionGroupResourcePtr partitionGroupResource = PartitionGroupResource::Create(
            _config.partitionMemoryInByte, _metricProvider, _config.blockCacheParam.c_str(), nullptr);
        if (!partitionGroupResource) {
            BS_LOG(WARN, "init global index resource failed");
            continue;
        }
        indexResource.reset(new IndexPartitionResource(*partitionGroupResource, "defaultPartitionName"));
        auto option = _config.options;
        indexPartition =
            IndexPartitionCreator(*indexResource).CreateByLoadSchema(option, localPartitionRoot, INVALID_VERSION);

        if (!indexPartition) {
            BS_LOG(WARN, "create index partition failed");
        }
        IndexPartition::OpenStatus os = indexPartition->Open(localPartitionRoot, remoteIndexPath, /*schema*/ nullptr,
                                                             option, getIndexVersion(latestVersion));
        if (os == IndexPartition::OpenStatus::OS_OK) {
            openSuccess = true;
            break;
        } else {
            BS_LOG(WARN, "open index partition failed");
            // TODO xiaohao.yxh add metric
        }
    }
    if (!openSuccess) {
        BS_LOG(ERROR, "load full with index root [%s] failed", remoteIndexPath.c_str());
        return false;
    }
    auto partReader = indexPartition->GetReader();
    if (!partReader) {
        BS_LOG(ERROR, "get partition reader failed");
        return false;
    }
    if (!partReader->GetPrimaryKeyReader()) {
        BS_LOG(ERROR, "failed to get pk reader");
        return false;
    }
    if (_config.enableOrderPreserving) {
        if (!partReader->GetAttributeReader(_config.orderPreservingField)) {
            BS_LOG(ERROR, "enable order preserving but failed to get attribute reader for field [%s]",
                   _config.orderPreservingField.c_str());
            return false;
        }
    }
    if (_config.enableUpdateToAdd) {
        if (!partReader->GetSourceReader()) {
            BS_LOG(ERROR, "update to add enabled  but failed to get source reader");
            return false;
        }
    }
    _partition = indexPartition;
    _indexResource = indexResource;
    _currentResource = latestVersion;
    BS_LOG(INFO, "load full success with version [%d]", getIndexVersion(_currentResource));
    return true;
}

ResourceKeeperPtr SrcDataNode::getUsingResource() noexcept { return _currentResource; }

IndexPartitionReaderPtr SrcDataNode::getReader() const noexcept
{
    if (_partition) {
        return _partition->GetReader();
    }
    return nullptr;
}

void SrcDataNode::updateResource(common::ResourceKeeperPtr& latest, bool& needRestart) noexcept
{
    if (!latest || !_targetResource) {
        return;
    }
    autil::ScopedLock lock(_targetMutex);
    vector<string> newIndexRoot = getIndexRootFromResource(latest);
    vector<string> oldIndexRoot = getIndexRootFromResource(_targetResource);
    if (newIndexRoot != oldIndexRoot) {
        BS_LOG(INFO, "index roots changed from [%s] to [%s], need restart", ToJsonString(oldIndexRoot).c_str(),
               ToJsonString(newIndexRoot).c_str());
        needRestart = true;
        return;
    }
    if (getIndexVersion(latest) > getIndexVersion(_targetResource) &&
        getIndexCluster(latest) == getIndexCluster(_targetResource)) {
        _targetResource = latest;
        BS_LOG(INFO, "src update new index resources to version [%d]", getIndexVersion(_targetResource));
    }
}

common::ResourceKeeperPtr SrcDataNode::getLatestVersion() noexcept
{
    ScopedLock lock(_targetMutex);
    return _targetResource;
}

bool SrcDataNode::getLocator(common::Locator& locator) const
{
    if (!_indexBuilder) {
        return false;
    }
    common::Locator rtLocator;
    locator = common::Locator();
    try {
        string locatorStr;
        if (!_indexBuilder->GetLastLocator(locatorStr)) {
            return false;
        }
        if (!locatorStr.empty() && !rtLocator.Deserialize(locatorStr)) {
            string errorMsg = string("deserialize [") + locatorStr + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }

        int64_t incVersionTime = 0;
        if (!_indexBuilder->GetIncVersionTimestamp(incVersionTime)) {
            return false;
        }
        bool fromInc;
        BS_LOG(INFO, "seekts [%ld] %ld", incVersionTime, rtLocator.GetOffset());
        int64_t seekTs = OnlineJoinPolicy::GetRtSeekTimestamp(incVersionTime, rtLocator.GetOffset(), fromInc);

        if (fromInc) {
            locator.SetSrc(rtLocator.GetSrc());
            locator.SetOffset(seekTs);
            BS_LOG(INFO, "incVersionTime[%ld], rt locator[%s], inc covers rt", incVersionTime,
                   rtLocator.DebugString().c_str());
        } else {
            locator = rtLocator;
        }
    } catch (const exception& e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string msg = "get locator from src " + locator.DebugString();
    BS_LOG(INFO, "[%s]", msg.c_str());
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    return true;
}

string SrcDataNode::getRemoteIndexPath() const noexcept { return _remoteIndexRoot; }

void SrcDataNode::reopenLoop() noexcept
{
    BS_LOG(DEBUG, "check reopen");
    auto latestVersion = getLatestVersion();
    auto targetVersionId = getIndexVersion(latestVersion);
    auto currentVersionId = getIndexVersion(_currentResource);
    REPORT_METRIC(_targetVersonMetric, targetVersionId);
    REPORT_METRIC(_currentVersionMetric, currentVersionId);
    REPORT_METRIC(_useIndexIdxMetric, _useIndexIdx);

    if (targetVersionId > currentVersionId) {
        auto ret = _partition->ReOpen(false, getIndexVersion(latestVersion));
        if (ret == IndexPartition::OpenStatus::OS_OK) {
            _currentResource = latestVersion;
            BS_LOG(INFO, "reopen to version [%d] success", getIndexVersion(latestVersion));
        } else {
            BS_LOG(ERROR, "reopen failed, return value [%d]", (int32_t)ret);
        }
    }
}

bool SrcDataNode::readSrcConfig(const config::ProcessorConfigReaderPtr& configReader,
                                config::SrcNodeConfig& srcNodeConfig, bool* isExist) noexcept
{
    if (!configReader->getProcessorConfig("src_node_config", &srcNodeConfig, isExist)) {
        BS_LOG(ERROR, "read src_node_config failed");
        return false;
    }
    if (srcNodeConfig.options.GetOnlineConfig().NeedSyncRealtimeIndex()) {
        ProcessorRuleConfig ruleConfig;
        if (!configReader->readRuleConfig(&ruleConfig)) {
            return false;
        }
        stringstream ss;
        ss << ruleConfig.partitionCount << "_" << ruleConfig.incParallelNum;
        srcNodeConfig.remoteRealtimeRootSuffix = ss.str();
    }
    return true;
}

bool SrcDataNode::readSrcConfig(const config::ProcessorConfigReaderPtr& configReader,
                                config::SrcNodeConfig& srcNodeConfig) noexcept
{
    bool isExist;
    return SrcDataNode::readSrcConfig(configReader, srcNodeConfig, &isExist);
}

void SrcDataNode::disableUselessFields(indexlib::config::IndexPartitionSchemaPtr& partitionSchema,
                                       indexlib::config::FieldSchemaPtr& fieldSchema, const set<string>& keepFieldNames,
                                       bool disableSource) noexcept
{
    auto indexSchema = partitionSchema->GetIndexSchema();
    auto attributeSchema = partitionSchema->GetAttributeSchema();
    auto& config = _config.options.GetOnlineConfig().GetDisableFieldsConfig();

    for (auto it = indexSchema->Begin(); it != indexSchema->End(); ++it) {
        if ((*it)->GetInvertedIndexType() != it_primarykey64 && (*it)->GetInvertedIndexType() != it_primarykey128) {
            config.indexes.push_back((*it)->GetIndexName());
        }
    }

    for (auto it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        string fieldName = (*it)->GetFieldName();
        if (keepFieldNames.find(fieldName) != keepFieldNames.end()) {
            continue;
        }
        fieldid_t fid = (*it)->GetFieldId();
        if (attributeSchema->IsInAttribute(fid)) {
            attrid_t attrId = attributeSchema->GetAttributeConfigByFieldId(fid)->GetAttrId();
            if (attributeSchema->GetPackIdByAttributeId(attrId) == INVALID_PACK_ATTRID) {
                config.attributes.push_back(fieldName);
            }
        }
    }

    auto packAttrIter = attributeSchema->CreatePackAttrIterator();
    for (auto it = packAttrIter->Begin(); it != packAttrIter->End(); ++it) {
        auto attributeConfigs = (*it)->GetAttributeConfigVec();
        bool hasKeepField = false;
        for (indexlib::config::AttributeConfigPtr attributeConfig : attributeConfigs) {
            if (keepFieldNames.find(attributeConfig->GetFieldConfig()->GetFieldName()) != keepFieldNames.end()) {
                hasKeepField = true;
                break;
            }
        }
        if (!hasKeepField) {
            config.packAttributes.push_back((*it)->GetAttrName());
        }
    }
    config.summarys = indexlib::config::DisableFieldsConfig::SDF_FIELD_ALL;
    if (disableSource) {
        config.sources = indexlib::config::DisableFieldsConfig::CDF_FIELD_ALL;
    }
}

bool SrcDataNode::rewriteSchemaAndLoadconfig(const ResourceReaderPtr& resourceReader,
                                             const string& localPartitionRoot) noexcept
{
    _config.options.GetOnlineConfig().SetValidateIndexEnabled(false);
    string cluster = getSrcClusterName();
    IndexPartitionSchemaPtr schema = resourceReader->getSchema(cluster);
    if (!schema) {
        BS_LOG(ERROR, "failed to get schema for cluster [%s]", cluster.c_str());
        return false;
    }
    if (_config.enableOrderPreserving) {
        RegionSchemaPtr regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        if (!regionSchema) {
            BS_LOG(ERROR, "failed to get region schema for cluster [%s]", cluster.c_str());
            return false;
        }
        _config.orderPreservingField = regionSchema->GetOrderPreservingFieldName();
        if (_config.orderPreservingField.empty()) {
            BS_LOG(ERROR, "order preserving field is empty for cluster[%s]", cluster.c_str());
            return false;
        }
    }
    if (_config.enableUpdateToAdd) {
        const SourceSchemaPtr& sourceSchema = schema->GetSourceSchema();
        if (!sourceSchema) {
            BS_LOG(ERROR, "failed to get source schema for cluster [%s]", cluster.c_str());
            return false;
        }
        _sourceSchema = sourceSchema;
    }

    auto fieldSchema = schema->GetFieldSchema();
    set<string> keepFieldNames;
    if (_config.enableOrderPreserving) {
        // add order preserving field
        string fieldName = _config.orderPreservingField;
        auto fieldConfig = fieldSchema->GetFieldConfig(fieldName);
        if (!fieldConfig) {
            BS_LOG(ERROR, "failed to get field config [%s]", fieldName.c_str());
            return false;
        }
        keepFieldNames.insert(_config.orderPreservingField);
        _orderPreservingFieldType = fieldConfig->GetFieldType();
        _preserverFieldId = fieldConfig->GetFieldId();
    }

    // add pk
    auto indexSchema = schema->GetIndexSchema();
    fieldid_t pkFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
    FieldConfigPtr pkFieldConfig = fieldSchema->GetFieldConfig(pkFieldId);
    keepFieldNames.insert(pkFieldConfig->GetFieldName());

    // TODO add src field

    disableUselessFields(schema, fieldSchema, keepFieldNames, !_config.enableUpdateToAdd);

    // add default load config
    if (!_hasGenerateLoadConfig) {
        auto& loadConfigList = _config.options.GetOnlineConfig().loadConfigList;
        LoadConfig defaultLoadConfig;
        defaultLoadConfig.SetRemote(true);
        defaultLoadConfig.SetDeploy(false);

        LoadConfig::FilePatternStringVector pattern;
        pattern.push_back(".*");
        defaultLoadConfig.SetFilePatternString(pattern);
        loadConfigList.PushBack(defaultLoadConfig);
        _hasGenerateLoadConfig = true;
    }

    string normalizeDir = fslib::util::FileUtil::normalizeDir(localPartitionRoot);
    schemavid_t schemaId = schema->GetSchemaVersionId();
    string schemaPath = fslib::util::FileUtil::joinFilePath(normalizeDir, Version::GetSchemaFileName(schemaId));
    try {
        fslib::util::FileUtil::removeIfExist(schemaPath);
        indexlib::index_base::SchemaAdapter::StoreSchema(normalizeDir, schema);
    } catch (const exception& e) {
        BS_LOG(ERROR, "store schema vailed, [%s]", e.what());
        return false;
    }

    return true;
}

docid_t SrcDataNode::search(const IndexPartitionReaderPtr& reader, const string& pkey) const
{
    return reader->GetPrimaryKeyReader()->Lookup(pkey);
}

bool SrcDataNode::isDocNewerThanIndex(const IndexPartitionReaderPtr& partReader, docid_t indexDocId,
                                      indexlib::document::DocumentPtr currentDoc) const
{
    if (!_config.enableOrderPreserving || indexDocId == INVALID_DOCID) {
        return true;
    }
    auto reader = partReader->GetAttributeReader(_config.orderPreservingField);

    NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, currentDoc);
    const AttributeDocumentPtr& attrDoc = normalDoc->GetAttributeDocument();
    const StringView& docTs = attrDoc->GetField(_preserverFieldId);
    bool emptyTs = false;
    if (docTs.empty()) {
        BS_INTERVAL_LOG2(60, WARN,
                         "fail to get doc [%s] attribute field [%s] for order preserve, will use default value 0",
                         normalDoc->GetPrimaryKey().c_str(), _config.orderPreservingField.c_str());
        emptyTs = true;
    }
    string tsStr;
    switch (_orderPreservingFieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        typedef indexlib::index::FieldTypeTraits<type>::AttrItemType actualType;                                       \
        actualType indexValue = 0;                                                                                     \
        bool isNull;                                                                                                   \
        auto iter = dynamic_cast<indexlib::index::AttributeIteratorTyped<actualType>*>(                                \
            reader->CreateIterator(normalDoc->GetPool()));                                                             \
        assert(iter);                                                                                                  \
        auto scopedIter = autil::unique_ptr_pool(normalDoc->GetPool(), iter);                                          \
        if (!scopedIter->Seek(indexDocId, indexValue, isNull)) {                                                       \
            throw autil::legacy::ExceptionBase("failed to read timestamp");                                            \
        }                                                                                                              \
        actualType docTsValue = 0;                                                                                     \
        if (!emptyTs) {                                                                                                \
            docTsValue = *((actualType*)docTs.data());                                                                 \
        }                                                                                                              \
        return docTsValue >= indexValue;                                                                               \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
    default:
        assert(false);
        BS_LOG(ERROR, "unexcepted field type");
    }
    return false;
}

void SrcDataNode::toRawDocument(const indexlib::document::DocumentPtr& doc, document::RawDocumentPtr rawDoc) noexcept
{
    indexlib::document::NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(indexlib::document::NormalDocument, doc);
    indexlib::document::SourceDocumentPtr srcDoc(new indexlib::document::SourceDocument(normalDoc->GetPool()));
    toSourceDocument((normalDoc->GetSourceDocument()), srcDoc.get());
    doc->ToRawDocument(rawDoc.get());
    srcDoc->ToRawDocument(*rawDoc);
}

void SrcDataNode::toSourceDocument(const indexlib::document::SerializedSourceDocumentPtr& serializedDoc,
                                   indexlib::document::SourceDocument* sourceDoc)
{
    SourceDocumentFormatter formatter;
    formatter.Init(_sourceSchema);
    formatter.DeserializeSourceDocument(serializedDoc, sourceDoc);
}

void SrcDataNode::toSerializedSourceDocument(const indexlib::document::SourceDocumentPtr& sourceDoc,
                                             autil::mem_pool::Pool* pool,
                                             indexlib::document::SerializedSourceDocumentPtr serializedDoc)
{
    SourceDocumentFormatter formatter;
    formatter.Init(_sourceSchema);
    formatter.SerializeSourceDocument(sourceDoc, pool, serializedDoc);
}

bool SrcDataNode::updateSrcDoc(const IndexPartitionReaderPtr& reader, docid_t docid,
                               indexlib::document::Document* updateDoc, bool& needRetry)
{
    indexlib::document::NormalDocument* document = dynamic_cast<indexlib::document::NormalDocument*>(updateDoc);
    indexlib::document::SourceDocumentPtr sourceDocument(new indexlib::document::SourceDocument(document->GetPool())),
        originalSourceDoc(new indexlib::document::SourceDocument(document->GetPool()));
    auto sourceReader = reader->GetSourceReader();
    if (!sourceReader->GetDocument(docid, sourceDocument.get())) {
        BS_LOG(ERROR, "get src document failed");
        needRetry = true;
        return false;
    }

    toSourceDocument(document->GetSourceDocument(), originalSourceDoc.get());
    if (!sourceDocument->Merge(originalSourceDoc)) {
        BS_LOG(ERROR, "doc [%s] merge src doc failed", document->GetPrimaryKey().c_str());
        needRetry = false;
        return false;
    }
    indexlib::document::SerializedSourceDocumentPtr serDoc(
        new indexlib::document::SerializedSourceDocument(document->GetPool()));
    toSerializedSourceDocument(sourceDocument, document->GetPool(), serDoc);
    document->SetSourceDocument(serDoc);
    return true;
}

bool SrcDataNode::build(const indexlib::document::DocumentPtr& doc)
{
    DocOperateType originalOp = doc->GetDocOperateType();
    if (originalOp == UPDATE_FIELD) {
        doc->ModifyDocOperateType(ADD_DOC);
    }
    bool ret = _indexBuilder->Build(doc);
    doc->ModifyDocOperateType(originalOp);
    return ret;
}
}} // namespace build_service::workflow
