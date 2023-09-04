#include "indexlib/index/pack_attribute/test/PackAttributeTestHelper.h"

#include "autil/StringTokenizer.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/rewriter/PackAttributeAppender.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/pack_attribute/PackAttributeIndexFactory.h"
#include "indexlib/index/pack_attribute/PackAttributeReader.h"
#include "indexlib/index/test/FakeDocMapper.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeTestHelper);

PackAttributeTestHelper::PackAttributeTestHelper() {}

PackAttributeTestHelper::~PackAttributeTestHelper() {}

void PackAttributeTestHelper::DoInit()
{
    if (!_factory) {
        _factory = std::make_unique<PackAttributeIndexFactory>();
    }
    if (!_docInfoExtractorFactory) {
        _docInfoExtractorFactory = std::make_unique<plain::DocumentInfoExtractorFactory>();
    }
}

StatusOr<std::shared_ptr<config::IIndexConfig>> PackAttributeTestHelper::DoMakeIndexConfig(const std::string& indexDesc,
                                                                                           size_t indexIdx)
{
    return MakePackAttributeConfig(indexDesc, indexIdx);
}

StatusOr<std::shared_ptr<IMemIndexer>> PackAttributeTestHelper::CreateMemIndexer()
{
    assert(_factory && GetIndexerParameter());
    auto memIndexer = _factory->CreateMemIndexer(_indexConfig, *GetIndexerParameter());
    if (auto status = memIndexer->Init(_indexConfig, _docInfoExtractorFactory.get()); !status.IsOK()) {
        AUTIL_LOG(ERROR, "init PackAttributeMemIndexer failed");
        return status.steal_error();
    }
    return memIndexer;
}

StatusOr<std::shared_ptr<IIndexMerger>> PackAttributeTestHelper::CreateIndexMerger()
{
    assert(_factory && GetIndexerParameter());
    auto indexMerger = _factory->CreateIndexMerger(_indexConfig);
    std::map<std::string, std::any> initParams;
    initParams[DocMapper::GetDocMapperType()] = std::string("ReclaimMap");
    if (auto status = indexMerger->Init(_indexConfig, initParams); !status.IsOK()) {
        AUTIL_LOG(ERROR, "init index merger failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    return indexMerger;
}

StatusOr<std::shared_ptr<framework::IndexTaskResourceManager>> PackAttributeTestHelper::CreateIndexTaskResourceManager(
    const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos, const std::string& docMapStr)
{
    auto indexTaskResourceManager = std::make_shared<framework::IndexTaskResourceManager>();
    // prepare DocMapper
    auto docMapper = std::make_shared<FakeDocMapper>("ReclaimMap", DocMapper::GetDocMapperType());
    std::vector<SrcSegmentInfo> srcSegInfos;
    docid_t baseDocid = 0;
    for (const auto& segment : GetTabletData()->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT)) {
        segmentid_t segId = segment->GetSegmentId();
        uint32_t docCount = segment->GetDocCount();
        docid_t baseDocId = baseDocid;
        std::set<docid_t> deletedDocs;
        srcSegInfos.emplace_back(segId, docCount, baseDocId, deletedDocs);
        baseDocid += docCount;
    }
    if (docMapStr.empty()) {
        assert(segmentMergeInfos.targetSegments.size() == 1);
        segmentid_t firstTargetSegId = segmentMergeInfos.targetSegments[0]->segmentId;
        segmentMergeInfos.targetSegments[0]->segmentInfo->docCount = baseDocid;
        uint32_t targetSegCount = segmentMergeInfos.targetSegments.size();
        docMapper->Init(srcSegInfos, firstTargetSegId, targetSegCount, false);
    } else {
        docMapper->Init(srcSegInfos, segmentMergeInfos, docMapStr);
    }
    auto [status, resourceDirectory] = GetRootDirectory()->MakeDirectory("merge_resource", {}).StatusWith();
    if (!status.IsOK()) {
        return status.steal_error();
    }
    auto legacyResourceDirectory = indexlib::file_system::IDirectory::ToLegacyDirectory(resourceDirectory);
    if (auto status = indexTaskResourceManager->Init(legacyResourceDirectory, "workPrefix", nullptr); !status.IsOK()) {
        AUTIL_LOG(ERROR, "init IndexTaskResourceCreator failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    indexTaskResourceManager->TEST_AddResource(docMapper);
    return indexTaskResourceManager;
}

StatusOr<std::shared_ptr<PackAttributeConfig>>
PackAttributeTestHelper::MakePackAttributeConfig(const std::string& packAttributeDesc, size_t indexIdx)
{
    autil::StringTokenizer st(packAttributeDesc, ":",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    assert(st.getNumTokens() > 0 && st.getNumTokens() <= 3);
    std::string packName = st[0];
    std::vector<std::string> subAttributeNames;
    autil::StringUtil::fromString(st[1], subAttributeNames, ",");
    assert(subAttributeNames.size() >= 1);
    std::string compressStr = (st.getNumTokens() >= 3) ? st[2] : std::string();
    std::string jsonTemplate = R"( {{
        "pack_name": {0},
        "sub_attributes": {1},
        "compress_type": {2}
    }} )";
    std::string jsonStr =
        fmt::format(jsonTemplate, autil::legacy::ToJsonString(packName),
                    autil::legacy::ToJsonString(subAttributeNames, true), autil::legacy::ToJsonString(compressStr));
    autil::legacy::Any any = autil::legacy::json::ParseJson(jsonStr);
    indexlibv2::config::IndexConfigDeserializeResource resource(GetFieldConfigs(), MutableRuntimeSettings());
    _packAttributeConfig = std::make_shared<PackAttributeConfig>();
    _packAttributeConfig->Deserialize(any, indexIdx, resource);
    if (!_packAttrAppender) {
        _packAttrAppender = std::make_unique<indexlibv2::document::PackAttributeAppender>();
    }
    _packAttrAppender->InitOnePackAttr(_packAttributeConfig);
    return _packAttributeConfig;
}

static void ReplaceMultiFieldSeprator(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                                      const std::shared_ptr<document::RawDocument>& rawDoc)
{
    if (fieldConfig->IsMultiValue()) {
        auto value = rawDoc->getField(fieldConfig->GetFieldName());
        autil::StringUtil::replace(value, ' ', autil::MULTI_VALUE_DELIMITER);
        rawDoc->setField(fieldConfig->GetFieldName(), value);
    }
}

static void FillNullValue(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                          const std::shared_ptr<document::RawDocument>& rawDoc)
{
    if (rawDoc->getDocOperateType() != ADD_DOC) {
        return;
    }
    const std::string& fieldName = fieldConfig->GetFieldName();
    if (fieldConfig->IsEnableNullField() && !rawDoc->exist(fieldName)) {
        rawDoc->setField(fieldName, fieldConfig->GetNullFieldLiteralString());
    }
}

void PackAttributeTestHelper::CompleteRawDocument(const std::shared_ptr<document::RawDocument>& rawDoc)
{
    for (const auto& fieldConfig : GetFieldConfigs()) {
        ReplaceMultiFieldSeprator(fieldConfig, rawDoc);
        FillNullValue(fieldConfig, rawDoc);
    }
}

std::shared_ptr<document::IIndexFields> PackAttributeTestHelper::MakePackAttributeIndexFields(const std::string& docs)
{
    // auto rawDocs = document::RawDocumentMaker::MakeBatch(docs);
    // docid_t docid = 0;
    // auto documentBatch = std::make_shared<document::DocumentBatch>();
    // for (auto& rawDoc : rawDocs) {
    //     CompleteRawDocument(rawDoc);
    // }
    // return documentBatch;
    return nullptr;
}

void PackAttributeTestHelper::ConvertAttributeField(const std::shared_ptr<document::RawDocument>& rawDoc,
                                                    const std::shared_ptr<document::NormalDocument>& normalDoc)
{
    // FYR: ExtendDocFieldsConvertor::convertAttributeField
    const auto& attrDoc = normalDoc->GetAttributeDocument();
    const auto& subAttrConfigs = _packAttributeConfig->GetAttributeConfigVec();
    for (const auto& subAttrConfig : subAttrConfigs) {
        const auto& fieldConfig = subAttrConfig->GetFieldConfig();
        const auto& fieldName = fieldConfig->GetFieldName();
        auto fieldId = fieldConfig->GetFieldId();
        const autil::StringView& rawValue = rawDoc->getField(autil::StringView(fieldName));
        if (fieldConfig->IsEnableNullField() &&
            (!rawDoc->exist(fieldName) || rawValue == fieldConfig->GetNullFieldLiteralString())) {
            attrDoc->SetNullField(fieldId);
        }
        const auto& attrConvertor = std::unique_ptr<AttributeConvertor>(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(subAttrConfig));
        autil::StringView convertedValue =
            attrConvertor->Encode(rawValue, normalDoc->GetPool(), attrDoc->GetFormatErrorLable());
        attrDoc->SetField(fieldId, convertedValue);
    }
}

std::shared_ptr<document::IDocumentBatch> PackAttributeTestHelper::MakeDocumentBatch(const std::string& docs)
{
    return MakeNormalDocumentBatch(docs);
}

// TODO(ZQ): see all ATTRIBUTE_INDEX_TYPE_STR need
// ExtendDocFieldsConvertor::GetAttributeConfigs()
std::shared_ptr<document::IDocumentBatch> PackAttributeTestHelper::MakeNormalDocumentBatch(const std::string& docs)
{
    // FYR: NormalDocumentMaker::Make
    // [NormalDocumentFactory] --> NormalExtendDocument --> [ExtendDocument::setRawDocument]
    // SKIP, [InitExtendDoc]: NormalDocumentMaker::ConvertModifyFields, TokenizeDocumentConvertor -->
    // NormalExtendDocument [NormalDocumentParser::Parse] --> [SingleDocumentParser::Parse] -->
    //    --> IDocumentBatch
    // lack: NormalDocumentMaker::ConvertModifyFields
    auto rawDocs = document::RawDocumentMaker::MakeBatch(docs);
    docid_t docid = 0;
    auto documentBatch = std::make_shared<document::DocumentBatch>();
    for (auto rawDoc : rawDocs) {
        CompleteRawDocument(rawDoc);
        auto normalDoc = std::make_shared<document::NormalDocument>();
        normalDoc->SetAttributeDocument(std::make_shared<indexlib::document::AttributeDocument>());
        DocOperateType docType = rawDoc->getDocOperateType();
        if (docType != ADD_DOC && docType != UPDATE_FIELD) {
            AUTIL_LOG(ERROR, "unsupported docType[%d]", docType);
            assert(false);
            continue;
        }
        ConvertAttributeField(rawDoc, normalDoc);
        if (!_packAttrAppender->AppendPackAttribute(normalDoc->GetAttributeDocument(), normalDoc->GetPool())) {
            AUTIL_LOG(ERROR, "parse error: append pack attribute failed.");
            assert(false);
            continue;
        }
        // SKIP: AddModifiedField
        // normalDoc->SetDocInfo(docInfo);
        normalDoc->SetDocOperateType(docType);
        normalDoc->SetLocator(rawDoc->getLocator());
        normalDoc->SetDocId(docid++); // ?
        documentBatch->AddDocument(normalDoc);
    }
    return documentBatch;
    // IDocumentBatch --> [DocumentIterator<IDocument>::Create] --> IDocument -->
    // [AttrFieldInfoExtractor] --> NormalDocument --> GetAttributeDocument --> GetField(_fieldId)
}

bool PackAttributeTestHelper::Query(docid_t docid, const std::string& expectValueStr)
{
    std::vector<std::vector<std::string>> expectValues;
    autil::StringUtil::fromString(expectValueStr, expectValues, "=", ",");
    for (const auto& kv : expectValues) {
        std::string attrName = kv[0];
        std::string expectValue = kv.size() == 1 ? "" : kv[1];
        if (!Query(docid, attrName, expectValue)) {
            return false;
        }
    }
    return true;
}

bool PackAttributeTestHelper::Query(docid_t docid, const std::string& attrName, const std::string& expectValue)
{
    std::string value;
    if (_packAttrReader->Read(docid, attrName, value)) {
        autil::StringUtil::replace(value, autil::MULTI_VALUE_DELIMITER, ' ');
        if (value == expectValue) {
            return true;
        }
    }
    AUTIL_LOG(ERROR, "subAttr[%s] value not equal, expect[%s], actual[%s]", attrName.c_str(), expectValue.c_str(),
              value.c_str());
    return false;
}

bool PackAttributeTestHelper::DoQuery(const std::string& queryStr, const std::string& expectValueStr)
{
    return Query(autil::StringUtil::numberFromString<docid_t>(queryStr), expectValueStr);
}

Status PackAttributeTestHelper::DoOpen()
{
    auto reader = _factory->CreateIndexReader(_indexConfig, *GetIndexerParameter());
    if (auto status = reader->Open(_indexConfig, GetTabletData()); !status.IsOK()) {
        return status;
    }
    _packAttrReader.reset(dynamic_cast<PackAttributeReader*>(reader.release()));
    return Status::OK();
}

} // namespace indexlibv2::index
