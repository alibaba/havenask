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
#include "indexlib/table/normal_table/NormalTabletDocIterator.h"

#include "autil/memory.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/AndPostingExecutor.h"
#include "indexlib/index/inverted_index/DocidRangePostingExecutor.h"
#include "indexlib/index/inverted_index/IndexQueryCondition.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/OrPostingExecutor.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermPostingExecutor.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletDocIterator);

const std::string NormalTabletDocIterator::USER_REQUIRED_FIELDS = "read_index_required_fields";
const std::string NormalTabletDocIterator::USER_DEFINE_INDEX_PARAM = "user_define_index_param";
const std::string NormalTabletDocIterator::BUILDIN_KEEP_DELETED_DOC = "__buildin_reserve_deleted_doc__";
const std::string NormalTabletDocIterator::BUILDIN_DOCID_RANGE = "__buildin_docid_range__";
const uint32_t NormalTabletDocIterator::INVALID_RATIO = 100;

NormalTabletDocIterator::NormalTabletDocIterator() {}

NormalTabletDocIterator::~NormalTabletDocIterator() {}

Status NormalTabletDocIterator::Init(const std::shared_ptr<framework::TabletData>& tabletData,
                                     std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                                     const std::shared_ptr<indexlibv2::framework::MetricsManager>& metricsManager,
                                     const std::map<std::string, std::string>& params)
{
    _tryBestExport = autil::EnvUtil::getEnv("INDEXLIB_TRY_BEST_EXPORT_INDEX", false);
    _tabletReader =
        std::make_shared<NormalTabletReader>(tabletData->GetOnDiskVersionReadSchema(), /*NormalTabletMetrics*/ nullptr);
    _params = params;
    framework::ReadResource readResource;
    readResource.metricsManager = metricsManager.get();
    auto status = _tabletReader->Open(tabletData, readResource);
    RETURN_IF_STATUS_ERROR(status, "tablet reader open failed");

    uint64_t docCount = tabletData->GetTabletDocCount();
    assert(docCount < (1UL << 32)); // 32bit doc count

    auto [leftRatio, rightRatio] = rangeInRatio;
    assert(leftRatio <= rightRatio);

    if (leftRatio == INVALID_RATIO) {
        std::string docIdRanges = indexlib::util::GetValueFromKeyValueMap(params, BUILDIN_DOCID_RANGE, "");
        std::vector<docid_t> docRange;
        autil::StringUtil::fromString(docIdRanges, docRange, "_");
        if (docRange.size() == 2 && docRange[0] <= docRange[1]) {
            _docRange = {docRange[0], docRange[1]};
        } else {
            RETURN_STATUS_ERROR(InvalidArgs, "parse __buildin_docid_range__ [%s] failed", docIdRanges.c_str());
        }
    } else {
        auto calc = [docCount](int32_t ratio) -> docid_t { return ratio * docCount / 100; };
        _docRange = {calc(leftRatio), calc(rightRatio + 1)};
    }

    bool keepDeletedDoc = indexlib::util::GetTypeValueFromKeyValueMap(params, BUILDIN_KEEP_DELETED_DOC, false);
    _deletionMapReader = keepDeletedDoc ? nullptr : _tabletReader->GetDeletionMapReader();

    std::string requiredFieldsStr = indexlib::util::GetValueFromKeyValueMap(params, USER_REQUIRED_FIELDS);
    status = InitFieldReaders(requiredFieldsStr);
    RETURN_IF_STATUS_ERROR(status, "init field readers failed for requiredFieldsStr [%s]", requiredFieldsStr.c_str());

    std::string userDefineIndexParamStr = indexlib::util::GetValueFromKeyValueMap(params, USER_DEFINE_INDEX_PARAM);
    status = InitPostingExecutor(userDefineIndexParamStr);
    RETURN_IF_STATUS_ERROR(status, "init PostingExecutor failed for userDefineIndexParamStr [%s]",
                           userDefineIndexParamStr.c_str());

    status = SeekByDocId(_docRange.first);
    RETURN_IF_STATUS_ERROR(status, "seek to doc [%d] failed", _docRange.first);
    AUTIL_LOG(INFO, "doc iter init finish, docRange is [%d, %d)", _docRange.first, _docRange.second);
    return Status::OK();
}

Status NormalTabletDocIterator::InitPostingExecutor(const std::string& userDefineIndexParamStr)
{
    std::vector<indexlib::index::IndexQueryCondition> userDefineIndexParam;
    if (!userDefineIndexParamStr.empty()) {
        try {
            autil::legacy::FromJsonString(userDefineIndexParam, userDefineIndexParamStr);
        } catch (const autil::legacy::ExceptionBase& e) {
            RETURN_STATUS_ERROR(InvalidArgs, "parse user define index param [%s] failed",
                                userDefineIndexParamStr.c_str());
        }
    }
    AUTIL_LOG(INFO, "required [%lu] conditions", userDefineIndexParam.size());
    AUTIL_LOG(INFO, "init posting executor with range [%d, %d)", _docRange.first, _docRange.second);
    _postingExecutor.reset(new indexlib::index::DocidRangePostingExecutor(_docRange.first, _docRange.second));
    if (!userDefineIndexParam.empty()) {
        auto udfExecutor = CreateUserDefinePostingExecutor(userDefineIndexParam);
        if (!udfExecutor) {
            RETURN_STATUS_ERROR(InvalidArgs, "create user define executor error with parameter [%s]",
                                userDefineIndexParamStr.c_str());
        }
        std::vector<std::shared_ptr<indexlib::index::PostingExecutor>> innerExecutors = {udfExecutor, _postingExecutor};
        _postingExecutor.reset(new indexlib::index::AndPostingExecutor(innerExecutors));
    }
    return Status::OK();
}

std::shared_ptr<indexlib::index::PostingExecutor> NormalTabletDocIterator::CreateUserDefinePostingExecutor(
    const std::vector<indexlib::index::IndexQueryCondition>& userDefineParam)
{
    assert(!userDefineParam.empty());
    std::vector<std::shared_ptr<indexlib::index::PostingExecutor>> innerExecutor;
    for (const auto& param : userDefineParam) {
        auto executor = CreateSinglePostingExecutor(param);
        if (!executor) {
            AUTIL_LOG(ERROR, "create posting executor for single condition [%s] fail.",
                      autil::legacy::ToJsonString(param, true).c_str());
            return nullptr;
        }
        innerExecutor.push_back(executor);
    }

    if (innerExecutor.size() == 1) {
        return innerExecutor[0];
    }
    return std::make_shared<indexlib::index::OrPostingExecutor>(innerExecutor);
}

std::shared_ptr<indexlib::index::PostingExecutor>
NormalTabletDocIterator::CreateSinglePostingExecutor(const indexlib::index::IndexQueryCondition& indexQueryCondition)
{
    const std::string& conditionType = indexQueryCondition.GetConditionType();
    const std::vector<indexlib::index::IndexTermInfo>& termInfos = indexQueryCondition.GetIndexTermInfos();

    if (termInfos.empty()) {
        AUTIL_LOG(ERROR, "invalid indexQueryCondition with empty term infos");
        return nullptr;
    }

    if (conditionType == indexlib::index::IndexQueryCondition::TERM_CONDITION_TYPE || termInfos.size() == 1) {
        return CreateTermPostingExecutor(termInfos[0]);
    }

    std::vector<std::shared_ptr<indexlib::index::PostingExecutor>> innerExecutor;
    for (size_t i = 0; i < termInfos.size(); i++) {
        auto executor = CreateTermPostingExecutor(termInfos[i]);
        if (!executor) {
            return nullptr;
        }
        innerExecutor.push_back(executor);
    }

    if (conditionType == indexlib::index::IndexQueryCondition::AND_CONDITION_TYPE) {
        return std::make_shared<indexlib::index::AndPostingExecutor>(innerExecutor);
    }
    if (conditionType == indexlib::index::IndexQueryCondition::OR_CONDITION_TYPE) {
        return std::make_shared<indexlib::index::OrPostingExecutor>(innerExecutor);
    }
    AUTIL_LOG(ERROR, "invalid conditionType [%s].", conditionType.c_str());
    return nullptr;
}

std::shared_ptr<indexlib::index::PostingExecutor>
NormalTabletDocIterator::CreateTermPostingExecutor(const indexlib::index::IndexTermInfo& termInfo)
{
    assert(_tabletReader);
    auto multiIndexReader =
        std::dynamic_pointer_cast<indexlib::index::MultiFieldIndexReader>(_tabletReader->GetMultiFieldIndexReader());
    if (!multiIndexReader) {
        AUTIL_LOG(ERROR, "create term posting executor for index [%s] fail.", termInfo.indexName.c_str());
        return nullptr;
    }

    if (!multiIndexReader->GetIndexReader(termInfo.indexName)) {
        AUTIL_LOG(ERROR, "create term posting executor for index [%s] fail.", termInfo.indexName.c_str());
        return nullptr;
    }
    indexlib::index::Term term(termInfo.term, termInfo.indexName);
    auto iterResult =
        multiIndexReader->Lookup(term, /*pool size*/ indexlib::index::InvertedIndexReader::DEFAULT_STATE_POOL_SIZE,
                                 /*postingtype*/ pt_default, /*pool*/ &_invertedIndexPool);
    if (!iterResult.Ok()) {
        AUTIL_LOG(ERROR, "look up term [%s] for index [%s] failed, error code [%d].", termInfo.term.c_str(),
                  termInfo.indexName.c_str(), (int32_t)iterResult.GetErrorCode());
        return nullptr;
    }
    auto iter = autil::shared_ptr_pool(&_invertedIndexPool, iterResult.Value());
    if (!iter) {
        AUTIL_LOG(INFO, "term [%s] does not exist in index [%s]", termInfo.term.c_str(), termInfo.indexName.c_str());
        return std::make_shared<indexlib::index::DocidRangePostingExecutor>(0, 0);
    }
    return std::make_shared<indexlib::index::TermPostingExecutor>(iter);
}

Status NormalTabletDocIterator::InitFieldReaders(const std::string& requiredFieldsStr)
{
    auto readSchema = _tabletReader->GetSchema();
    std::vector<std::string> fieldNames;
    if (!requiredFieldsStr.empty()) {
        try {
            autil::legacy::FromJsonString(fieldNames, requiredFieldsStr);
            AUTIL_LOG(INFO, "required [%lu] fields [%s]", fieldNames.size(), requiredFieldsStr.c_str());
        } catch (const autil::legacy::ExceptionBase& e) {
            RETURN_STATUS_ERROR(InvalidArgs, "parse required fields [%s] failed", requiredFieldsStr.c_str());
        }
    } else {
        for (const auto& fieldConfig : readSchema->GetFieldConfigs()) {
            fieldNames.push_back(fieldConfig->GetFieldName());
        }
    }

    auto summaryConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        readSchema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME));

    for (const auto& fieldName : fieldNames) {
        if (auto attributeReader = _tabletReader->GetAttributeReader(fieldName)) {
            attributeReader->EnableGlobalReadContext();
            auto iter = attributeReader->CreateSequentialIterator();
            assert(iter);
            _attrFieldIters[fieldName] = std::move(iter);
        } else if (summaryConfig && summaryConfig->GetSummaryConfig(fieldName)) {
            if (!_summaryReader) {
                _summaryReader = _tabletReader->GetSummaryReader();
                _summaryReader->ClearAttrReaders();
                _summaryCount = summaryConfig->GetSummaryCount();
            }
            _summaryFields.push_back({fieldName, summaryConfig->GetSummaryFieldId(readSchema->GetFieldId(fieldName))});
        } else {
            // we don't care virtual attribute or "try best export mode"
            if (readSchema->GetIndexConfig(VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR, fieldName)) {
                AUTIL_LOG(DEBUG, "field [%s] is virtual attribute, not export", fieldName.c_str());
                continue;
            }
            if (_tryBestExport) {
                AUTIL_LOG(WARN, "field [%s] has no attibute and summary, will not export", fieldName.c_str());
                continue;
            }
            RETURN_STATUS_ERROR(InvalidArgs, "get field failed [%s], not store in attribute or summary",
                                fieldName.c_str());
        }
    }
    return Status::OK();
}

Status NormalTabletDocIterator::Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                                     document::IDocument::DocInfo* docInfo)
{
    assert(HasNext());

    rawDocument->setDocOperateType(ADD_DOC);
    rawDocument->setIgnoreEmptyField(true);
    for (auto& [fieldName, attrFieldIter] : _attrFieldIters) {
        std::string fieldValue;
        if (!attrFieldIter->Seek(_currentDocId, fieldValue)) {
            AUTIL_LOG(ERROR, "read attribute [%s] docid [%d] failed", fieldName.c_str(), _currentDocId);
            return Status::Corruption();
        }
        rawDocument->setField(fieldName, fieldValue);
    }

    if (_summaryReader) {
        autil::mem_pool::Pool pool;
        indexlib::document::SearchSummaryDocument summaryDoc(&pool, _summaryCount);
        auto [status, success] = _summaryReader->GetDocument(_currentDocId, &summaryDoc);
        if (!status.IsOK() || !success) {
            AUTIL_LOG(ERROR, "get field from summary failed");
            return Status::Corruption();
        }
        for (auto& [fieldName, fieldId] : _summaryFields) {
            auto fieldValue = summaryDoc.GetFieldValue(fieldId);
            rawDocument->setField(fieldName, *fieldValue);
        }
    }

    auto status = SeekByDocId(_currentDocId + 1);
    RETURN_IF_STATUS_ERROR(status, "seek by docId [%d] failed", _currentDocId);
    std::string ckptStr(sizeof(docid_t), ' ');
    *((docid_t*)ckptStr.data()) = _currentDocId;
    *checkpoint = ckptStr;
    return Status::OK();
}

bool NormalTabletDocIterator::HasNext() const
{
    return _currentDocId != indexlib::END_DOCID && _currentDocId != INVALID_DOCID;
}

Status NormalTabletDocIterator::SeekByDocId(docid_t docId)
{
    try {
        _currentDocId = docId;
        do {
            _currentDocId = _postingExecutor->Seek(_currentDocId);
            if (_currentDocId == indexlib::END_DOCID || _currentDocId == INVALID_DOCID || !_deletionMapReader) {
                return Status::OK();
            }
            if (!_deletionMapReader->IsDeleted(_currentDocId)) {
                return Status::OK();
            }
            _currentDocId++;
        } while (true);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "catch exception [%s]", e.what());
        return Status::InternalError();
    }
}

Status NormalTabletDocIterator::Seek(const std::string& checkpoint)
{
    if (checkpoint.length() != sizeof(docid_t)) {
        RETURN_STATUS_ERROR(InternalError, "invalid checkpoint [%s]", checkpoint.c_str());
    }
    return SeekByDocId(*(docid_t*)(checkpoint.data()));
}

} // namespace indexlibv2::table
