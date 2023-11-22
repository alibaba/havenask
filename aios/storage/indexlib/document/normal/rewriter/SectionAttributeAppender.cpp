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
#include "indexlib/document/normal/rewriter/SectionAttributeAppender.h"

#include "autil/ConstString.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/field_format/attribute/StringAttributeConvertor.h"
#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace autil;
using namespace std;
using namespace indexlib::document;
using namespace indexlibv2::config;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SectionAttributeAppender);

SectionAttributeAppender::SectionAttributeAppender() {}

SectionAttributeAppender::SectionAttributeAppender(const SectionAttributeAppender& other)
    : _indexMetaVec(other._indexMetaVec)
{
}

SectionAttributeAppender::~SectionAttributeAppender() {}

bool SectionAttributeAppender::Init(const shared_ptr<ITabletSchema>& schema)
{
    assert(_indexMetaVec.empty());
    const auto& indexConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        const auto& invertedConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
        assert(invertedConfig);
        InvertedIndexType indexType = invertedConfig->GetInvertedIndexType();
        if (indexType == it_pack || indexType == it_expack) {
            auto packIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(invertedConfig);
            assert(packIndexConfig);
            if (!packIndexConfig->HasSectionAttribute()) {
                continue;
            }
            auto sectionAttrConf = packIndexConfig->GetSectionAttributeConfig();
            assert(sectionAttrConf);
            auto attrConfig = sectionAttrConf->CreateAttributeConfig(packIndexConfig->GetIndexName());
            if (attrConfig == nullptr) {
                AUTIL_LOG(ERROR, "section attribute appender init failed, attr config is nullptr, index name[%s]",
                          packIndexConfig->GetIndexName().c_str());
                assert(false);
                return false;
            }
            IndexMeta indexMeta;
            indexMeta.packIndexConfig = packIndexConfig;
            indexMeta.formatter.reset(new indexlib::index::SectionAttributeFormatter(sectionAttrConf));
            indexMeta.convertor.reset(
                new indexlibv2::index::StringAttributeConvertor(attrConfig->IsUniqEncode(), attrConfig->GetAttrName()));
            _indexMetaVec.push_back(indexMeta);
        }
    }
    return !_indexMetaVec.empty();
}

Status SectionAttributeAppender::AppendSectionAttribute(const std::shared_ptr<IndexDocument>& indexDocument)
{
    assert(indexDocument);
    if (indexDocument->GetMaxIndexIdInSectionAttribute() != INVALID_INDEXID) {
        // already append
        return Status::OK();
    }

    for (size_t i = 0; i < _indexMetaVec.size(); ++i) {
        auto status = AppendSectionAttributeForOneIndex(_indexMetaVec[i], indexDocument);
        RETURN_IF_STATUS_ERROR(status, "append section attribute failed");
        status = EncodeSectionAttributeForOneIndex(_indexMetaVec[i], indexDocument);
        RETURN_IF_STATUS_ERROR(status, "encode section attribute failed");
    }
    return Status::OK();
}

Status SectionAttributeAppender::AppendSectionAttributeForOneIndex(const IndexMeta& indexMeta,
                                                                   const std::shared_ptr<IndexDocument>& indexDocument)
{
    _sectionsCountInCurrentDoc = 0;
    _totalSectionLen = 0;
    _sectionOverFlowFlag = false;

    int32_t lastFieldIdxInPack = 0;
    auto sectionAttrConfig = indexMeta.packIndexConfig->GetSectionAttributeConfig();
    bool hasFid = sectionAttrConfig->HasFieldId();
    bool hasSectionWeight = sectionAttrConfig->HasSectionWeight();

    InvertedIndexConfig::Iterator iter = indexMeta.packIndexConfig->CreateIterator();
    while (iter.HasNext() && !_sectionOverFlowFlag) {
        auto fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        Field* field = indexDocument->GetField(fieldId);
        if (!field) {
            AUTIL_LOG(DEBUG, "Field [%d] not exit in IndexDocument", fieldId);
            continue;
        }

        if (field->GetFieldTag() == Field::FieldTag::NULL_FIELD) {
            continue;
        }

        auto tokenizeField = dynamic_cast<IndexTokenizeField*>(field);
        if (!tokenizeField) {
            RETURN_IF_STATUS_ERROR(Status::Corruption(), "fieldTag [%d] is not IndexTokenizeField",
                                   static_cast<int8_t>(field->GetFieldTag()));
        }

        int32_t fieldIdxInPack = indexMeta.packIndexConfig->GetFieldIdxInPack(fieldId);
        if (fieldIdxInPack < 0 || fieldIdxInPack >= (int32_t)PackageIndexConfig::PACK_MAX_FIELD_NUM) {
            AUTIL_LOG(WARN, "fieldId in Section overflow (>= %u): fieldId = %d", PackageIndexConfig::PACK_MAX_FIELD_NUM,
                      fieldIdxInPack);
        }

        AppendSectionAttributeForOneField(tokenizeField, fieldIdxInPack - lastFieldIdxInPack, hasFid, hasSectionWeight);
        lastFieldIdxInPack = fieldIdxInPack;
    }

    if (_sectionOverFlowFlag) {
        AUTIL_LOG(WARN, "Too many sections in doc(%d).", indexDocument->GetDocId());
    }
    return Status::OK();
}

Status SectionAttributeAppender::EncodeSectionAttributeForOneIndex(const IndexMeta& indexMeta,
                                                                   const std::shared_ptr<IndexDocument>& indexDocument)
{
    indexid_t indexId = indexMeta.packIndexConfig->GetIndexId();
    uint8_t buf[indexlib::index::SectionAttributeFormatter::DATA_SLICE_LEN];
    auto [status, encodedSize] = indexMeta.formatter->EncodeToBuffer(_sectionLens, _sectionFids, _sectionWeights,
                                                                     _sectionsCountInCurrentDoc, buf);
    RETURN_IF_STATUS_ERROR(status, "encode section attribute failed");
    const StringView sectionData((const char*)buf, (size_t)encodedSize);
    StringView convertData = indexMeta.convertor->Encode(sectionData, indexDocument->GetMemPool());
    indexDocument->SetSectionAttribute(indexId, convertData);
    return Status::OK();
}

void SectionAttributeAppender::AppendSectionAttributeForOneField(IndexTokenizeField* field, int32_t dGapFid,
                                                                 bool hasFid, bool hasSectionWeight)
{
    assert(field);
    bool firstSecInField = true;
    auto sectionIter = field->Begin();
    for (; sectionIter != field->End(); ++sectionIter) {
        if (_sectionsCountInCurrentDoc == MAX_SECTION_COUNT_PER_DOC) {
            _sectionOverFlowFlag = true;
            break;
        }

        if ((*sectionIter)->GetLength() > Section::MAX_SECTION_LENGTH) {
            AUTIL_LOG(WARN, "Section Length overflow (> %u): length = %u", Section::MAX_SECTION_LENGTH,
                      (*sectionIter)->GetLength());
            ERROR_COLLECTOR_LOG(WARN, "Section Length overflow (> %u): length = %u", Section::MAX_SECTION_LENGTH,
                                (*sectionIter)->GetLength());
        }

        if (hasFid) {
            if (firstSecInField) {
                _sectionFids[_sectionsCountInCurrentDoc] = dGapFid;
                firstSecInField = false;
            } else {
                _sectionFids[_sectionsCountInCurrentDoc] = 0;
            }
        }

        if (hasSectionWeight) {
            _sectionWeights[_sectionsCountInCurrentDoc] = (*sectionIter)->GetWeight();
        }
        _sectionLens[_sectionsCountInCurrentDoc] = (*sectionIter)->GetLength();
        _totalSectionLen += (*sectionIter)->GetLength();
        _sectionsCountInCurrentDoc++;
    }
}

SectionAttributeAppender* SectionAttributeAppender::Clone() { return new SectionAttributeAppender(*this); }
}} // namespace indexlibv2::document
