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
#include "indexlib/table/normal_table/NormalTabletModifier.h"

#include "autil/memory.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/InplaceAttributeModifier.h"
#include "indexlib/index/attribute/PatchAttributeModifier.h"
#include "indexlib/index/deletionmap/DeletionMapModifier.h"
#include "indexlib/index/inverted_index/InplaceInvertedIndexModifier.h"
#include "indexlib/index/inverted_index/PatchInvertedIndexModifier.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexFactory.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/table/normal_table/Common.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletModifier);

NormalTabletModifier::NormalTabletModifier() {}

NormalTabletModifier::~NormalTabletModifier() {}

Status NormalTabletModifier::Init(const std::shared_ptr<config::ITabletSchema>& schema,
                                  const framework::TabletData& tabletData, bool deleteInBranch,
                                  const std::shared_ptr<indexlib::file_system::IDirectory>& op2PatchDir)
{
    _deleteInBranch = deleteInBranch;
    auto st = InitPrimaryKeyReader(schema, tabletData);
    RETURN_IF_STATUS_ERROR(st, "init pk reader failed");
    st = InitDeletionMapModifier(schema, tabletData);
    RETURN_IF_STATUS_ERROR(st, "init deletion map failed");
    st = InitAttributeModifier(schema, tabletData, op2PatchDir);
    RETURN_IF_STATUS_ERROR(st, "init attr modifier failed");
    st = InitInvertedIndexModifier(schema, tabletData, op2PatchDir);
    RETURN_IF_STATUS_ERROR(st, "init inverted index modifier failed");
    return Status::OK();
}

Status NormalTabletModifier::InitDeletionMapModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                                     const framework::TabletData& tabletData)
{
    auto deletionMapConfig = schema->GetIndexConfig(index::DELETION_MAP_INDEX_TYPE_STR, index::DELETION_MAP_INDEX_NAME);
    if (!deletionMapConfig) {
        Status status = Status::NotFound("deletionmap config is not found in schema");
        RETURN_IF_STATUS_ERROR(status, "");
    }
    _deletionMapModifier = make_unique<index::DeletionMapModifier>();
    auto openStatus = _deletionMapModifier->Open(deletionMapConfig, &tabletData);
    RETURN_IF_STATUS_ERROR(openStatus, "open deletion map reader failed");
    return Status::OK();
}

Status
NormalTabletModifier::InitAttributeModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                            const framework::TabletData& tabletData,
                                            const std::shared_ptr<indexlib::file_system::IDirectory>& op2PatchDir)
{
    if (op2PatchDir) {
        _patchAttributeModifier = std::make_unique<index::PatchAttributeModifier>(schema, op2PatchDir);
        return _patchAttributeModifier->Init(tabletData);
    }
    _inplaceAttrModifier = std::make_unique<index::InplaceAttributeModifier>(schema);
    return _inplaceAttrModifier->Init(tabletData);
}

Status
NormalTabletModifier::InitInvertedIndexModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const framework::TabletData& tabletData,
                                                const std::shared_ptr<indexlib::file_system::IDirectory>& op2PatchDir)
{
    if (op2PatchDir) {
        _patchInvertedIndexModifier =
            std::make_unique<indexlib::index::PatchInvertedIndexModifier>(schema, op2PatchDir);
        return _patchInvertedIndexModifier->Init(tabletData);
    }
    _inplaceInvertedIndexModifier = std::make_unique<indexlib::index::InplaceInvertedIndexModifier>(schema);
    return _inplaceInvertedIndexModifier->Init(tabletData);
}

Status NormalTabletModifier::InitPrimaryKeyReader(const std::shared_ptr<config::ITabletSchema>& schema,
                                                  const framework::TabletData& tabletData)
{
    auto pkConfigs = schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (pkConfigs.size() != 1) {
        Status status = Status::NotFound("primarykey config size is ", pkConfigs.size(), ", not 1");
        RETURN_IF_STATUS_ERROR(status, "");
    }
    auto pkConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(pkConfigs[0]);
    assert(pkConfig);
    index::PrimaryKeyIndexFactory factory;
    auto pkReader = factory.CreateIndexReader(pkConfig, index::IndexerParameter {});
    _pkReader.reset(autil::dynamic_unique_cast<indexlib::index::PrimaryKeyIndexReader>(std::move(pkReader)).release());
    // OpenWithoutPKAttribute 会更新segment中的indexer，但是不会重新打开pk attribute
    // 在reader open的时候去更改segment中的indexer行为不太好
    auto status = _pkReader->OpenWithoutPKAttribute(pkConfigs[0], &tabletData);
    RETURN_IF_STATUS_ERROR(status, "open primarykey reader failed");
    return Status::OK();
}

Status NormalTabletModifier::RemoveDocument(docid_t docid)
{
    if (_deleteInBranch) {
        return _deletionMapModifier->DeleteInBranch(docid);
    }
    return _deletionMapModifier->Delete(docid);
}
Status NormalTabletModifier::RemoveDocuments(document::IDocumentBatch* batch)
{
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch);
    while (iter->HasNext()) {
        auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(iter->Next().get());
        docid_t oldDocid = INVALID_DOCID;
        if (ADD_DOC == normalDoc->GetDocOperateType()) {
            assert(INVALID_DOCID != normalDoc->GetDocId());
            oldDocid = normalDoc->GetDeleteDocId();
        } else {
            oldDocid = normalDoc->GetDocId();
        }
        if (oldDocid == INVALID_DOCID) {
            continue;
        }
        switch (normalDoc->GetDocOperateType()) {
        case ADD_DOC:
        case DELETE_DOC:
            RETURN_IF_STATUS_ERROR(RemoveDocument(oldDocid), "delete docid [%d] failed", oldDocid);
            break;
        case UPDATE_FIELD:
        case SKIP_DOC: // SKIP_DOC might come from AddToUpdateRewrite
            break;
        default:
            return Status::Unimplement("not support modify doc with type %d", normalDoc->GetDocOperateType());
        }
    }
    return Status::OK();
}

Status NormalTabletModifier::ModifyDocument(document::IDocumentBatch* batch)
{
    auto st = RemoveDocuments(batch);
    RETURN_IF_STATUS_ERROR(st, "remove documents failed ");

    assert(_inplaceAttrModifier);
    st = _inplaceAttrModifier->Update(batch);
    RETURN_IF_STATUS_ERROR(st, "update attr failed ");

    assert(_inplaceInvertedIndexModifier);
    st = _inplaceInvertedIndexModifier->Update(batch);
    RETURN_IF_STATUS_ERROR(st, "update inverted index failed ");
    return Status::OK();
}

Status NormalTabletModifier::Close()
{
    assert(_patchAttributeModifier);
    assert(_patchInvertedIndexModifier);
    auto status = _patchAttributeModifier->Close();
    RETURN_IF_STATUS_ERROR(status, "patch attribute modifier close failed");
    status = _patchInvertedIndexModifier->Close();
    RETURN_IF_STATUS_ERROR(status, "patch inverted index modifier close failed");
    return Status::OK();
}

bool NormalTabletModifier::UpdateFieldValue(docid_t docId, const std::string& fieldName, const autil::StringView& value,
                                            bool isNull)
{
    return _inplaceAttrModifier ? _inplaceAttrModifier->UpdateFieldWithName(docId, fieldName, value, isNull)
                                : _patchAttributeModifier->UpdateFieldWithName(docId, fieldName, value, isNull);
}

Status NormalTabletModifier::UpdateFieldTokens(docid_t docId, const indexlib::document::ModifiedTokens& modifiedTokens)
{
    return _inplaceInvertedIndexModifier
               ? _inplaceInvertedIndexModifier->UpdateOneFieldTokens(docId, modifiedTokens, /*isForReplay=*/true)
               : _patchInvertedIndexModifier->UpdateOneFieldTokens(docId, modifiedTokens, /*isForReplay=*/true);
}
bool NormalTabletModifier::UpdateIndexTerms(indexlib::index::IndexUpdateTermIterator* iterator)
{
    assert(_inplaceInvertedIndexModifier);
    return _inplaceInvertedIndexModifier->UpdateOneIndexTermsForReplay(iterator);
}
} // namespace indexlibv2::table
