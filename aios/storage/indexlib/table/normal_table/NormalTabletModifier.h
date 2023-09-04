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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/operation_log/OperationLogProcessor.h"
namespace indexlibv2::config {
class ITabletSchema;
}
namespace indexlibv2::document {
class IDocumentBatch;
}
namespace indexlibv2::framework {
class TabletData;
}
namespace indexlib::file_system {
class IDirectory;
}
namespace indexlibv2::index {
class DeletionMapModifier;
class InplaceAttributeModifier;
class PatchAttributeModifier;
} // namespace indexlibv2::index
namespace indexlib::index {
class PrimaryKeyIndexReader;
class InplaceInvertedIndexModifier;
class IndexUpdateTermIterator;
class PatchInvertedIndexModifier;
} // namespace indexlib::index
namespace indexlibv2::table {

// NormalTabletModifier inherits from OperationLogProcessor only in order to reuse code. There is no logical reason
// behind this.
// TODO: We need a better design for NormalTabletModifier and OperationLogProcessor.
class NormalTabletModifier : public indexlib::index::OperationLogProcessor
{
public:
    NormalTabletModifier();
    ~NormalTabletModifier();

    using IndexReaderMapKey = std::pair<std::string, std::string>;

public:
    Status Init(const std::shared_ptr<config::ITabletSchema>& schema, const framework::TabletData& tabletData,
                bool deleteInBranch, const std::shared_ptr<indexlib::file_system::IDirectory>& op2PatchDir);
    Status ModifyDocument(document::IDocumentBatch* batch);
    Status RemoveDocument(docid_t docid) override;

    bool UpdateFieldValue(docid_t docId, const std::string& fieldName, const autil::StringView& value,
                          bool isNull) override;
    Status UpdateFieldTokens(docid_t docId, const indexlib::document::ModifiedTokens& modifiedTokens) override;
    bool UpdateIndexTerms(indexlib::index::IndexUpdateTermIterator* iterator);

    Status Close();

private:
    Status RemoveDocuments(document::IDocumentBatch* batch);
    Status InitDeletionMapModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                   const framework::TabletData& tabletData);
    Status InitAttributeModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                 const framework::TabletData& tabletData,
                                 const std::shared_ptr<indexlib::file_system::IDirectory>& op2PatchDir);
    Status InitInvertedIndexModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                     const framework::TabletData& tabletData,
                                     const std::shared_ptr<indexlib::file_system::IDirectory>& op2PatchDir);
    Status InitPrimaryKeyReader(const std::shared_ptr<config::ITabletSchema>& schema,
                                const framework::TabletData& tabletData);

private:
    std::unique_ptr<indexlib::index::PrimaryKeyIndexReader> _pkReader;
    std::unique_ptr<index::DeletionMapModifier> _deletionMapModifier;
    std::unique_ptr<index::InplaceAttributeModifier> _inplaceAttrModifier;
    std::unique_ptr<indexlib::index::InplaceInvertedIndexModifier> _inplaceInvertedIndexModifier;
    std::unique_ptr<index::PatchAttributeModifier> _patchAttributeModifier;
    std::unique_ptr<indexlib::index::PatchInvertedIndexModifier> _patchInvertedIndexModifier;
    bool _deleteInBranch;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
