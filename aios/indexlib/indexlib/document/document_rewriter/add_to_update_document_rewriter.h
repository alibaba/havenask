#ifndef __INDEXLIB_ADD_TO_UPDATE_DOCUMENT_REWRITER_H
#define __INDEXLIB_ADD_TO_UPDATE_DOCUMENT_REWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/document.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/common/sort_description.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class AddToUpdateDocumentRewriter : public document::DocumentRewriter
{
public:
    AddToUpdateDocumentRewriter();
    ~AddToUpdateDocumentRewriter();
public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const std::vector<config::TruncateOptionConfigPtr>& truncateOptionConfigs,
              const std::vector<common::SortDescriptions>& sortDescVec);

    void Rewrite(const document::DocumentPtr& doc) override;

    document::AttributeDocumentPtr TryRewrite(const document::NormalDocumentPtr& doc);
    void RewriteIndexDocument(const document::NormalDocumentPtr& doc);
    
private:
    void AddUpdatableFields(const config::IndexPartitionSchemaPtr& schema, 
                            const std::vector<common::SortDescriptions>& sortDescVec);

    void FilterTruncateSortFields(
            const config::TruncateOptionConfigPtr& truncOptionConfig);
    void FilterTruncateProfileField(
            const config::TruncateProfilePtr& profile);
    void FilterTruncateStrategyField(
            const config::TruncateStrategyPtr& strategy);

    bool SetField(const std::string& fieldName, bool isUpdatable);

    bool SetUselessField(const std::string& uselessFieldName);

    void AllocFieldBitmap();

    bool IsSortField(const std::string& fieldName,
                     const std::vector<common::SortDescriptions>& sortDescVec);

private:
    config::FieldSchemaPtr mFieldSchema;
    util::Bitmap mUpdatableFieldIds;
    util::Bitmap mUselessFieldIds;
    document::AttributeDocumentFieldExtractorPtr mAttrFieldExtractor;
    
private:
    friend class AddToUpdateDocumentRewriterTest;
    friend class DocumentRewriterCreatorTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AddToUpdateDocumentRewriter);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_ADD_TO_UPDATE_DOCUMENT_REWRITER_H
