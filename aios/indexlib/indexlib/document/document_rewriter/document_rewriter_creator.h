#ifndef __INDEXLIB_DOCUMENT_REWRITER_CREATOR_H
#define __INDEXLIB_DOCUMENT_REWRITER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/sort_description.h"

DECLARE_REFERENCE_CLASS(document, AddToUpdateDocumentRewriter);

IE_NAMESPACE_BEGIN(document);

class DocumentRewriterCreator
{
public:
    DocumentRewriterCreator();
    ~DocumentRewriterCreator();
public:
    static document::DocumentRewriter* CreateTimestampRewriter(
            const config::IndexPartitionSchemaPtr& schema);

    static document::DocumentRewriter* CreateAddToUpdateDocumentRewriter(
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const common::SortDescriptions& sortDesc);

    static document::DocumentRewriter* CreateAddToUpdateDocumentRewriter(
            const config::IndexPartitionSchemaPtr& schema,
            const std::vector<config::IndexPartitionOptions>& optionVec,
            const std::vector<common::SortDescriptions>& sortDescVec);

    static document::DocumentRewriter* CreateSectionAttributeRewriter(
	const config::IndexPartitionSchemaPtr& schema);

    static document::DocumentRewriter* CreatePackAttributeRewriter(
	const config::IndexPartitionSchemaPtr& schema);

    static document::DocumentRewriter* CreateDeleteToAddDocumentRewriter();

private:
    static AddToUpdateDocumentRewriter* DoCreateAddToUpdateDocumentRewriter(
            const config::IndexPartitionSchemaPtr& schema,
            const std::vector<config::TruncateOptionConfigPtr>& truncateOptionConfigs,
            const std::vector<common::SortDescriptions>& sortDescVec);

    static std::vector<config::TruncateOptionConfigPtr> GetTruncateOptionConfigs(
            const std::vector<config::IndexPartitionOptions>& optionVec);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentRewriterCreator);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_REWRITER_CREATOR_H
