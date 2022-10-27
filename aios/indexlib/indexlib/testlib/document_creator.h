#ifndef __INDEXLIB_DOCUMENT_CREATOR_H
#define __INDEXLIB_DOCUMENT_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/testlib/raw_document.h"

IE_NAMESPACE_BEGIN(testlib);

class DocumentCreator
{
public:
    DocumentCreator();
    ~DocumentCreator();
public:
    static document::NormalDocumentPtr CreateDocument(
            const config::IndexPartitionSchemaPtr& schema,
            const std::string& docString,
	    bool needRewriteSectionAttribute = true);

    static std::vector<document::NormalDocumentPtr> CreateDocuments(
            const config::IndexPartitionSchemaPtr& schema,
            const std::string& docString,
	    bool needRewriteSectionAttribute = true);
private:
    static void SetPrimaryKey(config::IndexPartitionSchemaPtr schema,
                              RawDocumentPtr rawDoc,
                              document::DocumentPtr doc,
                              bool isLegacyKvIndexDocument);
    static void SetDocumentValue(config::IndexPartitionSchemaPtr schema,
                                 RawDocumentPtr rawDoc,
                                 document::DocumentPtr doc);

    static document::NormalDocumentPtr CreateDocument(
            const config::IndexPartitionSchemaPtr& schema,
            RawDocumentPtr rawDoc);

    static regionid_t ExtractRegionIdFromRawDocument(
            const config::IndexPartitionSchemaPtr& schema,
            const RawDocumentPtr& rawDocument,
            const  std::string& defaultRegionName);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentCreator);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_DOCUMENT_CREATOR_H
