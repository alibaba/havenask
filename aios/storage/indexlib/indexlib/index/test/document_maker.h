#pragma once

#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DocumentMaker
{
public:
    DocumentMaker();
    ~DocumentMaker();

public:
    typedef std::pair<pos_t, pospayload_t> MockPosition;
    typedef std::vector<std::string> StringVec;
    struct MockDoc {
        MockDoc() : docId(INVALID_DOCID), docPayload(0), firstOcc(0), fieldMap(0) { posList.reserve(1024); }

        static bool lessThan(const MockDoc& doc1, const MockDoc& doc2) { return doc1.docId < doc2.docId; }
        docid_t docId;
        docpayload_t docPayload;
        firstocc_t firstOcc;
        fieldmap_t fieldMap;
        std::vector<MockPosition> posList;
    };
    typedef std::vector<MockDoc> MockPosting;

    typedef std::map<std::string, MockPosting> MockIndex;
    typedef std::map<std::string, MockIndex> MockIndexes;

    typedef std::map<fieldid_t, std::string> MockFields;

    typedef std::vector<MockFields> MockSummary;

    typedef std::vector<std::string> MockAttribute;
    typedef std::map<fieldid_t, MockAttribute> MockAttributes;

    typedef std::set<docid_t> MockDeletionMap;
    typedef std::map<std::string, docid_t> MockPrimaryKey;
    typedef std::map<dictkey_t, std::string> HashKeyToStrMap;

    struct MockIndexPart;
    struct MockIndexPart {
        uint32_t docCount;
        MockIndexes indexes;
        MockSummary summary;
        MockAttributes attributes;
        MockPrimaryKey primaryKey;
        MockDeletionMap deletionMap;
        MockIndexPart* subIndexPart;
        MockIndexPart() : docCount(0), subIndexPart(NULL) {}

        uint32_t GetDocCount() const { return docCount; }
    };

    typedef std::vector<document::NormalDocumentPtr> DocumentVect;

public:
    /**
     * Make documents by input string.
     * @param docStr [in] document string, format:
     * <documents>: {<document>};{<document>}; ...
     * <document>: {docpayload, <fields>};
     * <fields>: [<field>], [<field>], ...
     * <field>: [fieldname: <sections>]
     * <sections>: (<section>) (<section>) ...
     * <section>: (token^pospayload token^pospayload ...)
     *
     * example:
     *
     * { 1,
     *   [field1: (token1^3 token2) (token2^2)],
     *   [field2: (token2 token2^9)],
     * };
     * { 3,
     *   [field2: (token5^3 token2^9)],
     *   [field3: (token2^33 token8^2 token9)],
     * };
     * @param schema [in] index partition schema
     * @param docVect [out] result documents
     * @param answer [out] answer of result documents,
     *  including index, attribute and summary.
     */
    static void MakeDocuments(const std::string& documentsStr, const config::IndexPartitionSchemaPtr& schema,
                              DocumentVect& docVect, MockIndexPart& answer);

    static void MakeDocuments(const std::string& documentsStr, const config::IndexPartitionSchemaPtr& schema,
                              DocumentVect& docVect, MockIndexPart& answer, const std::string& docIdStr);

    static void UpdateDocuments(const config::IndexPartitionSchemaPtr& schema, DocumentVect& segDocVect,
                                MockIndexPart& mockIndexPart);

    static void MakeDocumentsStr(uint32_t docCount, const config::FieldSchemaPtr& fieldSchema,
                                 std::string& documentsStr, const std::string& pkField = "",
                                 int32_t pkStartSuffix = -1);

    static void MakeSortedDocumentsStr(uint32_t docCount, const config::FieldSchemaPtr& fieldSchema,
                                       std::string& documentsStr, const std::string& sortFieldName,
                                       const std::string& pkField = "", int32_t pkStartSuffix = -1);

    static void MakeMultiSortedDocumentsStr(uint32_t docCount, const config::FieldSchemaPtr& fieldSchema,
                                            std::string& documentsStr, const StringVec& sortFieldNameVec,
                                            const StringVec& sortPatterns, const std::string& pkField = "",
                                            int32_t pkStartSuffix = -1);

    static void DeleteDocument(MockIndexPart& answer, docid_t docId);
    static document::NormalDocumentPtr MakeDeletionDocument(docid_t docId, int32_t pkSuffix);

    static document::NormalDocumentPtr MakeDeletionDocument(const std::string& pk, int64_t timeStamp);

    static document::NormalDocumentPtr MakeUpdateDocument(const config::IndexPartitionSchemaPtr& schema,
                                                          const std::string& pk, const std::vector<fieldid_t>& fieldIds,
                                                          const std::vector<std::string>& fieldValues,
                                                          int64_t timestamp);

    static void ReclaimDocuments(const MockIndexPart& oriIndexPart, const ReclaimMapPtr& reclaimMap,
                                 MockIndexPart& newIndexPart);

    static void ReclaimDeletiionMap(const MockDeletionMap& oriDeletionMap, const ReclaimMapPtr& reclaimMap,
                                    MockDeletionMap& newDeletiionMap);
    static void ReclaimPrimaryKey(const MockPrimaryKey& oriPrimaryKey, const ReclaimMapPtr& reclaimMap,
                                  MockPrimaryKey& newPrimaryKey);
    static void ReclaimAttribute(const MockAttributes& oriAttributes, const ReclaimMapPtr& reclaimMap,
                                 MockAttributes& newAttributes);
    static void ReclaimSummary(const MockSummary& oriSummary, const ReclaimMapPtr& reclaimMap, MockSummary& newSummary);
    static void ReclaimIndexs(const MockIndexes& oriIndexes, const ReclaimMapPtr& reclaimMap, MockIndexes& newIndexes);

    static document::NormalDocumentPtr MakeDocument(docid_t docId, const std::string& docStr,
                                                    const config::IndexPartitionSchemaPtr& schema,
                                                    MockIndexPart& answer);

    static document::NormalDocumentPtr MakeDocument(const std::string& docStr,
                                                    const config::IndexPartitionSchemaPtr& schema);

    static document::Field* MakeField(const config::FieldSchemaPtr& fieldSchema, const std::string& str,
                                      autil::mem_pool::Pool* pool = NULL);

    static document::Field* MakeField(const config::FieldSchemaPtr& fieldSchema, const std::string& str,
                                      HashKeyToStrMap& hashKeyToStrMap, autil::mem_pool::Pool* pool = NULL);

private:
    static void InternalMakeDocStr(uint32_t docCount, const config::FieldSchemaPtr& fieldSchema,
                                   std::string& documentsStr, const std::string& sortFieldName,
                                   const std::string& pkField, int32_t pkStartSuffix);

    static void InternalMakeDocStr(uint32_t docCount, const config::FieldSchemaPtr& fieldSchema,
                                   std::string& documentsStr, const StringVec& sortFieldNameVec,
                                   const StringVec& sortPatterns, const std::string& pkField, int32_t pkStartSuffix);

    static int GetSortFieldIdx(const std::string& fieldName, const StringVec& sortFieldNameVec);

    static void SetSortFieldValue(int32_t& curSortFieldValue, const std::string& sortPattern, std::stringstream& ss);

    static void InitAttributes(const config::IndexPartitionSchemaPtr& schema, MockAttributes& attributes);

    static document::NormalDocumentPtr CreateOneDocument(const config::IndexPartitionSchemaPtr& schema);

    static void MakeMockIndex(const config::IndexSchemaPtr& indexSchema, docid_t docId, docpayload_t docPayload,
                              const document::Field* field, const HashKeyToStrMap& hashKeyToStrMap,
                              std::vector<pos_t>& vecBasePos,
                              std::map<std::string, std::set<std::string>>& firstPosSets, MockIndexes& indexes);

    static void SetDocPayload(const config::IndexSchema* indexSchema, const docpayload_t docPayload,
                              const document::IndexTokenizeField* field, const HashKeyToStrMap& hashKeyToStrMap,
                              document::IndexDocumentPtr& indexDoc);
    static void FieldToString(const document::Field* field, const HashKeyToStrMap& hashKeyToStrMap, std::string& str);

private:
    static void InitDefaultAttrDocValue(document::AttributeDocumentPtr& attrDoc,
                                        const config::AttributeSchemaPtr& attrSchema,
                                        const config::FieldSchemaPtr& fieldSchema, autil::mem_pool::Pool* pool);
    static const std::string& GetText(const HashKeyToStrMap& hashKeyToStrMap, dictkey_t key);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentMaker);
}} // namespace indexlib::index
