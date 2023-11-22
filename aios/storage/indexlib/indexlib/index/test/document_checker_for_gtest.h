#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"

namespace indexlib { namespace index {

class DocumentCheckerForGtest
{
public:
    typedef DocumentMaker::MockDoc MockDoc;
    typedef DocumentMaker::MockPosting MockPosting;
    typedef DocumentMaker::MockIndex MockIndex;
    typedef DocumentMaker::MockIndexes MockIndexes;

    typedef DocumentMaker::MockFields MockFields;
    typedef DocumentMaker::MockSummary MockSummary;

    typedef DocumentMaker::MockAttribute MockAttribute;
    typedef DocumentMaker::MockAttributes MockAttributes;

    typedef DocumentMaker::MockDeletionMap MockDeletionMap;
    typedef DocumentMaker::MockPrimaryKey MockPrimaryKey;

    typedef DocumentMaker::MockIndexPart MockIndexPart;

public:
    DocumentCheckerForGtest();
    ~DocumentCheckerForGtest();

public:
    void CheckData(const partition::IndexPartitionReaderPtr& reader, const config::IndexPartitionSchemaPtr& schema,
                   const MockIndexPart& mockIndexPart);

    void CheckIndexesData(const std::shared_ptr<InvertedIndexReader>& indexReader,
                          const config::IndexSchemaPtr& indexSchema, const MockIndexPart& mockIndexPart);

    void CheckIndexData(const std::shared_ptr<InvertedIndexReader>& indexReader,
                        const config::IndexSchemaPtr& indexSchema, const std::string& indexName,
                        const MockIndexPart& mockIndexPart, const std::vector<std::string>& toCheckTokens);

    void CheckIndexData(const std::shared_ptr<InvertedIndexReader>& indexReader,
                        const config::IndexConfigPtr& indexConfig, const MockIndex& mockIndex,
                        const MockDeletionMap& deletionMap);

    void CheckIndexData(const std::shared_ptr<InvertedIndexReader>& indexReader,
                        const config::IndexConfigPtr& indexConfig, const MockIndex& mockIndex,
                        const MockDeletionMap& deletionMap, const std::vector<std::string>& toCheckTokens);

    void CheckBitmapData(const std::shared_ptr<InvertedIndexReader>& indexReader,
                         const config::IndexConfigPtr& indexConfig, const MockIndex& mockIndex,
                         const MockDeletionMap& deletionMap);

    void CheckPrimaryKeyIndexData(const std::shared_ptr<InvertedIndexReader>& indexReader,
                                  const config::IndexConfigPtr& indexConfig, const MockPrimaryKey& primaryKey,
                                  const MockDeletionMap& deletionMap);

    void CheckSummaryData(const SummaryReaderPtr& summaryReader, const config::SummarySchemaPtr& summarySchema,
                          const MockSummary& mockSummary, const MockDeletionMap& deletionMap);

    void CheckAttributesData(const partition::IndexPartitionReaderPtr& reader,
                             const config::AttributeSchemaPtr& attributeSchema, const MockAttributes& mockAttributes,
                             const MockDeletionMap& deletionMap);

    void CheckAttributeData(const AttributeReaderPtr& attributeReader,
                            const config::AttributeConfigPtr& attributeConfig, const MockAttribute& mockAttribute,
                            const MockDeletionMap& deletionMap);

    void CheckDeletionMap(const DeletionMapReaderPtr& actualDeletionMap, const MockDeletionMap& deletionMap);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentCheckerForGtest);
}} // namespace indexlib::index
