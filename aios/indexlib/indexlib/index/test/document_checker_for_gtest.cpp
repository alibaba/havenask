#include "indexlib/indexlib.h"
#include "indexlib/index/test/document_checker_for_gtest.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h"
#include "indexlib/index/normal/summary/summary_reader_impl.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/config/high_frequency_vocabulary.h"
#include "indexlib/test/unittest.h"
#include "autil/legacy/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocumentCheckerForGtest);

DocumentCheckerForGtest::DocumentCheckerForGtest() 
{
}

DocumentCheckerForGtest::~DocumentCheckerForGtest() 
{
}

void DocumentCheckerForGtest::CheckData(const partition::IndexPartitionReaderPtr& reader,
                                        const config::IndexPartitionSchemaPtr& schema,
                                        const MockIndexPart& mockIndexPart) 
{
    CheckSummaryData(reader->GetSummaryReader(),
                     schema->GetSummarySchema(),
                     mockIndexPart.summary,
                     mockIndexPart.deletionMap);
    
    CheckAttributesData(reader, schema->GetAttributeSchema(), 
                        mockIndexPart.attributes, 
                        mockIndexPart.deletionMap);

    CheckIndexesData(reader->GetIndexReader(), 
                     schema->GetIndexSchema(), 
                     mockIndexPart);
    CheckDeletionMap(reader->GetDeletionMapReader(), mockIndexPart.deletionMap);
}
    
void DocumentCheckerForGtest::CheckDeletionMap( 
        const DeletionMapReaderPtr& actualDeletionMap, 
        const MockDeletionMap& deletionMap)
{
    assert(actualDeletionMap);
    INDEXLIB_TEST_EQUAL(deletionMap.size(),
                        actualDeletionMap->GetDeletedDocCount());
    for (MockDeletionMap::const_iterator it = deletionMap.begin(); 
         it != deletionMap.end(); ++it)
    {
        INDEXLIB_TEST_TRUE(actualDeletionMap->IsDeleted(*it));
    }
}

void DocumentCheckerForGtest::CheckIndexesData(const IndexReaderPtr& indexReader,
                                       const IndexSchemaPtr& indexSchema,
                                       const MockIndexPart& mockIndexPart)
{
    if (!indexSchema)
    {
        return;
    }
    const MockDeletionMap& deletionMap = mockIndexPart.deletionMap;
    const MockIndexes& mockIndexes = mockIndexPart.indexes;
    IndexConfigPtr pkIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();

    for (uint32_t i = 0; i < indexSchema->GetIndexCount(); i++)
    {
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
        const string& indexName = indexConfig->GetIndexName();
        if (pkIndexConfig && indexName == pkIndexConfig->GetIndexName())
        {
            CheckPrimaryKeyIndexData(indexReader, indexConfig,
                    mockIndexPart.primaryKey, mockIndexPart.deletionMap);
            continue;
        }

        MockIndexes::const_iterator it = mockIndexes.find(indexName);
        assert(it != mockIndexes.end());
        
        CheckIndexData(indexReader, indexConfig, it->second, deletionMap);
    }
}

void DocumentCheckerForGtest::CheckIndexData(const IndexReaderPtr& indexReader,
                                     const IndexConfigPtr& indexConfig,
                                     const MockIndex& mockIndex, 
                                     const MockDeletionMap& deletionMap)
{
    vector<string> toCheckTokens;
    for (MockIndex::const_iterator it = mockIndex.begin(); 
         it != mockIndex.end(); it++)
    {
        toCheckTokens.push_back(it->first);
    }
    CheckIndexData(indexReader, indexConfig, mockIndex, deletionMap, 
                   toCheckTokens);
}

void DocumentCheckerForGtest::CheckIndexData(const IndexReaderPtr& indexReader,
                                     const IndexSchemaPtr& indexSchema,
                                     const string& indexName,
                                     const MockIndexPart& mockIndexPart,
                                     const vector<string>& toCheckTokens)
{
    if (!indexSchema)
    {
        return;
    }
    const MockDeletionMap& deletionMap = mockIndexPart.deletionMap;
    const MockIndexes& mockIndexes = mockIndexPart.indexes;
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig);
    MockIndexes::const_iterator it = mockIndexes.find(indexName);
    assert(it != mockIndexes.end());
    CheckIndexData(indexReader, indexConfig, it->second, 
                   deletionMap, toCheckTokens);
}

void DocumentCheckerForGtest::CheckPrimaryKeyIndexData(
        const IndexReaderPtr& indexReader,
        const IndexConfigPtr& indexConfig, 
        const MockPrimaryKey& primaryKey,
        const MockDeletionMap& deletionMap)
{
    INDEXLIB_TEST_TRUE(indexReader);    
    for (MockPrimaryKey::const_iterator keyIt = primaryKey.begin();
         keyIt != primaryKey.end(); ++keyIt)
    {
        IE_LOG(TRACE1, "primary key: [%s]", keyIt->first.c_str());
        if (deletionMap.find(keyIt->second) != deletionMap.end())
        {
            IE_LOG(TRACE1, "deleted docid: %d.", keyIt->second);
            continue;
        }

        Term term(keyIt->first, indexConfig->GetIndexName());
        autil::mem_pool::Pool pool;
        PostingIterator* postIter = indexReader->Lookup(term, 1000, pt_default, &pool);
        assert(postIter);
        INDEXLIB_TEST_TRUE(postIter);
        docid_t docId = postIter->SeekDoc(INVALID_DOCID);
        IE_LOG(TRACE1, "primary key: [%s], expect: %d, actual: %d",
               keyIt->first.c_str(), keyIt->second, docId);
        
        assert(keyIt->second == docId);
        INDEXLIB_TEST_EQUAL(keyIt->second, docId);
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, postIter);
    }
}

void DocumentCheckerForGtest::CheckIndexData(const IndexReaderPtr& indexReader,
                                     const IndexConfigPtr& indexConfig,
                                     const MockIndex& mockIndex, 
                                     const MockDeletionMap& deletionMap,
                                     const vector<string>& toCheckTokens)
{
    INDEXLIB_TEST_TRUE(indexReader);
    docid_t docId = INVALID_DOCID;
    docid_t expectDocId = INVALID_DOCID;
    for (vector<string>::const_iterator keyIt = toCheckTokens.begin();
         keyIt != toCheckTokens.end(); ++keyIt)
    {
        IE_LOG(TRACE1, "Lookup key: %s", (*keyIt).c_str());
        Term term(*keyIt, indexConfig->GetIndexName());

        MockIndex::const_iterator it = mockIndex.find(*keyIt);
        assert(it != mockIndex.end());
        const MockPosting& mockPosting = it->second;
        autil::mem_pool::Pool pool;
        PostingIterator *postIter = indexReader->Lookup(term,
                mockPosting.size(), pt_normal, &pool);
        assert(postIter);
        INDEXLIB_TEST_TRUE(postIter);
                
        for (size_t i = 0; i < mockPosting.size(); ++i)
        {
            const MockDoc& mockDoc = mockPosting[i];
            expectDocId = mockDoc.docId;
            // if (deletionMap.find(expectDocId) != deletionMap.end())
            // {
            //     IE_LOG(TRACE1, "deleted docid: %d.", mockDoc.docId);
            //     continue;
            // }
            docId = postIter->SeekDoc(docId);

            INDEXLIB_TEST_EQUAL(expectDocId, docId);
            IE_LOG(TRACE2, "docid, expect: %d, actual: %d", mockDoc.docId, docId);
            
            IndexType indexType = indexConfig->GetIndexType();
            if (indexType == it_pack || indexType == it_expack 
                || indexType == it_text)
            {
                INDEXLIB_TEST_EQUAL(mockDoc.docPayload,
                        postIter->GetDocPayload());
                IE_LOG(TRACE2, "docpayload, expect: %d, actual: %d", 
                       mockDoc.docPayload, postIter->GetDocPayload());
            }
            else
            {
                INDEXLIB_TEST_EQUAL(0, postIter->GetDocPayload());
            }
                    
            if (!postIter->HasPosition())
            {
                continue;
            }

            TermMatchData termMatchData;
            postIter->Unpack(termMatchData);
            INDEXLIB_TEST_EQUAL((tf_t)mockDoc.posList.size(), 
                    termMatchData.GetTermFreq());

            if (indexType == it_expack)
            {
                INDEXLIB_TEST_EQUAL((fieldmap_t)mockDoc.fieldMap,
                        termMatchData.GetFieldMap());
            }

            InDocPositionIteratorPtr positionIter = 
                termMatchData.GetInDocPositionIterator();
            pos_t pos = 0;
            for (size_t j = 0; j < mockDoc.posList.size(); ++j)
            {
                pos = positionIter->SeekPosition(pos);
                INDEXLIB_TEST_EQUAL(mockDoc.posList[j].first, pos);
                IE_LOG(TRACE2, "pos, expect: %d, actual: %d",
                       mockDoc.posList[j].first, pos);

                // mockDoc does not have answer. just call to confirm it works
                positionIter->GetSectionId();
                positionIter->GetSectionLength();
                positionIter->GetSectionWeight();
                positionIter->GetFieldId();

                INDEXLIB_TEST_EQUAL(mockDoc.posList[j].second,
                        positionIter->GetPosPayload());
                IE_LOG(TRACE2, "pospayload, expect: %d, actual: %d",
                       mockDoc.posList[j].second, positionIter->GetPosPayload());
            }
            INDEXLIB_TEST_EQUAL(INVALID_POSITION,
                    positionIter->SeekPosition(pos));
        }
        if (postIter)
        {
            docId = postIter->SeekDoc(docId);
            INDEXLIB_TEST_EQUAL(INVALID_DOCID, docId);
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, postIter);
    }
    
    CheckBitmapData(indexReader, indexConfig, mockIndex, deletionMap);
}

void DocumentCheckerForGtest::CheckBitmapData(const IndexReaderPtr& indexReader,
                                      const IndexConfigPtr& indexConfig,
                                      const MockIndex& mockIndex,
                                      const MockDeletionMap& deletionMap)
{
    HighFrequencyVocabularyPtr vol = indexConfig->GetHighFreqVocabulary();
    if (vol == NULL)
    {
        return;
    }

    for (MockIndex::const_iterator keyIt = mockIndex.begin();
         keyIt != mockIndex.end(); ++keyIt)
    {
        if (!vol->Lookup(keyIt->first))
        {
            continue;
        }

        Term term(keyIt->first, indexConfig->GetIndexName());
        autil::mem_pool::Pool pool;
        const MockPosting& mockPosting = keyIt->second;
        PostingIterator *postIter = indexReader->Lookup(term, 
                mockPosting.size(), pt_bitmap, &pool);
        INDEXLIB_TEST_TRUE(postIter);        

        docid_t docId = INVALID_DOCID;
        for (size_t i = 0; i < mockPosting.size(); ++i)
        {
            const MockDoc& mockDoc = mockPosting[i];
            docid_t expectDocId = mockDoc.docId;
            // if (deletionMap.find(expectDocId) != deletionMap.end())
            // {
            //     IE_LOG(TRACE1, "deleted docid: %d.", mockDoc.docId);
            //     continue;
            // }
            docId = postIter->SeekDoc(docId);
//            assert(expectDocId == docId);
            INDEXLIB_TEST_EQUAL(expectDocId, docId);
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, postIter);
    }
}

void DocumentCheckerForGtest::CheckSummaryData(const SummaryReaderPtr& summaryReader,
                                       const config::SummarySchemaPtr& summarySchema,
                                       const MockSummary& mockSummary,
                                       const MockDeletionMap& deletionMap)
{
    if (!summarySchema)
    {
        return;
    }
    for (size_t i = 0; i < mockSummary.size(); i++)
    {
        if (deletionMap.find(i) != deletionMap.end())
        {
            continue;
        }
        SearchSummaryDocument summaryDoc(NULL, summarySchema->GetSummaryCount());
        summaryReader->GetDocument((docid_t)i, &summaryDoc);

        const MockFields& mockFields = mockSummary[i];
        for (MockFields::const_iterator fieldIt = mockFields.begin(); 
                fieldIt != mockFields.end(); fieldIt++)
        {
            fieldid_t fieldId = fieldIt->first;
            summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)fieldId);
            if (summaryFieldId != INVALID_SUMMARYFIELDID)
            {
                const autil::ConstString* str = summaryDoc.GetFieldValue(summaryFieldId);
                string actualField;
                if (str != NULL)
                {
                    actualField.assign(str->data(), str->size());
                }
                string expectField = fieldIt->second;
                INDEXLIB_TEST_EQUAL(expectField, actualField);
            }
        }
    }
}

void DocumentCheckerForGtest::CheckAttributesData(const partition::IndexPartitionReaderPtr& reader,
                                                  const config::AttributeSchemaPtr& attributeSchema,
                                                  const MockAttributes& mockAttributes,
                                                  const MockDeletionMap& deletionMap)
{
    if (!attributeSchema)
    {
        return;
    }

    for (size_t i = 0; i < attributeSchema->GetAttributeCount(); i++)
    {
        AttributeConfigPtr attributeConfig =
            attributeSchema->GetAttributeConfig(i);
        MockAttributes::const_iterator it = mockAttributes.find(
                attributeConfig->GetFieldConfig()->GetFieldId());
        assert(it != mockAttributes.end());
        AttributeReaderPtr attributeReader = reader->GetAttributeReader(
                attributeConfig->GetFieldConfig()->GetFieldName());
        
        CheckAttributeData(attributeReader, 
                           attributeConfig, 
                           it->second, 
                           deletionMap);
    }
}

void DocumentCheckerForGtest::CheckAttributeData(const AttributeReaderPtr& attributeReader,
        const config::AttributeConfigPtr& attributeConfig,
        const MockAttribute& mockAttribute,
        const MockDeletionMap& deletionMap)
{
    string attrName = attributeConfig->GetAttrName();
    if (attrName == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME
        || attrName == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME)
    {
        //TODO
        return;
    }

    Pool pool;
    INDEXLIB_TEST_TRUE(attributeReader);
    for (size_t i = 0; i < mockAttribute.size(); i++)
    {
        if (deletionMap.find(i) != deletionMap.end())
        {
            continue;
        }

        string value;
        bool ret = attributeReader->Read((docid_t)i, value, &pool);
        INDEXLIB_TEST_TRUE(ret) << attrName;
        INDEXLIB_TEST_EQUAL(mockAttribute[i], value) << attrName;
    }
}

IE_NAMESPACE_END(index);

