#pragma once

#define TEST_SUMMARY_SERIALIZED_DOCUMENT_SERIALIZE_EQUAL(doc1, doc2)                                                   \
    do {                                                                                                               \
        INDEXLIB_TEST_EQUAL(doc1.GetLength(), doc2.GetLength());                                                       \
        INDEXLIB_TEST_TRUE(0 == memcmp(doc1.GetValue(), doc2.GetValue(), doc1.GetLength()));                           \
    } while (0)

#define TEST_SUMMARY_DOCUMENT_SERIALIZE_EQUAL(summary_doc1, summary_doc2)                                              \
    do {                                                                                                               \
        INDEXLIB_TEST_EQUAL(summary_doc1._fields.size(), summary_doc2._fields.size());                                 \
        for (size_t fieldidx = 0; fieldidx < summary_doc1._fields.size(); fieldidx++) {                                \
            INDEXLIB_TEST_EQUAL(summary_doc1._fields[fieldidx], summary_doc2._fields[fieldidx]);                       \
        }                                                                                                              \
    } while (0)

#define TEST_ATTRIBUTE_DOCUMENT_SERIALIZE_EQUAL(attr_doc1, attr_doc2)                                                  \
    do {                                                                                                               \
        INDEXLIB_TEST_EQUAL(attr_doc1._normalAttrDoc._fields.size(), attr_doc2._normalAttrDoc._fields.size());         \
        for (size_t fieldidx = 0; fieldidx < attr_doc1._normalAttrDoc._fields.size(); fieldidx++) {                    \
            INDEXLIB_TEST_EQUAL(attr_doc1._normalAttrDoc._fields[fieldidx],                                            \
                                attr_doc2._normalAttrDoc._fields[fieldidx]);                                           \
        }                                                                                                              \
        INDEXLIB_TEST_EQUAL(attr_doc1._packFields.size(), attr_doc2._packFields.size());                               \
        for (size_t i = 0; i < attr_doc1._packFields.size(); ++i) {                                                    \
            INDEXLIB_TEST_EQUAL(attr_doc1._packFields[i], attr_doc2._packFields[i]);                                   \
        }                                                                                                              \
    } while (0)

// todo: add check section attribute
#define TEST_INDEX_DOCUMENT_SERIALIZE_EQUAL(index_doc1, index_doc2)                                                    \
    do {                                                                                                               \
        vector<Field*> fields1(index_doc1.GetFieldBegin(), index_doc1.GetFieldEnd());                                  \
        vector<Field*> fields2(index_doc2.GetFieldBegin(), index_doc2.GetFieldEnd());                                  \
        INDEXLIB_TEST_EQUAL(index_doc1.GetPrimaryKey(), index_doc2.GetPrimaryKey());                                   \
        INDEXLIB_TEST_EQUAL(fields1.size(), fields2.size());                                                           \
        for (size_t fieldidx = 0; fieldidx < fields1.size(); fieldidx++) {                                             \
            if (fields1[fieldidx] == nullptr && fields2[fieldidx] == nullptr)                                          \
                continue;                                                                                              \
            INDEXLIB_TEST_TRUE(*fields1[fieldidx] == *fields2[fieldidx]);                                              \
        }                                                                                                              \
        INDEXLIB_TEST_EQUAL(index_doc1.GetMaxIndexIdInSectionAttribute(),                                              \
                            index_doc2.GetMaxIndexIdInSectionAttribute());                                             \
        if (index_doc1.GetMaxIndexIdInSectionAttribute() == INVALID_INDEXID) {                                         \
            break;                                                                                                     \
        }                                                                                                              \
        for (indexid_t indexId = 0; indexId <= index_doc1.GetMaxIndexIdInSectionAttribute(); indexId++) {              \
            INDEXLIB_TEST_EQUAL(index_doc1.GetSectionAttribute(indexId), index_doc2.GetSectionAttribute(indexId));     \
        }                                                                                                              \
    } while (0)

#define TEST_DOCUMENT_SERIALIZE_EQUAL(doc1, doc2)                                                                      \
    do {                                                                                                               \
        TEST_INDEX_DOCUMENT_SERIALIZE_EQUAL((*doc1.GetIndexDocument()), (*doc2.GetIndexDocument()));                   \
        TEST_ATTRIBUTE_DOCUMENT_SERIALIZE_EQUAL((*doc1.GetAttributeDocument()), (*doc2.GetAttributeDocument()));       \
        TEST_SUMMARY_SERIALIZED_DOCUMENT_SERIALIZE_EQUAL((*doc1.GetSummaryDocument()), (*doc2.GetSummaryDocument()));  \
        INDEXLIB_TEST_EQUAL(doc1.GetModifiedFields(), doc2.GetModifiedFields());                                       \
        INDEXLIB_TEST_EQUAL(doc1.GetSubModifiedFields(), doc2.GetSubModifiedFields());                                 \
        INDEXLIB_TEST_EQUAL(doc1.GetTimestamp(), doc2.GetTimestamp());                                                 \
        INDEXLIB_TEST_EQUAL(doc1.GetSource(), doc2.GetSource());                                                       \
        INDEXLIB_TEST_EQUAL(doc1.GetLocator(), doc2.GetLocator());                                                     \
        INDEXLIB_TEST_EQUAL(doc1.GetDocOperateType(), doc2.GetDocOperateType());                                       \
        INDEXLIB_TEST_EQUAL(doc1.GetOriginalOperateType(), doc2.GetOriginalOperateType());                             \
        INDEXLIB_TEST_EQUAL(doc1.GetRegionId(), doc2.GetRegionId());                                                   \
        INDEXLIB_TEST_EQUAL(doc1.NeedTrace(), doc2.NeedTrace());                                                       \
    } while (0)
