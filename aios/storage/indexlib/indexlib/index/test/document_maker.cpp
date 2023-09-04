#include "indexlib/index/test/document_maker.h"

#include <vector>

#include "autil/ConstString.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/document/locator.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/util/Random.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;

using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, DocumentMaker);

DocumentMaker::DocumentMaker() {}

DocumentMaker::~DocumentMaker() {}

void DocumentMaker::MakeDocuments(const string& documentsStr, const IndexPartitionSchemaPtr& schema,
                                  DocumentVect& docVect, MockIndexPart& answer)
{
    autil::StringTokenizer st(documentsStr, ";",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);

    docVect.reserve(st.getNumTokens());
    docid_t baseDocId = answer.docCount;

    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        NormalDocumentPtr doc = MakeDocument((docid_t)(i + baseDocId), st[i], schema, answer);
        docVect.push_back(doc);
    }
    answer.docCount += docVect.size();
}

void DocumentMaker::MakeDocuments(const string& documentsStr, const IndexPartitionSchemaPtr& schema,
                                  DocumentVect& docVect, MockIndexPart& answer, const string& docIdStr)
{
    autil::StringTokenizer st(documentsStr, ";",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    vector<docid_t> docIds;
    autil::StringUtil::fromString(docIdStr, docIds, ";");
    assert(docIds.size() == st.getNumTokens());

    docVect.reserve(st.getNumTokens());

    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        NormalDocumentPtr doc = MakeDocument(docIds[i], st[i], schema, answer);
        docVect.push_back(doc);
    }
    answer.docCount += docVect.size();
}

void DocumentMaker::UpdateDocuments(const IndexPartitionSchemaPtr& schema, DocumentVect& segDocVect,
                                    MockIndexPart& mockIndexPart)
{
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    SummaryFormatter summaryFormatter(schema->GetSummarySchema());
    for (size_t i = 0; i < segDocVect.size(); ++i) {
        segDocVect[i]->SetDocOperateType(UPDATE_FIELD);
        docid_t docId = segDocVect[i]->GetDocId();
        assert(docId != INVALID_DOCID);
        // get attribute doc
        AttributeDocumentPtr attributeDocument = segDocVect[i]->GetAttributeDocument();
        AttributeDocumentPtr newAttributeDocument(new AttributeDocument());

        SummaryDocument::Iterator iter = attributeDocument->CreateIterator();
        while (iter.HasNext()) {
            fieldid_t fieldId = -1;
            const autil::StringView fieldVal = iter.Next(fieldId);
            FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldId);
            AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                attrSchema->GetAttributeConfigByFieldId(fieldId)));
            assert(convertor);
            FieldType fieldType = fieldConfig->GetFieldType();
            if (fieldType != ft_uint32) {
                continue;
            }

            uint32_t newFieldVal = -1;
            autil::StringUtil::strToUInt32(fieldVal.data(), newFieldVal);
            newFieldVal++;
            string newFieldValStr = autil::StringUtil::toString(newFieldVal);

            StringView encodeStr = convertor->Encode(StringView(newFieldValStr), segDocVect[i]->GetPool());

            // change attribute doc
            newAttributeDocument->SetField(fieldId, encodeStr);

            // change mock attribute index
            mockIndexPart.attributes[fieldId][docId] = newFieldValStr;

            // change mock summary index
            mockIndexPart.summary[docId][fieldId] = newFieldValStr;
        } // end while
        segDocVect[i]->SetAttributeDocument(newAttributeDocument);
    } // end for
}

void DocumentMaker::MakeDocumentsStr(uint32_t docCount, const FieldSchemaPtr& fieldSchema, string& documentsStr,
                                     const string& pkField, int32_t pkStartSuffix)
{
    InternalMakeDocStr(docCount, fieldSchema, documentsStr, "", pkField, pkStartSuffix);
}

void DocumentMaker::MakeSortedDocumentsStr(uint32_t docCount, const FieldSchemaPtr& fieldSchema, string& documentsStr,
                                           const string& sortFieldName, const string& pkField, int32_t pkStartSuffix)
{
    InternalMakeDocStr(docCount, fieldSchema, documentsStr, sortFieldName, pkField, pkStartSuffix);
}

void DocumentMaker::MakeMultiSortedDocumentsStr(uint32_t docCount, const config::FieldSchemaPtr& fieldSchema,
                                                std::string& documentsStr, const StringVec& sortFieldNameVec,
                                                const StringVec& sortPatterns, const std::string& pkField,
                                                int32_t pkStartSuffix)
{
    InternalMakeDocStr(docCount, fieldSchema, documentsStr, sortFieldNameVec, sortPatterns, pkField, pkStartSuffix);
}

void DocumentMaker::DeleteDocument(MockIndexPart& answer, docid_t docId)
{
    answer.deletionMap.insert(docId);
    MockIndexPart* subIndexPart = answer.subIndexPart;
    if (subIndexPart) {
        subIndexPart->deletionMap.insert(docId);
    }
}

NormalDocumentPtr DocumentMaker::MakeDeletionDocument(docid_t docId, int32_t pkSuffix)
{
    NormalDocumentPtr doc(new NormalDocument());
    doc->SetDocId(docId);
    doc->SetDocOperateType(DELETE_DOC);

    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    stringstream ss;
    ss << "pk_" << pkSuffix;
    indexDoc->SetPrimaryKey(ss.str());

    doc->SetIndexDocument(indexDoc);
    return doc;
}

NormalDocumentPtr DocumentMaker::MakeDeletionDocument(const string& pk, int64_t timestamp)
{
    NormalDocumentPtr doc(new NormalDocument());
    doc->SetTimestamp(timestamp);
    doc->SetDocOperateType(DELETE_DOC);

    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    indexDoc->SetPrimaryKey(pk);

    doc->SetIndexDocument(indexDoc);
    return doc;
}

NormalDocumentPtr DocumentMaker::MakeUpdateDocument(const IndexPartitionSchemaPtr& schema, const string& pk,
                                                    const vector<fieldid_t>& fieldIds,
                                                    const vector<string>& fieldValues, int64_t timestamp)
{
    NormalDocumentPtr doc(new NormalDocument());
    doc->SetTimestamp(timestamp);
    doc->SetDocOperateType(UPDATE_FIELD);

    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    indexDoc->SetPrimaryKey(pk);
    doc->SetIndexDocument(indexDoc);

    AttributeDocumentPtr attrDoc(new AttributeDocument());
    assert(fieldIds.size() == fieldValues.size());

    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    assert(fieldSchema);

    for (size_t i = 0; i < fieldIds.size(); i++) {
        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldIds[i]);
        assert(fieldConfig);
        AttributeConvertorPtr attrConvertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
            schema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldIds[i])));
        StringView encodeStr = attrConvertor->Encode(StringView(fieldValues[i]), doc->GetPool());
        attrDoc->SetField(fieldIds[i], encodeStr);
    }
    doc->SetAttributeDocument(attrDoc);
    return doc;
}

void DocumentMaker::InternalMakeDocStr(uint32_t docCount, const FieldSchemaPtr& fieldSchema, string& documentsStr,
                                       const string& sortFieldName, const string& pkField, int32_t pkStartSuffix)
{
    int32_t numberSeed = 0;
    int32_t curSortFieldValue = docCount;
    int32_t sectionLength = 1;
    int32_t tokenNum = 1;
    bool hasDocPayload = false;
    bool hasPosPayload = false;

    string token = "token";
    documentsStr.clear();

    for (uint32_t i = 0; i < docCount; ++i) {
        documentsStr.append("{");
        if (hasDocPayload) {
            stringstream ss;
            ss << numberSeed << ", ";
            numberSeed = (numberSeed * 3 + 1) % 100; // random
            documentsStr.append(ss.str());
            hasDocPayload = false;
        } else {
            hasDocPayload = true;
        }
        for (size_t j = 0; j < fieldSchema->GetFieldCount(); ++j) {
            documentsStr.append("[");
            FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(j);
            const string& fieldName = fieldConfig->GetFieldName();
            documentsStr.append(fieldName + ":");
            if (!pkField.empty() && pkField == fieldName) {
                stringstream ss;
                ss << "pk_" << pkStartSuffix;
                documentsStr.append("(");
                documentsStr.append(ss.str());
                documentsStr.append(") ");
                ++pkStartSuffix;
            } else if (fieldConfig->GetFieldType() == ft_text) {
                sectionLength = (sectionLength * 3 + 2) % 7 + 1; // random
                for (int32_t section = 0; section < sectionLength; section++) {
                    documentsStr.append("(");
                    tokenNum = (tokenNum * 3 + 2) % 29 + 1;
                    for (int32_t t = 0; t < tokenNum; ++t) {
                        stringstream ss;
                        ss << token << numberSeed;
                        numberSeed = (numberSeed * 3 + 1) % 100; // random
                        if (hasPosPayload) {
                            ss << "^" << numberSeed;
                            numberSeed = (numberSeed * 3 + 1) % 100; // random
                            hasPosPayload = false;
                        } else {
                            hasPosPayload = true;
                        }
                        documentsStr.append(ss.str());
                        if (t != tokenNum - 1) {
                            documentsStr.append(" ");
                        }
                    }
                    documentsStr.append(") ");
                }
            } else if (fieldConfig->GetFieldType() == ft_string) {
                documentsStr.append("(");
                stringstream ss;
                ss << token << numberSeed;
                numberSeed = (numberSeed * 3 + 1) % 100; // random
                documentsStr.append(ss.str());
                documentsStr.append(") ");
            } else {
                documentsStr.append("(");
                stringstream ss;
                if (fieldName != sortFieldName) {
                    ss << numberSeed;
                    numberSeed = (numberSeed * 7 + 11) % 100; // random
                } else {
                    ss << curSortFieldValue;
                    if ((util::dev_urandom() % 2) == 0) {
                        curSortFieldValue--;
                    }
                }
                documentsStr.append(ss.str());
                documentsStr.append(") ");
            }
            documentsStr.append("],");
        }
        documentsStr.append("}");
        if (i < docCount - 1) {
            documentsStr.append(";\n");
        }
    }
}

void DocumentMaker::InternalMakeDocStr(uint32_t docCount, const FieldSchemaPtr& fieldSchema, string& documentsStr,
                                       const StringVec& sortFieldNameVec, const StringVec& sortPatterns,
                                       const string& pkField, int32_t pkStartSuffix)
{
    int32_t numberSeed = 0;
    // uint32_t curSortFieldValue = docCount;
    int32_t sectionLength = 1;
    int32_t tokenNum = 1;
    bool hasDocPayload = false;
    bool hasPosPayload = false;

    std::vector<int32_t> curSortFieldValueVec;
    for (size_t i = 0; i < sortPatterns.size(); ++i) {
        if (sortPatterns[i] == "ASC") {
            curSortFieldValueVec.push_back(0);
        } else {
            curSortFieldValueVec.push_back(docCount);
        }
    }

    string token = "token";
    documentsStr.clear();

    for (uint32_t i = 0; i < docCount; ++i) {
        documentsStr.append("{");
        if (hasDocPayload) {
            stringstream ss;
            ss << numberSeed << ", ";
            numberSeed = (numberSeed * 3 + 1) % 100; // random
            documentsStr.append(ss.str());
            hasDocPayload = false;
        } else {
            hasDocPayload = true;
        }
        for (size_t j = 0; j < fieldSchema->GetFieldCount(); ++j) {
            documentsStr.append("[");
            FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(j);
            const string& fieldName = fieldConfig->GetFieldName();
            documentsStr.append(fieldName + ":");
            if (!pkField.empty() && pkField == fieldName) {
                stringstream ss;
                ss << "pk_" << pkStartSuffix;
                documentsStr.append("(");
                documentsStr.append(ss.str());
                documentsStr.append(") ");
                ++pkStartSuffix;
            } else if (fieldConfig->GetFieldType() == ft_text) {
                sectionLength = (sectionLength * 3 + 2) % 7 + 1; // random
                for (int32_t section = 0; section < sectionLength; section++) {
                    documentsStr.append("(");
                    tokenNum = (tokenNum * 3 + 2) % 29 + 1;
                    for (int32_t t = 0; t < tokenNum; ++t) {
                        stringstream ss;
                        ss << token << numberSeed;
                        numberSeed = (numberSeed * 3 + 1) % 100; // random
                        if (hasPosPayload) {
                            ss << "^" << numberSeed;
                            numberSeed = (numberSeed * 3 + 1) % 100; // random
                            hasPosPayload = false;
                        } else {
                            hasPosPayload = true;
                        }
                        documentsStr.append(ss.str());
                        if (t != tokenNum - 1) {
                            documentsStr.append(" ");
                        }
                    }
                    documentsStr.append(") ");
                }
            } else if (fieldConfig->GetFieldType() == ft_string) {
                documentsStr.append("(");
                stringstream ss;
                ss << token << numberSeed;
                numberSeed = (numberSeed * 3 + 1) % 100; // random
                documentsStr.append(ss.str());
                documentsStr.append(") ");
            } else {
                documentsStr.append("(");
                stringstream ss;
                int iFieldIdx = GetSortFieldIdx(fieldName, sortFieldNameVec);
                if (iFieldIdx < 0) {
                    ss << numberSeed;
                    numberSeed = (numberSeed * 7 + 11) % 100; // random
                } else {
                    SetSortFieldValue(curSortFieldValueVec[iFieldIdx], sortPatterns[iFieldIdx], ss);
                }
                documentsStr.append(ss.str());
                documentsStr.append(") ");
            }
            documentsStr.append("],");
        }
        documentsStr.append("}");
        if (i < docCount - 1) {
            documentsStr.append(";\n");
        }
    }
}

int DocumentMaker::GetSortFieldIdx(const std::string& fieldName, const StringVec& sortFieldNameVec)
{
    for (size_t i = 0; i < sortFieldNameVec.size(); ++i) {
        if (fieldName == sortFieldNameVec[i]) {
            return i;
        }
    }
    return -1;
}

void DocumentMaker::SetSortFieldValue(int32_t& curSortFieldValue, const std::string& sortPattern, stringstream& ss)
{
    ss << curSortFieldValue;
    if ((util::dev_urandom() % 2) == 0) {
        if (sortPattern == "ASC") {
            curSortFieldValue++;
        } else {
            curSortFieldValue--;
        }
    }
}

document::NormalDocumentPtr DocumentMaker::MakeDocument(const std::string& docStr,
                                                        const config::IndexPartitionSchemaPtr& schema)
{
    DocumentMaker::MockIndexPart mockIndexPart;
    return MakeDocument(0, docStr, schema, mockIndexPart);
}

NormalDocumentPtr DocumentMaker::MakeDocument(docid_t docId, const string& docStr,
                                              const IndexPartitionSchemaPtr& schema, MockIndexPart& answer)
{
    MockIndexes& indexes = answer.indexes;
    answer.summary.reserve(1024);
    MockFields summary;
    MockAttributes& attributes = answer.attributes;
    InitAttributes(schema, attributes);

    string str = docStr.substr(1, docStr.length() - 2);
    autil::StringTokenizer st(str, ",",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);

    size_t cursor = 0;

    docpayload_t docPayload = 0;
    if (st.getNumTokens() > 0 && st[cursor].find('[') == string::npos) {
        bool ret = autil::StringUtil::strToUInt16(st[0].c_str(), docPayload);
        assert(ret);
        (void)ret;
        ++cursor;
    }

    uint32_t docVersion = INVALID_DOC_VERSION;
    if (cursor < st.getNumTokens() && st[cursor].find('[') == string::npos && st[cursor][0] == '#') {
        // parse docVersion
        bool ret = autil::StringUtil::strToUInt32(st[cursor].c_str() + 1, docVersion);
        assert(ret);
        (void)ret;
        ++cursor;
    }

    NormalDocumentPtr doc = CreateOneDocument(schema);
    autil::mem_pool::Pool* pool = doc->GetPool();
    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    SummaryDocumentPtr summaryDoc(new SummaryDocument());
    AttributeDocumentPtr attributeDoc = doc->GetAttributeDocument();
    InitDefaultAttrDocValue(attributeDoc, schema->GetAttributeSchema(), schema->GetFieldSchema(), pool);

    Locator locator(StringUtil::toString(docId));
    doc->SetLocator(locator);
    doc->SetTimestamp(docId);
    doc->SetDocOperateType(ADD_DOC);

    vector<pos_t> vecBasePos;
    map<string, set<string>> firstPosSets;

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    if (indexSchema) {
        vecBasePos.resize(indexSchema->GetIndexCount(), 0);
    }
    for (; cursor < st.getNumTokens(); ++cursor) {
        HashKeyToStrMap hashKeyToStrMap;
        FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
        Field* field = MakeField(fieldSchema, st[cursor], hashKeyToStrMap, pool);
        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(field->GetFieldId());
        string strField;
        FieldToString(field, hashKeyToStrMap, strField);
        SummarySchemaPtr summarySchema = schema->GetSummarySchema();
        if (summarySchema && summarySchema->IsInSummary(field->GetFieldId())) {
            summaryDoc->SetField(field->GetFieldId(), autil::MakeCString(strField, pool));
            summary[field->GetFieldId()] = strField;
        }
        AttributeSchemaPtr attributeSchema = schema->GetAttributeSchema();
        if (attributeSchema && attributeSchema->IsInAttribute(field->GetFieldId())) {
            AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                attributeSchema->GetAttributeConfigByFieldId(field->GetFieldId())));
            assert(convertor);
            StringView encodeStr = convertor->Encode(StringView(strField), pool);
            attributeDoc->SetField(field->GetFieldId(), encodeStr);
            MockAttribute& mockAttriubte = attributes[field->GetFieldId()];
            *(mockAttriubte.rbegin()) = strField;
        }

        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        if (indexSchema && indexSchema->IsInIndex(field->GetFieldId())) {
            indexDoc->SetField(field->GetFieldId(), field);
            if (indexSchema->HasPrimaryKeyIndex() && indexSchema->GetPrimaryKeyIndexFieldId() == field->GetFieldId()) {
                indexDoc->SetPrimaryKey(strField);
                MockPrimaryKey& mockPrimaryKey = answer.primaryKey;

                MockPrimaryKey::const_iterator it = mockPrimaryKey.find(strField);
                if (it != mockPrimaryKey.end()) {
                    docid_t oldDocId = it->second;
                    MockDeletionMap& deletionMap = answer.deletionMap;
                    deletionMap.insert(oldDocId);
                }
                mockPrimaryKey[strField] = docId;
            }
            // TODO: mockIndex support raw field
            if (fieldConfig->GetFieldType() != ft_raw) {
                MakeMockIndex(indexSchema, docId, docPayload, field, hashKeyToStrMap, vecBasePos, firstPosSets,
                              indexes);
                if (docPayload != 0) {
                    SetDocPayload(indexSchema.get(), docPayload, dynamic_cast<IndexTokenizeField*>(field),
                                  hashKeyToStrMap, indexDoc);
                }
            }
        } else {
            IE_POOL_COMPATIBLE_DELETE_CLASS(pool, field);
            field = NULL;
        }
    }

    if (schema->GetSummarySchema()) {
        SerializedSummaryDocumentPtr serDoc;
        SummaryFormatter formatter(schema->GetSummarySchema());
        formatter.SerializeSummaryDoc(summaryDoc, serDoc);
        doc->SetSummaryDocument(serDoc);
        answer.summary.push_back(summary);
    }
    doc->SetDocId(docId);

    return doc;
}

void DocumentMaker::InitAttributes(const IndexPartitionSchemaPtr& schema, MockAttributes& attributes)
{
    if (schema->GetAttributeSchema()) {
        AttributeSchemaPtr attributeSchema = schema->GetAttributeSchema();
        for (size_t i = 0; i < attributeSchema->GetAttributeCount(); ++i) {
            AttributeConfigPtr attrConfig = attributeSchema->GetAttributeConfig(i);
            const FieldConfigPtr& fieldConfig = attrConfig->GetFieldConfig();
            MockAttribute& mockAttribute = attributes[fieldConfig->GetFieldId()];
            mockAttribute.reserve(1024);
            if (fieldConfig->GetFieldType() <= ft_string || attrConfig->IsMultiValue()) {
                mockAttribute.push_back("");
            } else {
                mockAttribute.push_back("0");
            }
        }
    }
}

NormalDocumentPtr DocumentMaker::CreateOneDocument(const IndexPartitionSchemaPtr& schema)
{
    bool hasPK = false;
    (void)hasPK;
    NormalDocumentPtr doc(new NormalDocument());
    IndexDocumentPtr indexDoc;
    if (schema->GetIndexSchema()) {
        IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
        doc->SetIndexDocument(indexDoc);
        hasPK = schema->GetIndexSchema()->HasPrimaryKeyIndex();
    }
    // SummaryDocumentPtr summaryDoc;
    // if (schema->GetSummarySchema())
    // {
    //     summaryDoc.reset(new SummaryDocument());
    //     doc->SetSummaryDocument(summaryDoc);
    // }
    AttributeDocumentPtr attributeDoc;
    if (schema->GetAttributeSchema()) {
        attributeDoc.reset(new AttributeDocument());
        doc->SetAttributeDocument(attributeDoc);
    }
    return doc;
}

Field* DocumentMaker::MakeField(const FieldSchemaPtr& fieldSchema, const string& str, autil::mem_pool::Pool* pool)
{
    HashKeyToStrMap hashKeyToStrMap;
    return MakeField(fieldSchema, str, hashKeyToStrMap, pool);
}

/**
 * <field>: [fieldname: <sections>]
 * <sections>: (<section>) (<section>) ...
 * <section>: (token^pospayload token^pospayload ...)
 */
Field* DocumentMaker::MakeField(const FieldSchemaPtr& fieldSchema, const string& str, HashKeyToStrMap& hashKeyToStrMap,
                                autil::mem_pool::Pool* pool)
{
    string fieldStr = str.substr(1, str.length() - 2);
    autil::StringTokenizer st1(fieldStr, ":",
                               autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    string fieldName = st1[0];
    const FieldConfigPtr& fieldConfig = fieldSchema->GetFieldConfig(fieldName);
    assert(fieldConfig);
    fieldid_t fieldId = fieldConfig->GetFieldId();

    if (fieldConfig->GetFieldType() == ft_raw) {
        auto field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexRawField, pool);
        field->SetData(autil::MakeCString(st1[1], pool));
        return field;
    }

    auto field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);

    field->SetFieldId(fieldId);

    if (st1.getNumTokens() == 1) {
        return field;
    }
    assert(st1.getNumTokens() == 2);

    const string& sectionStr = st1[1];
    size_t sectionStart = 0;
    size_t sectionEnd = 0;
    bool first = true;
    while (true) {
        sectionStart = sectionStr.find('(', sectionEnd);
        if (sectionStart == string::npos) {
            break;
        }
        sectionEnd = sectionStr.find(')', sectionStart);
        string oneSection = sectionStr.substr(sectionStart + 1, sectionEnd - sectionStart - 1);
        autil::StringTokenizer st2(oneSection, " ",
                                   autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
        assert(st2.getNumTokens() > 0);
        Section* section = field->CreateSection();
        section->SetLength(st2.getNumTokens() + 1);
        for (size_t i = 0; i < st2.getNumTokens(); ++i) {
            autil::StringTokenizer st3(st2[i], "^",
                                       autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
            assert(st3.getNumTokens() <= 2);

            pospayload_t posPayload = 0;
            if (st3.getNumTokens() == 2) {
                autil::StringUtil::strToUInt8(st3[1].c_str(), posPayload);
            }

            pos_t inc = 1;
            if (i == 0) {
                if (first) {
                    first = false;
                    inc = 0;
                } else {
                    inc = 2;
                }
            }
            dictkey_t key;
            KeyHasherWrapper::GetHashKeyByFieldType(fieldConfig->GetFieldType(), st3[0].c_str(), st3[0].size(), key);
            hashKeyToStrMap[key] = st3[0];
            section->CreateToken(key, inc, posPayload);
        }
    }
    return field;
}

void DocumentMaker::InitDefaultAttrDocValue(AttributeDocumentPtr& attrDoc, const AttributeSchemaPtr& attrSchema,
                                            const FieldSchemaPtr& fieldSchema, autil::mem_pool::Pool* pool)
{
    for (fieldid_t fid = 0; fid < (fieldid_t)fieldSchema->GetFieldCount(); ++fid) {
        if (attrSchema && attrSchema->IsInAttribute(fid)) {
            FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fid);
            AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                attrSchema->GetAttributeConfigByFieldId(fid)));
            StringView encodeStr = convertor->Encode(StringView::empty_instance(), pool);
            attrDoc->SetField(fid, encodeStr);
        }
    }
}

void DocumentMaker::MakeMockIndex(const IndexSchemaPtr& indexSchema, docid_t docId, docpayload_t docPayload,
                                  const Field* field, const HashKeyToStrMap& hashKeyToStrMap, vector<pos_t>& vecBasePos,
                                  map<string, set<string>>& firstPosSets, MockIndexes& indexes)
{
    for (uint32_t i = 0; i < indexSchema->GetIndexCount(); ++i) {
        const IndexConfigPtr& indexConfig = indexSchema->GetIndexConfig(i);
        if (!indexConfig->IsInIndex(field->GetFieldId())) {
            continue;
        }

        if (indexConfig == indexSchema->GetPrimaryKeyIndexConfig()) {
            continue;
        }

        MockIndex& index = indexes[indexConfig->GetIndexName()];
        set<string>& firstPosSet = firstPosSets[indexConfig->GetIndexName()];

        // TODO support RAW_FIELD
        auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
        for (auto fieldIter = tokenizeField->Begin(); fieldIter != tokenizeField->End(); ++fieldIter) {
            const Section* section = *fieldIter;
            for (size_t j = 0; j < section->GetTokenCount(); ++j) {
                const Token* token = section->GetToken(j);
                const string& tokenText = GetText(hashKeyToStrMap, token->GetHashKey());
                MockPosting& mockPosting = index[tokenText];
                mockPosting.reserve(1024);
                if (mockPosting.size() == 0 || mockPosting[mockPosting.size() - 1].docId != docId) {
                    MockDoc newMockDoc;
                    newMockDoc.docId = docId;
                    newMockDoc.docPayload = docPayload;
                    mockPosting.push_back(newMockDoc);
                }
                MockDoc& mockDoc = mockPosting[mockPosting.size() - 1];

                InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
                if (indexType == it_expack) {
                    if (firstPosSet.find(tokenText) == firstPosSet.end()) {
                        if (vecBasePos[i] > numeric_limits<firstocc_t>::max()) {
                            mockDoc.firstOcc = numeric_limits<firstocc_t>::max();
                        } else {
                            mockDoc.firstOcc = (firstocc_t)vecBasePos[i];
                        }
                        firstPosSet.insert(tokenText);
                    }

                    PackageIndexConfigPtr packageIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
                    int32_t fieldIdx = packageIndexConfig->GetFieldIdxInPack(field->GetFieldId());
                    mockDoc.fieldMap |= (fieldmap_t)(1 << fieldIdx);
                }
                mockDoc.posList.push_back(make_pair(vecBasePos[i]++, token->GetPosPayload()));
            }
            vecBasePos[i]++;
        }
    }
}

// TODO: DocumentMaker should support correct doc payload for different terms within a doc.
void DocumentMaker::SetDocPayload(const IndexSchema* indexSchema, const docpayload_t docPayload,
                                  const IndexTokenizeField* field, const HashKeyToStrMap& hashKeyToStrMap,
                                  IndexDocumentPtr& indexDoc)
{
    for (auto fieldIter = field->Begin(); fieldIter != field->End(); ++fieldIter) {
        const Section* section = *fieldIter;
        for (size_t i = 0; i < section->GetTokenCount(); ++i) {
            const Token* token = section->GetToken(i);
            const std::string& tokenText = GetText(hashKeyToStrMap, token->GetHashKey());
            auto indexConfigs = indexSchema->CreateIterator(/*needVirtual=*/false, /*type=*/IndexStatus::is_normal);
            for (auto indexIt = indexConfigs->Begin(); indexIt != indexConfigs->End(); indexIt++) {
                if (!(*indexIt)->GetTruncatePayloadConfig().GetName().empty()) {
                    indexDoc->SetDocPayload((*indexIt)->GetTruncatePayloadConfig(), tokenText, docPayload);
                }
            }
            indexDoc->SetDocPayload(token->GetHashKey(), docPayload);
        }
    }
}

void DocumentMaker::FieldToString(const Field* field, const HashKeyToStrMap& hashKeyToStrMap, string& str)
{
    if (field->GetFieldTag() == Field::FieldTag::RAW_FIELD) {
        auto rawField = dynamic_cast<const IndexRawField*>(field);
        assert(rawField);
        str = rawField->GetData().to_string();
        return;
    }

    const IndexTokenizeField* tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    assert(tokenizeField);
    str.clear();
    auto fieldIter = tokenizeField->Begin();
    if (fieldIter == tokenizeField->End()) {
        return;
    }
    while (true) {
        const Section* section = *fieldIter;
        for (size_t i = 0; i < section->GetTokenCount(); ++i) {
            const Token* token = section->GetToken(i);
            str.append(GetText(hashKeyToStrMap, token->GetHashKey()));
            if (i < section->GetTokenCount() - 1) {
                str.append(" ");
            }
        }
        ++fieldIter;
        if (fieldIter == tokenizeField->End()) {
            break;
        }
        str.append("\n");
    }
}

void DocumentMaker::ReclaimDocuments(const MockIndexPart& oriIndexPart, const ReclaimMapPtr& reclaimMap,
                                     MockIndexPart& newIndexPart)
{
    ReclaimIndexs(oriIndexPart.indexes, reclaimMap, newIndexPart.indexes);
    ReclaimSummary(oriIndexPart.summary, reclaimMap, newIndexPart.summary);
    ReclaimAttribute(oriIndexPart.attributes, reclaimMap, newIndexPart.attributes);
    ReclaimPrimaryKey(oriIndexPart.primaryKey, reclaimMap, newIndexPart.primaryKey);
    ReclaimDeletiionMap(oriIndexPart.deletionMap, reclaimMap, newIndexPart.deletionMap);
    newIndexPart.docCount = oriIndexPart.docCount - reclaimMap->GetDeletedDocCount();
}

void DocumentMaker::ReclaimIndexs(const MockIndexes& oriIndexes, const ReclaimMapPtr& reclaimMap,
                                  MockIndexes& newIndexes)
{
    MockIndexes::const_iterator it = oriIndexes.begin();
    for (; it != oriIndexes.end(); ++it) {
        const MockIndex& mockIndex = it->second;
        MockIndex& newMockIndex = newIndexes[it->first];
        MockIndex::const_iterator indexIt = mockIndex.begin();
        for (; indexIt != mockIndex.end(); ++indexIt) {
            const MockPosting& mockPosting = indexIt->second;
            MockPosting& newMockPosting = newMockIndex[indexIt->first];
            for (size_t i = 0; i < mockPosting.size(); ++i) {
                const MockDoc& mockDoc = mockPosting[i];
                docid_t newDocId = reclaimMap->GetNewId(mockDoc.docId);
                if (newDocId != INVALID_DOCID) {
                    MockDoc newMockDoc = mockDoc;
                    newMockDoc.docId = newDocId;
                    newMockPosting.push_back(newMockDoc);
                }
            }
            if (newMockPosting.size() == 0) {
                newMockIndex.erase(indexIt->first);
            } else {
                sort(newMockPosting.begin(), newMockPosting.end(), MockDoc::lessThan);
            }
        }
    }
}

void DocumentMaker::ReclaimSummary(const MockSummary& oriSummary, const ReclaimMapPtr& reclaimMap,
                                   MockSummary& newSummary)
{
    newSummary.reserve(oriSummary.size());
    for (size_t i = 0; i < oriSummary.size(); ++i) {
        docid_t newDocId = reclaimMap->GetNewId((docid_t)i);
        if (newDocId != INVALID_DOCID) {
            if ((size_t)newDocId >= newSummary.size()) {
                newSummary.resize(newDocId + 1);
            }
            newSummary[newDocId] = oriSummary[i];
        }
    }
}

void DocumentMaker::ReclaimAttribute(const MockAttributes& oriAttributes, const ReclaimMapPtr& reclaimMap,
                                     MockAttributes& newAttributes)
{
    MockAttributes::const_iterator it = oriAttributes.begin();
    for (; it != oriAttributes.end(); ++it) {
        MockAttribute& newMockAttribute = newAttributes[it->first];
        newMockAttribute.reserve(reclaimMap->GetTotalDocCount());
        const MockAttribute& oldMockAttribute = it->second;
        for (size_t i = 0; i < oldMockAttribute.size(); ++i) {
            docid_t newDocId = reclaimMap->GetNewId((docid_t)i);
            if (newDocId != INVALID_DOCID) {
                if ((size_t)newDocId >= newMockAttribute.size()) {
                    newMockAttribute.resize(newDocId + 1);
                }
                newMockAttribute[newDocId] = oldMockAttribute[i];
            }
        }
    }
}

void DocumentMaker::ReclaimPrimaryKey(const MockPrimaryKey& oriPrimaryKey, const ReclaimMapPtr& reclaimMap,
                                      MockPrimaryKey& newPrimaryKey)
{
    MockPrimaryKey::const_iterator it = oriPrimaryKey.begin();
    for (; it != oriPrimaryKey.end(); ++it) {
        docid_t newDocId = reclaimMap->GetNewId((docid_t)it->second);
        if (newDocId != INVALID_DOCID) {
            newPrimaryKey[it->first] = newDocId;
        }
    }
}

void DocumentMaker::ReclaimDeletiionMap(const MockDeletionMap& oriDeletionMap, const ReclaimMapPtr& reclaimMap,
                                        MockDeletionMap& newDeletionMap)
{
    MockDeletionMap::const_iterator it = oriDeletionMap.begin();
    for (; it != oriDeletionMap.end(); ++it) {
        docid_t newDocId = reclaimMap->GetNewId((docid_t)*it);
        if (newDocId != INVALID_DOCID) {
            newDeletionMap.insert(newDocId);
        }
    }
}

const std::string& DocumentMaker::GetText(const HashKeyToStrMap& hashKeyToStrMap, dictkey_t key)
{
    HashKeyToStrMap::const_iterator it = hashKeyToStrMap.find(key);
    assert(it != hashKeyToStrMap.end());
    return it->second;
}
}} // namespace indexlib::index
