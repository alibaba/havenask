#include <autil/StringUtil.h>
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/config/number_index_type_transformor.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexDocumentMaker);

uint64_t IndexDocumentMaker::GetHashKey(const string &key, TokenType tokenType)
{
    uint64_t hashKey = 0;
    if (tokenType == tt_text)
    {
        hashKey = autil::HashAlgorithm::hashString64(key.c_str());
    }
    else 
    {
        StringUtil::strToUInt64(key.c_str(), hashKey);
    }
    return hashKey;
}

void IndexDocumentMaker::UpdateAnswer(const string &key, docid_t docId, 
                                      pos_t globalPos, pospayload_t posPayload,
                                      PostingAnswerMap* answerMap)
{
    if (answerMap)
    {
        PostingAnswerMap::iterator it = (*answerMap).find(key);
        if (it == (*answerMap).end())
        {
            static KeyAnswer defKeyAnswer;
            (*answerMap)[key] = defKeyAnswer;
        }
        (*answerMap)[key].totalTF++;

        map<docid_t, KeyAnswerInDoc>::iterator inDocIt = 
            (*answerMap)[key].inDocAnswers.find(docId);
        if (inDocIt == (*answerMap)[key].inDocAnswers.end())
        {
            static KeyAnswerInDoc defKeyAnswerInDoc;
            (*answerMap)[key].inDocAnswers[docId] = defKeyAnswerInDoc;

            (*answerMap)[key].docIdList.push_back(docId);            
            (*answerMap)[key].tfList.push_back(1);
            if (globalPos > numeric_limits<firstocc_t>::max())
            {
                (*answerMap)[key].inDocAnswers[docId].firstOcc = 
                        numeric_limits<firstocc_t>::max();
            }
            else
            {
                (*answerMap)[key].inDocAnswers[docId].firstOcc =
                    (firstocc_t)globalPos;
            }
        }
        else
        {
            (*answerMap)[key].tfList.back()++;
        }
            
        KeyAnswerInDoc& answerInDoc =
            (*answerMap)[key].inDocAnswers[docId];
        answerInDoc.positionList.push_back(globalPos);
        answerInDoc.posPayloadList.push_back(posPayload);
        // cout << "+++ answer: key:"  << key << " docid" 
        //      << docId << " globalPos" << globalPos 
        //      << " size:" <<  (*answerMap)[key].docIdList.size() << endl;
    }
}

void IndexDocumentMaker::CreateSections(uint32_t sectionNum,
                                        vector<Section*>& sections, 
                                        docid_t docId,
                                        Answer* answer,
                                        HashKeyToStrMap &hashKeyToStrMap,
                                        TokenType tokenType,
                                        pos_t* answerBasePosPtr,
                                        autil::mem_pool::Pool* pool)
{
    PostingAnswerMap *postingAnswerMap = NULL;
    if (answer != NULL)
    {
        postingAnswerMap = &(answer->postingAnswerMap);
    }

    bool firstToken = true;
    pos_t globalPos = 0; 
    if (answerBasePosPtr != NULL)
    {
        globalPos = *answerBasePosPtr;
    }
    for (uint32_t i = 1; i <= sectionNum; ++i)
    {
        Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, i + 1, pool);
        for (uint32_t j = 0; j < i; ++j)
        {
            stringstream ss;
            if (tokenType == tt_text)
            {
                ss << "token" << j;
            }
            else 
            {
                ss << j;
            }
            pospayload_t posPayload = (pospayload_t)j;
            pos_t posIncrement = 0;
            if (answerBasePosPtr)
            {
                if (sections.size())
                {
                    if (firstToken)
                    {
                        posIncrement = (pos_t)2;
                    }
                    else
                    {
                        posIncrement = (pos_t)1;
                    }
                }
                else
                {
                    posIncrement = (pos_t)0;
                }
            }
            else
            {
                posIncrement = firstToken ? (pos_t)0 : (pos_t)1;
            }
            dictkey_t hashKey = GetHashKey(ss.str(), tokenType);
            section->CreateToken(hashKey, posIncrement, posPayload);
            hashKeyToStrMap[hashKey] = ss.str();
            UpdateAnswer(ss.str(), docId, globalPos, posPayload, postingAnswerMap);
            globalPos++;

            firstToken = false;
        }

        // string hotToken = "hottoken";
        // pospayload_t posPayload = (pospayload_t)i;
        // pos_t posIncrement = firstToken ? (pos_t)0 : (pos_t)1;
        // Token* token = section->CreateToken(hotToken.c_str(), hotToken.size(),
        //         posIncrement, posPayload);

        // UpdateAnswer(token, docId, globalPos, posPayload, postingAnswerMap);
        // globalPos++;

        firstToken = answerBasePosPtr != NULL;

        section->SetLength(i + 1);
        section->SetWeight(i + 1);
        sections.push_back(section);

        if (answerBasePosPtr != NULL)
        {
            ++globalPos;
        }
    }

    if (answerBasePosPtr != NULL)
    {
        *answerBasePosPtr = globalPos;
    }
}

IndexTokenizeField* IndexDocumentMaker::CreateField(vector<Section*>& sectionVec,
                                       autil::mem_pool::Pool* pool)
{
    IndexTokenizeField* field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
    for (uint32_t i = 0; i < sectionVec.size(); ++i)
    {
        field->AddSection(sectionVec[i]);
    }

    return field;
}

IndexDocumentPtr IndexDocumentMaker::MakeIndexDocument(
        Pool* pool, const IndexDocument::FieldVector& fieldVec, 
        docid_t docId, Answer* answer,
        const HashKeyToStrMap &hashKeyToStrMap,
        TokenType tokenType,
        uint32_t* basePos)
{
    IndexDocumentPtr doc(new IndexDocument(pool));
    doc->SetDocId(docId);
    
    uint32_t position = basePos ? *basePos : 0;
    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
        IndexTokenizeField::Iterator fieldIter;
        auto field = dynamic_cast<IndexTokenizeField*>(fieldVec[i]);
        for (fieldIter = field->Begin(); fieldIter != field->End(); ++fieldIter)
        {
            const Section* section = (*fieldIter);
            for (size_t j = 0; j < section->GetTokenCount(); j++)
            {
                uint64_t hashKey = section->GetToken(j)->GetHashKey();

                docpayload_t docPayload = hashKey % 64 + docId % 32;
                termpayload_t termPayload = hashKey % 128;
                doc->SetDocPayload(hashKey, docPayload);
                doc->SetTermPayload(hashKey,termPayload);
                const string &key = hashKeyToStrMap.find(hashKey)->second;

                if (answer != NULL)
                {
                    (*answer).postingAnswerMap[key].termPayload = termPayload;
                    (*answer).postingAnswerMap[key].inDocAnswers[docId].docPayload = docPayload;
                }
                
            }

            if (answer != NULL)
            {
                common::SectionMeta meta;
                meta.fieldId = i;
                meta.length = section->GetLength();
                meta.weight = section->GetWeight();

                stringstream ss;
                sectionid_t sectionId = fieldIter - field->Begin();
                ss << docId << ";" << i << ";" << sectionId;
                string idString = ss.str();
                answer->sectionAnswerMap[idString] = SectionAnswer(position, meta);
                position += section->GetLength();
            }
        }

        if (fieldVec[i]->GetFieldId() == INVALID_FIELDID)
        {
            fieldVec[i]->SetFieldId(i);
        }
        doc->AddField(fieldVec[i]);
        if (basePos)
        {
            *basePos = position;
        }
    }
    return doc;
}

IndexDocumentPtr IndexDocumentMaker::MakeIndexDocument(
        Pool* pool, const Field* field, docid_t docId, 
        TokenType tokenType)
{
    IndexDocument::FieldVector fieldVec(1, const_cast<Field*>(field));
    return MakeIndexDocument(pool, fieldVec, docId, tokenType);
}

IndexDocumentPtr IndexDocumentMaker::MakeIndexDocument(
        Pool* pool, const IndexDocument::FieldVector& fieldVec, 
        docid_t docId, TokenType tokenType)
{
    IndexDocumentPtr doc(new IndexDocument(pool));
    doc->SetDocId(docId);
    map<uint64_t, docpayload_t> termDocPayloadMap;
    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
        IndexTokenizeField::Iterator fieldIter;
        auto field = dynamic_cast<IndexTokenizeField*>(fieldVec[i]);
        for (fieldIter = field->Begin(); fieldIter != field->End(); ++fieldIter)
        {
            const Section* section = (*fieldIter);
            for (size_t j = 0; j < section->GetTokenCount(); j++)
            {
                uint64_t hashKey = section->GetToken(j)->GetHashKey();

                doc->SetDocPayload(hashKey, hashKey % 100);
                doc->SetTermPayload(hashKey,66888866);
            }
        }
        if (fieldVec[i]->GetFieldId() == INVALID_FIELDID)
        {
            fieldVec[i]->SetFieldId(i);
        }
        doc->AddField(fieldVec[i]);
    }
    return doc;
}

void IndexDocumentMaker::MakeIndexDocuments(
        Pool* pool, vector<IndexDocumentPtr>& indexDocs,
        uint32_t docNum, docid_t beginDocId, Answer* answer, 
        TokenType tokenType)
{
    uint32_t* basePosPtr = NULL;
    pos_t *answerBasePosPtr = NULL;
    for (uint32_t i = 1; i <= docNum; ++i)
    {
        HashKeyToStrMap hashKeyToStrMap;
        vector<Section*> sectionVec;
        docid_t docId = beginDocId + i - 1;
        CreateSections(i, sectionVec, docId, answer, hashKeyToStrMap, tokenType,
                       answerBasePosPtr, pool);
        Field* field = CreateField(sectionVec, pool);
        field->SetFieldId(0);
        
        IndexDocument::FieldVector fieldVec(1, const_cast<Field*>(field));
        IndexDocumentPtr indexDoc = MakeIndexDocument(
                pool, fieldVec, docId, answer, hashKeyToStrMap, tokenType, basePosPtr);
        indexDoc->SetDocId(i - 1); // set local docid
        indexDocs.push_back(indexDoc);
    }
}


void IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(
    vector<document::IndexDocumentPtr>& indexDocs,
    const IndexPartitionSchemaPtr& schema)
{
    SectionAttributeRewriter sectionAttrRewriter;
    sectionAttrRewriter.Init(schema);
    NormalDocumentPtr document(new NormalDocument);
    document->ModifyDocOperateType(ADD_DOC);
    for (size_t i = 0; i < indexDocs.size(); ++i)
    {
	document->SetIndexDocument(indexDocs[i]);
	sectionAttrRewriter.Rewrite(document);
    }
}

void IndexDocumentMaker::AddDocsToWriter(vector<IndexDocumentPtr>& indexDocs,
        IndexWriter& writer)
{
    for (size_t idx = 0; idx < indexDocs.size(); ++idx)
    {
        IndexDocument::Iterator iter = indexDocs[idx]->CreateIterator();
        while (iter.HasNext())
        {
            writer.AddField(iter.Next());
        }
        writer.EndDocument(*(indexDocs[idx]));
    }
}

void IndexDocumentMaker::AddFieldsToWriter(IndexDocument::FieldVector& fieldVec,
        IndexWriter& writer, docid_t begDocId, bool empty)
{
    Pool pool;
    for (size_t idx = 0; idx < fieldVec.size(); ++idx)
    {
        IndexDocumentPtr doc = MakeIndexDocument(&pool, fieldVec[idx], idx + begDocId);
        if (!empty)
        {
            writer.AddField(fieldVec[idx]);
        }
        writer.EndDocument(*doc);
    }

    fieldVec.clear();
}

void IndexDocumentMaker::CleanDir(const string& dir)
{
    if (FileSystemWrapper::IsExist(dir))
    {
        FileSystemWrapper::DeleteDir(dir);
    }

    // section meta may not be exist, but rm -rf can ignore error...
    string sectionPath = dir + "_section";
    if (FileSystemWrapper::IsExist(sectionPath))
    {
        FileSystemWrapper::DeleteDir(sectionPath);    
    }
}

void IndexDocumentMaker::ResetDir(const string& dir)
{
    CleanDir(dir);
    FileSystemWrapper::MkDir(dir, true);
}

void IndexDocumentMaker::UpdateFieldMapInAnswer(
        const IndexDocument::FieldVector& fieldVec, 
        docid_t docId, PackageIndexConfigPtr& indexConfig, Answer* answer)
{
    PostingAnswerMap::iterator it = answer->postingAnswerMap.begin();
    while (it != answer->postingAnswerMap.end())
    {
        map<docid_t, KeyAnswerInDoc>::iterator inDocIt = 
            (*it).second.inDocAnswers.find(docId);
        if (inDocIt != (*it).second.inDocAnswers.end())
        {
            for (size_t i = 0; i < fieldVec.size(); i++)
            {
                int32_t fieldIdxInPack = indexConfig->GetFieldIdxInPack(i);
                if (fieldIdxInPack >= 0)
                {
                    (*inDocIt).second.fieldMap |= (1 << fieldIdxInPack);
                }
            }
        }
        it++;
    }
}

void IndexDocumentMaker::CreatePackageIndexConfig(
        PackageIndexConfigPtr& indexConf, IndexPartitionSchemaPtr& schema,
        uint32_t fieldNum, IndexType indexType, const string &indexName)
{
    schema.reset(new IndexPartitionSchema);
    for (uint32_t i = 0; i < fieldNum; i++)
    {
        stringstream ss;
        ss << "field" << i;
	schema->AddFieldConfig(ss.str(), ft_text, false, false);
    }
    indexConf.reset(new PackageIndexConfig(indexName, indexType));
    indexConf->SetFieldSchema(schema->GetFieldSchema());
    for (uint32_t i = 0; i < fieldNum; i++)
    {
        stringstream ss;
        ss << "field" << i;
        indexConf->AddFieldConfig(ss.str(), 10 - i + 1);
    }
    schema->AddIndexConfig(indexConf);
}

void IndexDocumentMaker::CreateSingleFieldIndexConfig(SingleFieldIndexConfigPtr &indexConfig, 
        const string &indexName, IndexType indexType, FieldType type, optionflag_t optionFlag)
{
    indexConfig.reset(new SingleFieldIndexConfig(indexName, indexType));
    FieldConfigPtr fieldConfig(new FieldConfig("field0", type, false));
    fieldConfig->SetFieldId(0);    
    indexConfig->SetOptionFlag(optionFlag);   
    indexConfig->SetFieldConfig(fieldConfig);
    
    if (indexType == it_number)
    {
        IndexType indexType = config::NumberIndexTypeTransformor::TransformFieldTypeToIndexType(type);
        indexConfig->SetIndexType(indexType);
    }
 }


IE_NAMESPACE_END(index);

