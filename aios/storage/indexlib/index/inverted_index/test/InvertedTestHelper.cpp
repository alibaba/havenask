#include "indexlib/index/inverted_index/test/InvertedTestHelper.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/RecyclePool.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/Section.h"
#include "indexlib/document/normal/rewriter/SectionAttributeRewriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"
#include "indexlib/index/inverted_index/IndexFormatWriterCreator.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/test/PostingMaker.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/SimplePool.h"

using namespace std;
using namespace indexlib::document;
using autil::mem_pool::Pool;

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedTestHelper);

void InvertedTestHelper::UpdateAnswer(const string& key, docid_t docId, pos_t globalPos, pospayload_t posPayload,
                                      PostingAnswerMap* answerMap)
{
    if (answerMap) {
        PostingAnswerMap::iterator it = (*answerMap).find(key);
        if (it == (*answerMap).end()) {
            static KeyAnswer defKeyAnswer;
            (*answerMap)[key] = defKeyAnswer;
        }
        (*answerMap)[key].totalTF++;

        map<docid_t, KeyAnswerInDoc>::iterator inDocIt = (*answerMap)[key].inDocAnswers.find(docId);
        if (inDocIt == (*answerMap)[key].inDocAnswers.end()) {
            static KeyAnswerInDoc defKeyAnswerInDoc;
            (*answerMap)[key].inDocAnswers[docId] = defKeyAnswerInDoc;

            (*answerMap)[key].docIdList.push_back(docId);
            (*answerMap)[key].tfList.push_back(1);
            if (globalPos > numeric_limits<firstocc_t>::max()) {
                (*answerMap)[key].inDocAnswers[docId].firstOcc = numeric_limits<firstocc_t>::max();
            } else {
                (*answerMap)[key].inDocAnswers[docId].firstOcc = (firstocc_t)globalPos;
            }
        } else {
            (*answerMap)[key].tfList.back()++;
        }

        KeyAnswerInDoc& answerInDoc = (*answerMap)[key].inDocAnswers[docId];
        answerInDoc.positionList.push_back(globalPos);
        answerInDoc.posPayloadList.push_back(posPayload);
        // cout << "+++ answer: key:"  << key << " docid"
        //      << docId << " globalPos" << globalPos
        //      << " size:" <<  (*answerMap)[key].docIdList.size() << endl;
    }
}

void InvertedTestHelper::CreateSections(uint32_t sectionNum, vector<Section*>& sections, docid_t docId, Answer* answer,
                                        HashKeyToStrMap& hashKeyToStrMap, TokenType tokenType, pos_t* answerBasePosPtr,
                                        Pool* pool)
{
    PostingAnswerMap* postingAnswerMap = NULL;
    if (answer != NULL) {
        postingAnswerMap = &(answer->postingAnswerMap);
    }

    bool firstToken = true;
    pos_t globalPos = 0;
    if (answerBasePosPtr != NULL) {
        globalPos = *answerBasePosPtr;
    }
    for (uint32_t i = 1; i <= sectionNum; ++i) {
        Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, i + 1, pool);
        for (uint32_t j = 0; j < i; ++j) {
            stringstream ss;
            if (tokenType == tt_text) {
                ss << "token" << j;
            } else {
                ss << j;
            }
            pospayload_t posPayload = (pospayload_t)j;
            pos_t posIncrement = 0;
            if (answerBasePosPtr) {
                if (sections.size()) {
                    if (firstToken) {
                        posIncrement = (pos_t)2;
                    } else {
                        posIncrement = (pos_t)1;
                    }
                } else {
                    posIncrement = (pos_t)0;
                }
            } else {
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

        if (answerBasePosPtr != NULL) {
            ++globalPos;
        }
    }

    if (answerBasePosPtr != NULL) {
        *answerBasePosPtr = globalPos;
    }
}

IndexTokenizeField* InvertedTestHelper::CreateField(vector<Section*>& sectionVec, Pool* pool)
{
    IndexTokenizeField* field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
    for (uint32_t i = 0; i < sectionVec.size(); ++i) {
        field->AddSection(sectionVec[i]);
    }

    return field;
}

shared_ptr<IndexDocument> InvertedTestHelper::MakeIndexDocument(Pool* pool, const IndexDocument::FieldVector& fieldVec,
                                                                docid_t docId, Answer* answer,
                                                                const HashKeyToStrMap& hashKeyToStrMap,
                                                                TokenType tokenType, uint32_t* basePos)
{
    shared_ptr<IndexDocument> doc(new IndexDocument(pool));
    doc->SetDocId(docId);

    uint32_t position = basePos ? *basePos : 0;
    for (uint32_t i = 0; i < fieldVec.size(); ++i) {
        IndexTokenizeField::Iterator fieldIter;
        auto field = dynamic_cast<IndexTokenizeField*>(fieldVec[i]);
        for (fieldIter = field->Begin(); fieldIter != field->End(); ++fieldIter) {
            const Section* section = (*fieldIter);
            for (size_t j = 0; j < section->GetTokenCount(); j++) {
                uint64_t hashKey = section->GetToken(j)->GetHashKey();

                docpayload_t docPayload = hashKey % 64 + docId % 32;
                termpayload_t termPayload = hashKey % 128;
                doc->SetDocPayload(hashKey, docPayload);
                doc->SetTermPayload(hashKey, termPayload);
                const string& key = hashKeyToStrMap.find(hashKey)->second;

                if (answer != NULL) {
                    (*answer).postingAnswerMap[key].termPayload = termPayload;
                    (*answer).postingAnswerMap[key].inDocAnswers[docId].docPayload = docPayload;
                }
            }

            if (answer != NULL) {
                SectionMeta meta;
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

        if (fieldVec[i]->GetFieldId() == INVALID_FIELDID) {
            fieldVec[i]->SetFieldId(i);
        }
        doc->AddField(fieldVec[i]);
        if (basePos) {
            *basePos = position;
        }
    }
    return doc;
}

shared_ptr<IndexDocument> InvertedTestHelper::MakeIndexDocument(Pool* pool, const Field* field, docid_t docId,
                                                                TokenType tokenType)
{
    IndexDocument::FieldVector fieldVec(1, const_cast<Field*>(field));
    return MakeIndexDocument(pool, fieldVec, docId, tokenType);
}

shared_ptr<IndexDocument> InvertedTestHelper::MakeIndexDocument(Pool* pool, const IndexDocument::FieldVector& fieldVec,
                                                                docid_t docId, TokenType tokenType)
{
    shared_ptr<IndexDocument> doc(new IndexDocument(pool));
    doc->SetDocId(docId);
    map<uint64_t, docpayload_t> termDocPayloadMap;
    for (uint32_t i = 0; i < fieldVec.size(); ++i) {
        IndexTokenizeField::Iterator fieldIter;
        auto field = dynamic_cast<IndexTokenizeField*>(fieldVec[i]);
        for (fieldIter = field->Begin(); fieldIter != field->End(); ++fieldIter) {
            const Section* section = (*fieldIter);
            for (size_t j = 0; j < section->GetTokenCount(); j++) {
                uint64_t hashKey = section->GetToken(j)->GetHashKey();

                doc->SetDocPayload(hashKey, hashKey % 100);
                doc->SetTermPayload(hashKey, 66888866);
            }
        }
        if (fieldVec[i]->GetFieldId() == INVALID_FIELDID) {
            fieldVec[i]->SetFieldId(i);
        }
        doc->AddField(fieldVec[i]);
    }
    return doc;
}

void InvertedTestHelper::MakeIndexDocuments(Pool* pool, vector<shared_ptr<IndexDocument>>& indexDocs, uint32_t docNum,
                                            docid_t beginDocId, Answer* answer, TokenType tokenType)
{
    uint32_t* basePosPtr = NULL;
    pos_t* answerBasePosPtr = NULL;
    for (uint32_t i = 1; i <= docNum; ++i) {
        HashKeyToStrMap hashKeyToStrMap;
        vector<Section*> sectionVec;
        docid_t docId = beginDocId + i - 1;
        CreateSections(i, sectionVec, docId, answer, hashKeyToStrMap, tokenType, answerBasePosPtr, pool);
        Field* field = CreateField(sectionVec, pool);
        field->SetFieldId(0);

        IndexDocument::FieldVector fieldVec(1, const_cast<Field*>(field));
        shared_ptr<IndexDocument> indexDoc =
            MakeIndexDocument(pool, fieldVec, docId, answer, hashKeyToStrMap, tokenType, basePosPtr);
        indexDoc->SetDocId(i - 1); // set local docid
        indexDocs.push_back(indexDoc);
    }
}

void InvertedTestHelper::RewriteSectionAttributeInIndexDocuments(
    vector<shared_ptr<IndexDocument>>& indexDocs, const shared_ptr<indexlibv2::config::ITabletSchema>& schema)
{
    indexlibv2::document::SectionAttributeRewriter sectionAttrRewriter;
    if (!sectionAttrRewriter.Init(schema)) {
        return;
    }
    auto document = make_shared<indexlibv2::document::NormalDocument>();
    document->ModifyDocOperateType(ADD_DOC);
    for (size_t i = 0; i < indexDocs.size(); ++i) {
        document->SetIndexDocument(indexDocs[i]);
        auto status = sectionAttrRewriter.RewriteOneDoc(document);
        assert(status.IsOK());
    }
}

void InvertedTestHelper::UpdateFieldMapInAnswer(const IndexDocument::FieldVector& fieldVec, docid_t docId,
                                                std::shared_ptr<indexlibv2::config::PackageIndexConfig>& indexConfig,
                                                Answer* answer)
{
    PostingAnswerMap::iterator it = answer->postingAnswerMap.begin();
    while (it != answer->postingAnswerMap.end()) {
        map<docid_t, KeyAnswerInDoc>::iterator inDocIt = (*it).second.inDocAnswers.find(docId);
        if (inDocIt != (*it).second.inDocAnswers.end()) {
            for (size_t i = 0; i < fieldVec.size(); i++) {
                int32_t fieldIdxInPack = indexConfig->GetFieldIdxInPack(i);
                if (fieldIdxInPack >= 0) {
                    (*inDocIt).second.fieldMap |= (1 << fieldIdxInPack);
                }
            }
        }
        it++;
    }
}

uint64_t InvertedTestHelper::GetHashKey(const string& key, TokenType tokenType)
{
    uint64_t hashKey = 0;
    if (tokenType == tt_text) {
        hashKey = autil::HashAlgorithm::hashString64(key.c_str());
    } else {
        autil::StringUtil::strToUInt64(key.c_str(), hashKey);
    }
    return hashKey;
}

void InvertedTestHelper::BuildMultiSegmentsFromDataStrings(const vector<string>& dataStrs, const string& rootDir,
                                                           const string& indexName, const vector<docid_t>& baseDocIds,
                                                           vector<PosAnswerMap>& answerMaps,
                                                           vector<uint8_t>& compressModes,
                                                           const IndexFormatOption& indexFormatOption)
{
    for (size_t i = 0; i < dataStrs.size(); i++) {
        stringstream ss;
        ss << rootDir << SEGMENT_FILE_NAME_PREFIX << "_" << i << "_level_0/";
        string indexDir = ss.str() + INDEX_DIR_NAME + "/" + indexName + "/";
        fslib::fs::FileSystem::mkDir(indexDir, true);
        string dumpFilePath = indexDir + POSTING_FILE_NAME;

        PosAnswerMap tempMap;
        uint8_t compressMode =
            BuildOneSegmentFromDataString(dataStrs[i], dumpFilePath, baseDocIds[i], tempMap, indexFormatOption);
        compressModes.push_back(compressMode);
        answerMaps.push_back(tempMap);
    }
}

uint8_t InvertedTestHelper::BuildOneSegmentFromDataString(const std::string& dataStr, const std::string& filePath,
                                                          docid_t baseDocId, PosAnswerMap& answerMap,
                                                          const IndexFormatOption& indexFormatOption)
{
    Pool byteSlicePool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);
    autil::mem_pool::RecyclePool bufferPool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);
    util::SimplePool simplePool;

    PostingFormatOption formatOption = indexFormatOption.GetPostingFormatOption();

    PostingWriterResource writerResource(&simplePool, &byteSlicePool, &bufferPool, formatOption);
    PostingWriterImpl writer(&writerResource);
    answerMap = PostingMaker::MakeDocMap(dataStr);
    PostingMaker::BuildPostingData(writer, answerMap, baseDocId);

    shared_ptr<indexlib::file_system::BufferedFileWriter> file(new indexlib::file_system::BufferedFileWriter);
    THROW_IF_FS_ERROR(file->Open(filePath, filePath), "BufferedFileWriter open failed, path [%s]", filePath.c_str());
    writer.EndSegment();

    TermMetaDumper tmDumper(formatOption);
    TermMeta termMeta(writer.GetDF(), writer.GetTotalTF(), 0);
    tmDumper.Dump(file, termMeta);
    writer.Dump(file);
    file->Close().GetOrThrow();
    return ShortListOptimizeUtil::GetCompressMode(writer.GetDF(), writer.GetTotalTF(),
                                                  formatOption.GetDocListCompressMode());
}

void InvertedTestHelper::PrepareSegmentFiles(const indexlib::file_system::DirectoryPtr& segmentDir, string indexName,
                                             std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig,
                                             uint32_t docCount)
{
    auto singleIndexDir = segmentDir->MakeDirectory(util::PathUtil::JoinPath(INDEX_DIR_NAME, indexName));

    PrepareDictionaryFile(singleIndexDir, DICTIONARY_FILE_NAME, indexConfig);
    singleIndexDir->Store(POSTING_FILE_NAME, "", file_system::WriterOption::AtomicDump());

    indexlibv2::framework::SegmentInfo segmentInfo;
    segmentInfo.docCount = docCount;
    auto status = segmentInfo.Store(segmentDir->GetIDirectory());
    assert(status.IsOK());
}

void InvertedTestHelper::PrepareDictionaryFile(
    const indexlib::file_system::DirectoryPtr& directory, const string& dictFileName,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    util::SimplePool pool;
    std::shared_ptr<DictionaryWriter> dictionaryWriter(
        IndexFormatWriterCreator::CreateDictionaryWriter(indexConfig, nullptr, &pool));
    dictionaryWriter->Open(directory, dictFileName);
    dictionaryWriter->AddItem(index::DictKeyInfo(1), 1);
    dictionaryWriter->Close();
}

document::Field* InvertedTestHelper::MakeField(const shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                               const string& str, autil::mem_pool::Pool* pool)
{
    HashKeyToStrMap hashKeyToStrMap;
    return MakeField(schema, str, hashKeyToStrMap, pool);
}

/**
 * <field>: [fieldname: <sections>]
 * <sections>: (<section>) (<section>) ...
 * <section>: (token^pospayload token^pospayload ...)
 */
Field* InvertedTestHelper::MakeField(const shared_ptr<indexlibv2::config::ITabletSchema>& schema, const string& str,
                                     HashKeyToStrMap& hashKeyToStrMap, autil::mem_pool::Pool* pool)
{
    string fieldStr = str.substr(1, str.length() - 2);
    autil::StringTokenizer st1(fieldStr, ":",
                               autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    string fieldName = st1[0];
    const auto& fieldConfig = schema->GetFieldConfig(fieldName);
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

} // namespace indexlib::index
