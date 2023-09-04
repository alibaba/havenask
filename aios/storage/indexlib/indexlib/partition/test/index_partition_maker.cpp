#include "indexlib/partition/test/index_partition_maker.h"

#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/partition_writer.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexPartitionMaker);

IndexPartitionMaker::IndexPartitionMaker() {}

IndexPartitionMaker::~IndexPartitionMaker() {}

void IndexPartitionMaker::MakeMultiSegmentData(vector<uint32_t>& docCounts, IndexPartition& indexPart4Write,
                                               MockIndexPart& mockIndexPart, int32_t pkStartSuffix,
                                               DeletionMode delMode, bool isUpdate)
{
    PartitionWriterPtr writer = indexPart4Write.GetWriter();
    config::IndexPartitionSchemaPtr schema = indexPart4Write.GetSchema();
    DocumentMaker::DocumentVect oldDocVect;
    for (uint32_t i = 0; i < docCounts.size(); i++) {
        DocumentMaker::DocumentVect docVect = MakeOneSegmentDataWithoutDump(
            docCounts[i], indexPart4Write, mockIndexPart, pkStartSuffix, delMode, oldDocVect);
        writer->DumpSegment();
        indexPart4Write.ReOpenNewSegment();

        if (i == docCounts.size() - 1) {
            break;
        }

        pkStartSuffix += docCounts[i];

        oldDocVect.insert(oldDocVect.end(), docVect.begin(), docVect.end());
        if (isUpdate && oldDocVect.size() > 0) {
            DocumentMaker::UpdateDocuments(schema, oldDocVect, mockIndexPart);
        }
    }
    writer->Close();
}

void IndexPartitionMaker::MakeOneSegmentData(uint32_t docCount, IndexPartition& indexPart4Write,
                                             MockIndexPart& mockIndexPart, int32_t pkStartSuffix, DeletionMode delMode,
                                             DocumentVect docsToUpdate)
{
    MakeOneSegmentDataWithoutDump(docCount, indexPart4Write, mockIndexPart, pkStartSuffix, delMode, docsToUpdate);
    // auto fs = indexPart4Write.GetFileSystem();
    // auto ret = fs->Sync(true);
    PartitionWriterPtr writer = indexPart4Write.GetWriter();
    writer->Close();
}

void IndexPartitionMaker::MakeDocuments(uint32_t docCount, const IndexPartitionSchemaPtr& schema, int32_t pkStartSuffix,
                                        MockIndexPart& mockIndexPart, DocumentMaker::DocumentVect& segDocVect)
{
    string docStr;
    string pkFieldName = schema->GetIndexSchema()->GetPrimaryKeyIndexFieldName();
    DocumentMaker::MakeDocumentsStr(docCount, schema->GetFieldSchema(), docStr, pkFieldName, pkStartSuffix);

    DocumentMaker::MakeDocuments(docStr, schema, segDocVect, mockIndexPart);
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        assert(mockIndexPart.subIndexPart);
        DocumentMaker::DocumentVect subSegDocVect;
        MakeDocuments(docCount, subSchema, pkStartSuffix, *mockIndexPart.subIndexPart, subSegDocVect);
        for (size_t i = 0; i < segDocVect.size(); ++i) {
            // TODO: 1 to more
            segDocVect[i]->AddSubDocument(subSegDocVect[i]);
        }
    }

    SectionAttributeRewriter rewriter;
    if (rewriter.Init(schema)) {
        for (size_t i = 0; i < segDocVect.size(); i++) {
            rewriter.Rewrite(segDocVect[i]);
        }
    }
}

DocumentMaker::DocumentVect
IndexPartitionMaker::MakeOneSegmentDataWithoutDump(uint32_t docCount, IndexPartition& indexPart4Write,
                                                   MockIndexPart& mockIndexPart, int32_t pkStartSuffix,
                                                   DeletionMode delMode, DocumentVect docsToUpdate)
{
    PartitionWriterPtr partWriter = indexPart4Write.GetWriter();
    OnlinePartitionWriterPtr onlineWriter = DYNAMIC_POINTER_CAST(OnlinePartitionWriter, partWriter);
    OfflinePartitionWriterPtr writer;
    if (onlineWriter) {
        writer = onlineWriter->mWriter;
    } else {
        writer = DYNAMIC_POINTER_CAST(OfflinePartitionWriter, partWriter);
    }
    IndexPartitionSchemaPtr schema = indexPart4Write.GetSchema();

    DocumentMaker::DocumentVect segDocVect;
    MakeDocuments(docCount, schema, pkStartSuffix, mockIndexPart, segDocVect);

    for (size_t i = 0; i < segDocVect.size(); ++i) {
        writer->BuildDocument(segDocVect[i]);
    }

    if (docsToUpdate.size() > 0) {
        for (size_t i = 0; i < docsToUpdate.size(); ++i) {
            writer->BuildDocument(docsToUpdate[i]);
        }

        DocumentMaker::UpdateDocuments(schema, segDocVect, mockIndexPart);
        for (size_t i = 0; i < segDocVect.size(); ++i) {
            writer->BuildDocument(segDocVect[i]);
        }
    }

    for (docid_t i = 0; i < (docid_t)segDocVect.size(); ++i) {
        if (IndexTestUtil::deleteFuncs[delMode](i)) {
            docid_t docId = segDocVect[i]->GetDocId();
            DocumentMaker::DeleteDocument(mockIndexPart, docId);

            segDocVect[i]->ModifyDocOperateType(DELETE_DOC);
            writer->BuildDocument(segDocVect[i]);
            segDocVect[i]->ModifyDocOperateType(ADD_DOC);
        }
    }

    return segDocVect;
}

void IndexPartitionMaker::InnerMakeMultiSegmentsSortedData(vector<uint32_t>& docCounts,
                                                           IndexPartitionPtr& indexPart4Write,
                                                           MockIndexPart& mockIndexPart, DocumentVect& docVect,
                                                           int32_t pkStartSuffix, const std::string& sortFieldName,
                                                           DeletionMode delMode, bool isUpdate)
{
    // add document and dump.
    PartitionWriterPtr partWriter = indexPart4Write->GetWriter();
    OnlinePartitionWriterPtr onlineWriter = DYNAMIC_POINTER_CAST(OnlinePartitionWriter, partWriter);
    OfflinePartitionWriterPtr writer;
    if (onlineWriter) {
        writer = onlineWriter->mWriter;
    } else {
        writer = DYNAMIC_POINTER_CAST(OfflinePartitionWriter, partWriter);
    }

    IndexPartitionSchemaPtr schema = indexPart4Write->GetSchema();
    AttributeSchemaPtr attributeSchema = schema->GetAttributeSchema();
    AttributeConfigPtr sortAttributeConfig = attributeSchema->GetAttributeConfig(sortFieldName);

    int32_t pkSuffix = pkStartSuffix;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        string docStr;
        DocumentMaker::MakeSortedDocumentsStr(docCounts[i], schema->GetFieldSchema(), docStr, sortFieldName, "string1",
                                              pkSuffix);
        pkSuffix += docCounts[i];

        DocumentMaker::DocumentVect segDocVect;
        DocumentMaker::MakeDocuments(docStr, schema, segDocVect, mockIndexPart);
        docVect.insert(docVect.end(), segDocVect.begin(), segDocVect.end());
    }

    uint32_t curDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        for (size_t j = 0; j < docCounts[i]; ++j) {
            writer->BuildDocument(docVect[curDocId++]);
        }

        for (docid_t j = 0; j < (docid_t)docCounts[i]; ++j) {
            if (IndexTestUtil::deleteFuncs[delMode](j)) {
                docid_t docId = docVect[curDocId - docCounts[i] + j]->GetDocId();
                DocumentMaker::DeleteDocument(mockIndexPart, docId);
                OfflinePartitionWriterPtr offlineWriter = DYNAMIC_POINTER_CAST(OfflinePartitionWriter, writer);
                PartitionModifierPtr modifier = offlineWriter->GetModifier();
                modifier->RemoveDocument(docId);
            }
        }

        if (isUpdate) {
            DocumentMaker::UpdateDocuments(schema, docVect, mockIndexPart);
            for (size_t j = 0; j < docCounts[i]; ++j) {
                bool ret = writer->BuildDocument(docVect[curDocId - docCounts[i] + j]);
                (void)ret; // avoid warning in release mode
                assert(ret);
            }
        }
        writer->DumpSegment();
        indexPart4Write->ReOpenNewSegment();
    }

    MergeSortDocuments(docCounts, sortAttributeConfig, docVect);
    writer->Close();
}

void IndexPartitionMaker::MakeOneDelSegmentData(uint32_t docCount, IndexPartition& indexPart4Write,
                                                MockIndexPart& mockIndexPart, int32_t pkStartSuffix,
                                                DeletionMode delMode)
{
    assert(delMode != IndexTestUtil::DM_NODELETE);
    PartitionWriterPtr writer = indexPart4Write.GetWriter();

    uint32_t pkSuffix = pkStartSuffix;
    string docStr;
    IndexPartitionSchemaPtr schema = indexPart4Write.GetSchema();
    DocumentMaker::MakeDocumentsStr(docCount, schema->GetFieldSchema(), docStr, "string1", pkSuffix);

    DocumentMaker::DocumentVect segDocVect;
    DocumentMaker::MakeDocuments(docStr, schema, segDocVect, mockIndexPart);
    for (docid_t i = 0; i < (docid_t)segDocVect.size(); ++i) {
        if (IndexTestUtil::deleteFuncs[delMode](i)) {
            docid_t docId = segDocVect[i]->GetDocId();
            DocumentMaker::DeleteDocument(mockIndexPart, docId);
            segDocVect[i]->SetDocOperateType(DELETE_DOC);
            writer->BuildDocument(segDocVect[i]);
        }
    }

    writer->Close();
}

void IndexPartitionMaker::MakeMultiSegmentsSortedData(vector<uint32_t>& docCounts, IndexPartitionPtr& indexPart4Write,
                                                      MockIndexPart& mockIndexPart, int32_t pkStartSuffix,
                                                      const string& sortFieldName, DeletionMode delMode, bool isUpdate)
{
    DocumentVect docVect;
    MockIndexPart tmpIndexPart;
    InnerMakeMultiSegmentsSortedData(docCounts, indexPart4Write, tmpIndexPart, docVect, pkStartSuffix, sortFieldName,
                                     delMode, isUpdate);

    ReclaimMapPtr reclaimMap = MakeSortedReclaimMap(tmpIndexPart, docVect);
    DocumentMaker::ReclaimDocuments(tmpIndexPart, reclaimMap, mockIndexPart);
}

ReclaimMapPtr IndexPartitionMaker::MakeSortedReclaimMap(const MockIndexPart& oldIndexPart,
                                                        const DocumentVect& sortedDocVec)
{
    vector<docid_t> docIdArray(sortedDocVec.size());

    docid_t newDocId = 0;
    docid_t oldDocId;
    for (size_t i = 0; i < sortedDocVec.size(); ++i) {
        oldDocId = sortedDocVec[i]->GetDocId();
        if (oldIndexPart.deletionMap.find(oldDocId) == oldIndexPart.deletionMap.end()) {
            docIdArray[oldDocId] = newDocId++;
        } else {
            docIdArray[oldDocId] = INVALID_DOCID;
        }
    }

    ReclaimMapPtr reclaimMap(new ReclaimMap);
    reclaimMap->SetSliceArray(docIdArray);
    return reclaimMap;
}

template <typename T>
int DescCompare(const DocumentPtr& left, const DocumentPtr& right, fieldid_t fid)
{
    NormalDocumentPtr leftDoc = DYNAMIC_POINTER_CAST(NormalDocument, left);
    NormalDocumentPtr rightDoc = DYNAMIC_POINTER_CAST(NormalDocument, right);
    const StringView& leftField = leftDoc->GetAttributeDocument()->GetField(fid);
    T leftValue = *(T*)leftField.data();
    const StringView& rightField = rightDoc->GetAttributeDocument()->GetField(fid);
    T rightValue = *(T*)rightField.data();

    if (leftValue > rightValue)
        return 1;
    if (leftValue < rightValue)
        return -1;
    return 0;
}

template <typename T>
int AscCompare(const DocumentPtr& left, const DocumentPtr& right, fieldid_t fid)
{
    NormalDocumentPtr leftDoc = DYNAMIC_POINTER_CAST(NormalDocument, left);
    NormalDocumentPtr rightDoc = DYNAMIC_POINTER_CAST(NormalDocument, right);

    const StringView& leftField = leftDoc->GetAttributeDocument()->GetField(fid);
    T leftValue = *(T*)leftField.data();
    const StringView& rightField = rightDoc->GetAttributeDocument()->GetField(fid);
    T rightValue = *(T*)rightField.data();

    if (leftValue < rightValue)
        return 1;
    if (leftValue > rightValue)
        return -1;
    return 0;
}
typedef int (*CompareFun)(const DocumentPtr& left, const DocumentPtr& right, fieldid_t fid);
typedef std::vector<CompareFun> CompareFunVec;

class DocumentMultiComp
{
    typedef DocumentMaker::DocumentVect DocumentVect;

public:
    DocumentMultiComp(DocumentVect& docVect, std::vector<fieldid_t> fieldIds, CompareFunVec compareFunVec,
                      vector<uint32_t>& docCounts)
        : mFieldIds(fieldIds)
        , mCompareFunVec(compareFunVec)
    {
    }

public:
    bool operator()(const DocumentPtr& left, const DocumentPtr& right)
    {
        NormalDocumentPtr leftDoc = DYNAMIC_POINTER_CAST(NormalDocument, left);
        NormalDocumentPtr rightDoc = DYNAMIC_POINTER_CAST(NormalDocument, right);

        CompareFun compareFunc = NULL;
        for (size_t i = 0; i < mFieldIds.size(); ++i) {
            compareFunc = mCompareFunVec[i];
            int iRet = compareFunc(left, right, mFieldIds[i]);
            if (iRet > 0)
                return true;
            if (iRet < 0)
                return false;
        }
        docid_t leftDocId = leftDoc->GetDocId();
        docid_t rightDocId = rightDoc->GetDocId();
        return leftDocId < rightDocId;
    }

private:
    std::vector<fieldid_t> mFieldIds;
    CompareFunVec mCompareFunVec;
};
void IndexPartitionMaker::SortDocuments(vector<uint32_t>& docCounts, const AttributeConfigPtr& attrConfig,
                                        DocumentVect& docVect)
{
    FieldType fieldType = attrConfig->GetFieldType();
    fieldid_t fieldId = attrConfig->GetFieldConfig()->GetFieldId();
    switch (fieldType) {
    case ft_integer:
        DoSortDocuments<int>(docCounts, fieldId, docVect);
        break;
    case ft_float:
        DoSortDocuments<float>(docCounts, fieldId, docVect);
        break;
    case ft_long:
        DoSortDocuments<long>(docCounts, fieldId, docVect);
        break;
    case ft_uint32:
        DoSortDocuments<uint32_t>(docCounts, fieldId, docVect);
        break;
    default:
        assert(0);
    }
}

void IndexPartitionMaker::MultiSortDocuments(std::vector<uint32_t>& docCounts,
                                             std::vector<config::AttributeConfigPtr>& attrConfigs,
                                             std::vector<std::string>& sortPatterns, DocumentVect& docVect)
{
    assert(attrConfigs.size() == sortPatterns.size());
    std::vector<FieldType> typeVec;
    std::vector<fieldid_t> idVec;
    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        typeVec.push_back(attrConfigs[i]->GetFieldType());
        idVec.push_back(attrConfigs[i]->GetFieldConfig()->GetFieldId());
    }
    DoMultiSortDocuments(docCounts, typeVec, idVec, sortPatterns, docVect);
}

void IndexPartitionMaker::DoMultiSortDocuments(std::vector<uint32_t>& docCounts, std::vector<FieldType>& typeVec,
                                               std::vector<fieldid_t>& idVec, std::vector<std::string>& sortPatterns,
                                               DocumentVect& docVect)
{
    CompareFunVec compareFuncs;
    for (size_t i = 0; i < typeVec.size(); ++i) {
        FieldType fieldType = typeVec[i];
        // fieldid_t fieldId = idVec[i];
        switch (fieldType) {
        case ft_integer:
            if (sortPatterns[i] == "ASC") {
                compareFuncs.push_back(AscCompare<int>);
            } else {
                compareFuncs.push_back(DescCompare<int>);
            }
            break;
        case ft_float:
            if (sortPatterns[i] == "ASC") {
                compareFuncs.push_back(AscCompare<float>);
            } else {
                compareFuncs.push_back(DescCompare<float>);
            }
            break;
        case ft_long:
            if (sortPatterns[i] == "ASC") {
                compareFuncs.push_back(AscCompare<long>);
            } else {
                compareFuncs.push_back(DescCompare<long>);
            }
            break;
        case ft_uint32:
            if (sortPatterns[i] == "ASC") {
                compareFuncs.push_back(AscCompare<uint32_t>);
            } else {
                compareFuncs.push_back(DescCompare<uint32_t>);
            }
            break;
        default:
            assert(0);
        }
    }
    DocumentMultiComp documentMultiComp(docVect, idVec, compareFuncs, docCounts);
    sort(docVect.begin(), docVect.end(), documentMultiComp);
}

template <typename T>
class DocumentComp
{
    typedef DocumentMaker::DocumentVect DocumentVect;

public:
    DocumentComp(DocumentVect& docVect, fieldid_t fieldId, vector<uint32_t>& docCounts)
        : mFieldId(fieldId)
        , mDocCounts(docCounts)
    {
    }

public:
    bool operator()(const DocumentPtr& left, const DocumentPtr& right)
    {
        NormalDocumentPtr leftDoc = DYNAMIC_POINTER_CAST(NormalDocument, left);
        NormalDocumentPtr rightDoc = DYNAMIC_POINTER_CAST(NormalDocument, right);

        const StringView& leftField = leftDoc->GetAttributeDocument()->GetField(mFieldId);
        T leftValue = *(T*)leftField.data();

        const StringView& rightField = rightDoc->GetAttributeDocument()->GetField(mFieldId);
        T rightValue = *(T*)rightField.data();

        if (leftValue > rightValue)
            return true;
        if (leftValue < rightValue)
            return false;

        docid_t baseDocId = 0;
        docid_t leftDocId = leftDoc->GetDocId();
        docid_t rightDocId = rightDoc->GetDocId();
        for (size_t i = 0; i < mDocCounts.size(); ++i) {
            if (leftDocId >= baseDocId + (docid_t)mDocCounts[i] && rightDocId >= baseDocId + (docid_t)mDocCounts[i]) {
                baseDocId += mDocCounts[i];
                continue;
            }
            if (leftDocId < baseDocId + (docid_t)mDocCounts[i] && rightDocId < baseDocId + (docid_t)mDocCounts[i]) {
                return leftDocId < rightDocId; // in the same segment
            }

            return leftDocId > rightDocId; // not in the same segment
        }
        return false;
    }

private:
    fieldid_t mFieldId;
    vector<uint32_t>& mDocCounts;
};

template <typename T>
void IndexPartitionMaker::DoSortDocuments(vector<uint32_t>& srcDocCounts, fieldid_t fieldId, DocumentVect& docVect)
{
    DocumentComp<T> comp(docVect, fieldId, srcDocCounts);
    sort(docVect.begin(), docVect.end(), comp);
}

template <typename T>
void IndexPartitionMaker::DoMergeSortDocuments(vector<uint32_t>& srcDocCounts, fieldid_t fieldId, DocumentVect& docVect)
{
    DocumentComp<T> comp(docVect, fieldId, srcDocCounts);
    uint32_t mergedDocCount = srcDocCounts[0];
    for (size_t i = 1; i < srcDocCounts.size(); ++i) {
        DocumentVect outputVect;
        outputVect.resize(mergedDocCount + srcDocCounts[i]);
        merge(docVect.begin(), docVect.begin() + mergedDocCount, docVect.begin() + mergedDocCount,
              docVect.begin() + mergedDocCount + srcDocCounts[i], outputVect.begin(), comp);
        copy(outputVect.begin(), outputVect.end(), docVect.begin());

        mergedDocCount += srcDocCounts[i];
    }
}

void IndexPartitionMaker::MergeSortDocuments(vector<uint32_t>& srcDocCounts, const AttributeConfigPtr& attrConfig,
                                             DocumentVect& docVect)
{
    FieldType fieldType = attrConfig->GetFieldType();
    fieldid_t fieldId = attrConfig->GetFieldConfig()->GetFieldId();
    switch (fieldType) {
    case ft_integer:
        DoMergeSortDocuments<int>(srcDocCounts, fieldId, docVect);
        break;
    case ft_float:
        DoMergeSortDocuments<float>(srcDocCounts, fieldId, docVect);
        break;
    case ft_long:
        DoMergeSortDocuments<long>(srcDocCounts, fieldId, docVect);
        break;
    case ft_uint32:
        DoMergeSortDocuments<uint32_t>(srcDocCounts, fieldId, docVect);
        break;
    default:
        assert(0);
    }
}

bool IndexPartitionMaker::IsVersionFileExsit(const std::string& rootDir, versionid_t versionId)
{
    stringstream ss;
    ss << rootDir << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return FslibWrapper::IsExist(ss.str()).GetOrThrow();
}
}} // namespace indexlib::partition
