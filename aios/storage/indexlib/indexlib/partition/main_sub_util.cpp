/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/partition/main_sub_util.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_writer.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/inplace_index_modifier.h"
#include "indexlib/index/normal/source/source_writer.h"
#include "indexlib/index/normal/summary/summary_writer.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib { namespace partition {

partition::SingleSegmentWriterPtr MainSubUtil::GetSegmentWriter(const index_base::SegmentWriterPtr& segmentWriter,
                                                                bool hasSub, bool isSub)
{
    assert(segmentWriter);
    if (hasSub) {
        partition::SubDocSegmentWriterPtr mainSubSegmentWriter =
            DYNAMIC_POINTER_CAST(partition::SubDocSegmentWriter, segmentWriter);
        assert(mainSubSegmentWriter);
        if (isSub) {
            return mainSubSegmentWriter->GetSubWriter();
        }
        return mainSubSegmentWriter->GetMainWriter();
    }
    partition::SingleSegmentWriterPtr singleSegmentWriter =
        DYNAMIC_POINTER_CAST(partition::SingleSegmentWriter, segmentWriter);
    return singleSegmentWriter;
}

partition::InplaceModifierPtr MainSubUtil::GetInplaceModifier(const partition::PartitionModifierPtr& modifier,
                                                              bool hasSub, bool isSub)
{
    assert(modifier);
    partition::PartitionModifierPtr partitionModifier = modifier;
    if (hasSub) {
        partition::SubDocModifierPtr mainSubModifier = DYNAMIC_POINTER_CAST(partition::SubDocModifier, modifier);
        assert(mainSubModifier);
        partitionModifier = isSub ? mainSubModifier->GetSubModifier() : mainSubModifier->GetMainModifier();
    }
    partition::InplaceModifierPtr inplaceModifier = DYNAMIC_POINTER_CAST(partition::InplaceModifier, partitionModifier);
    assert(inplaceModifier);
    return inplaceModifier;
}

index::AttributeReaderPtr MainSubUtil::GetAttributeReader(const partition::PartitionModifierPtr& modifier,
                                                          const config::AttributeConfigPtr& attrConfig, bool hasSub,
                                                          bool isSub)
{
    assert(modifier);
    partition::InplaceModifierPtr inplaceModifier = GetInplaceModifier(modifier, hasSub, isSub);
    index::InplaceAttributeModifierPtr inplaceAttributeModifier = inplaceModifier->GetAttributeModifier();
    if (inplaceAttributeModifier == nullptr) {
        return nullptr;
    }
    return inplaceAttributeModifier->GetAttributeReader(attrConfig);
}

index::PackAttributeReaderPtr MainSubUtil::GetPackAttributeReader(const partition::PartitionModifierPtr& modifier,
                                                                  const config::PackAttributeConfigPtr& packAttrConfig,
                                                                  bool hasSub, bool isSub)
{
    partition::InplaceModifierPtr inplaceModifier = GetInplaceModifier(modifier, hasSub, isSub);
    index::InplaceAttributeModifierPtr inplaceAttributeModifier = inplaceModifier->GetAttributeModifier();
    if (inplaceAttributeModifier == nullptr) {
        return nullptr;
    }
    return inplaceAttributeModifier->GetPackAttributeReader(packAttrConfig->GetPackAttrId());
}

index::SummaryWriterPtr MainSubUtil::GetSummaryWriter(const index_base::SegmentWriterPtr& segmentWriter, bool hasSub,
                                                      bool isSub)
{
    return GetSegmentWriter(segmentWriter, hasSub, isSub)->GetSummaryWriter();
}

index::SourceWriterPtr MainSubUtil::GetSourceWriter(const index_base::SegmentWriterPtr& segmentWriter, bool hasSub,
                                                    bool isSub)
{
    return GetSegmentWriter(segmentWriter, hasSub, isSub)->GetSourceWriter();
}

docid_t MainSubUtil::GetBuildingSegmentBaseDocId(const partition::PartitionModifierPtr& modifier, bool hasSub,
                                                 bool isSub)
{
    partition::InplaceModifierPtr inplaceModifier = GetInplaceModifier(modifier, hasSub, isSub);
    if (inplaceModifier) {
        return inplaceModifier->GetBuildingSegmentBaseDocId();
    }
    return INVALID_DOCID;
}

// UPDATE_FIELD doc will not change how filter should be done, no PK will be added or deleted.
// If the last doc of PK1 is DELETE_DOC, all docs of PK1 will be removed, and the last DELETE_DOC will take effect.
// If the last doc of PK1 is ADD_DOC, all docs of PK1 will be removed, and the last ADD_DOC will take effect. Also,
// if PK1 is in index, the old doc of PK1 in index will also be deleted.

// For main sub doc case,
// We support add new main doc--handled the same as above.
// Update main doc only: handled normally.
// delete main doc only--handled normally.

// If update sub doc,
// Update sub doc only: Need special handling. Can filter some useless updates if sub doc is removed.(TODO(panghai.hj))
// Update main and sub doc: Need special handling. Can filter some useless updates if sub doc is
// removed.(TODO(panghai.hj)) delete sub doc only: Need special handling. Can filter some useless updates if sub doc is
// removed.(TODO(panghai.hj)) special handling:

// add sub doc only: not supported
// There is no such as that delete main doc and some of the sub docs.
std::vector<document::DocumentPtr> MainSubUtil::FilterDocsForBatch(const std::vector<document::DocumentPtr>& documents,
                                                                   bool hasSub)
{
    if (documents.empty()) {
        return documents;
    }
    std::map<std::string, size_t> lastDeletedPKToDocIndex;
    std::map<std::string, size_t> lastAddedPKToDocIndex;
    // populate lastAddedPKToDocIndex and lastDeletedPKToDocIndxe.
    for (int i = documents.size() - 1; i >= 0; --i) {
        const document::DocumentPtr& document = documents[i];
        DocOperateType opType = document->GetDocOperateType();
        const std::string& pk = document->GetPrimaryKey();
        if (opType == UPDATE_FIELD || opType == DELETE_SUB_DOC) {
            continue;
        }
        assert(opType == ADD_DOC || opType == DELETE_DOC);
        if (lastAddedPKToDocIndex.find(pk) != lastAddedPKToDocIndex.end() or
            lastDeletedPKToDocIndex.find(pk) != lastDeletedPKToDocIndex.end()) {
            continue;
        }
        if (opType == DELETE_DOC) {
            lastDeletedPKToDocIndex[pk] = i;
            continue;
        }
        if (opType == ADD_DOC) {
            lastAddedPKToDocIndex[pk] = i;
            continue;
        }
    }
    // filter documents based on lastDeletedPKToDocIndex and lastAddedPKToDocIndex.
    util::Bitmap deletedDoc(/*nItemCount=*/documents.size(), /*bSet=*/false, /*pool=*/nullptr);
    for (int i = documents.size() - 1; i >= 0; --i) {
        const document::DocumentPtr& document = documents[i];
        const std::string& pk = document->GetPrimaryKey();
        if (lastDeletedPKToDocIndex.find(pk) != lastDeletedPKToDocIndex.end()) {
            if (lastDeletedPKToDocIndex[pk] != i) {
                deletedDoc.Set(i);
            }
            continue;
        }
        if (lastAddedPKToDocIndex.find(pk) != lastAddedPKToDocIndex.end()) {
            if (lastAddedPKToDocIndex[pk] > i) {
                deletedDoc.Set(i);
                continue;
            }
            assert(document->GetDocOperateType() == ADD_DOC or document->GetDocOperateType() == UPDATE_FIELD or
                   document->GetDocOperateType() == DELETE_SUB_DOC);
        }
    }
    if (hasSub) {
        MainSubUtil::FilterSubDocsForBatch(documents, lastDeletedPKToDocIndex, lastAddedPKToDocIndex, &deletedDoc);
    }
    std::vector<document::DocumentPtr> result;
    for (int i = 0; i < documents.size(); ++i) {
        if (deletedDoc.Test(i)) {
            continue;
        }
        result.push_back(documents[i]);
    }
    return result;
}

// Assume main doc has 3 ops: Amain Dmain Umain, and sub doc has 2 ops: Dsub Usub(there is no Asub case).
// After filter main doc, there can be only 5 type of cases.
// _Dsub_Dmain_, _Dmain_Dsub_, _Dsub_Amain_, _Amain_Dsub_, _Dsub_. Umain or Usub might be added in all the
// placeholder(_) locations, but updates are only valid in the _ case in *Dsub*Amain_, *Amain*Dsub_, *Dsub_. For
// _Dsub_Dmain_,  _Dmain_Dsub_, _Dsub_Amain_, Dsub can be omitted. We only need to deal with _Amain_Dsub_ case.
void MainSubUtil::FilterSubDocsForBatch(const std::vector<document::DocumentPtr>& documents,
                                        const std::map<std::string, size_t>& lastDeletedPKToDocIndex,
                                        const std::map<std::string, size_t>& lastAddedPKToDocIndex,
                                        util::Bitmap* deletedDoc)
{
    if (documents.empty()) {
        return;
    }
    // For ADD doc in batch, map from main doc pk to deleted sub docs' indices.
    std::map<std::string, std::map<std::string, size_t>> mainPKToSubDeletedPKToDocIndex;
    for (int i = 0; i < documents.size(); ++i) {
        const document::DocumentPtr& document = documents[i];
        DocOperateType opType = document->GetDocOperateType();
        if (opType != DELETE_SUB_DOC) {
            continue;
        }
        const std::string& mainPK = document->GetPrimaryKey();
        if (lastDeletedPKToDocIndex.find(mainPK) != lastDeletedPKToDocIndex.end()) {
            deletedDoc->Set(i);
            continue;
        }
        if (lastAddedPKToDocIndex.find(mainPK) != lastAddedPKToDocIndex.end()) {
            assert(lastAddedPKToDocIndex.at(mainPK) != i);
            deletedDoc->Set(i);
            if (lastAddedPKToDocIndex.at(mainPK) >= i) {
                continue;
            }
        }
        // If DELETE_SUB_DOC is a standalone delete sub doc, or if DELETE_SUB_DOC deletes from an ADD in the batch.
        document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, documents[i]);
        assert(doc);
        std::vector<document::NormalDocumentPtr> subDocs = doc->GetSubDocuments();
        for (int j = 0; j < subDocs.size(); ++j) {
            const document::NormalDocumentPtr& subDoc = subDocs[j];
            const std::string& subPK = subDoc->GetPrimaryKey();
            if (mainPKToSubDeletedPKToDocIndex.find(mainPK) != mainPKToSubDeletedPKToDocIndex.end()) {
                mainPKToSubDeletedPKToDocIndex[mainPK][subPK] = i;
            } else {
                mainPKToSubDeletedPKToDocIndex[mainPK] = std::map<std::string, size_t> {{subPK, i}};
            }
        }
    }
    // Remove deleted sub doc from ADD or UPDATE docs that appear after the DELETE_SUB doc in the batch.
    for (int i = 0; i < documents.size(); ++i) {
        document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, documents[i]);
        DocOperateType opType = doc->GetDocOperateType();
        if (opType != UPDATE_FIELD and opType != ADD_DOC and opType != DELETE_SUB_DOC) {
            continue;
        }
        const std::string& mainPK = documents[i]->GetPrimaryKey();
        if (mainPKToSubDeletedPKToDocIndex.find(mainPK) == mainPKToSubDeletedPKToDocIndex.end()) {
            continue;
        }
        for (const auto& pair : mainPKToSubDeletedPKToDocIndex[mainPK]) {
            const std::string& subPK = pair.first;
            size_t docIndex = pair.second;
            if (!(opType == DELETE_SUB_DOC and i == docIndex)) {
                doc->RemoveSubDocument(subPK);
            }
        }
        if (opType == DELETE_SUB_DOC and doc->GetSubDocuments().empty()) {
            deletedDoc->Set(i);
        }
    }
}

}} // namespace indexlib::partition
