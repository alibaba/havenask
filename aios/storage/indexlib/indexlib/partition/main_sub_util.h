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
#pragma once

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(index, PackAttributeWriter);
DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index, SummaryWriter);
DECLARE_REFERENCE_CLASS(index, SourceWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, InplaceModifier);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);
DECLARE_REFERENCE_CLASS(util, Bitmap);

namespace indexlib { namespace partition {

class MainSubUtil
{
public:
    static index::AttributeReaderPtr GetAttributeReader(const partition::PartitionModifierPtr& modifier,
                                                        const config::AttributeConfigPtr& attrConfig, bool hasSub,
                                                        bool isSub);
    static index::PackAttributeReaderPtr GetPackAttributeReader(const partition::PartitionModifierPtr& modifier,
                                                                const config::PackAttributeConfigPtr& packAttrConfig,
                                                                bool hasSub, bool isSub);

    static index::SummaryWriterPtr GetSummaryWriter(const index_base::SegmentWriterPtr& segmentWriter, bool hasSub,
                                                    bool isSub);
    static index::SourceWriterPtr GetSourceWriter(const index_base::SegmentWriterPtr& segmentWriter, bool hasSub,
                                                  bool isSub);

    static docid_t GetBuildingSegmentBaseDocId(const partition::PartitionModifierPtr& modifier, bool hasSub,
                                               bool isSub);

    static std::vector<document::DocumentPtr> FilterDocsForBatch(const std::vector<document::DocumentPtr>& documents,
                                                                 bool hasSub);

    static partition::InplaceModifierPtr GetInplaceModifier(const partition::PartitionModifierPtr& modifier,
                                                            bool hasSub, bool isSub);
    static partition::SingleSegmentWriterPtr GetSegmentWriter(const index_base::SegmentWriterPtr& segmentWriter,
                                                              bool hasSub, bool isSub);

private:
    static void FilterSubDocsForBatch(const std::vector<document::DocumentPtr>& documents,
                                      const std::map<std::string, size_t>& lastDeletedPKToDocIndex,
                                      const std::map<std::string, size_t>& lastAddedPKToDocIndex,
                                      util::Bitmap* deletedDoc);

private:
    MainSubUtil() {}
    ~MainSubUtil() {}
};

}} // namespace indexlib::partition
