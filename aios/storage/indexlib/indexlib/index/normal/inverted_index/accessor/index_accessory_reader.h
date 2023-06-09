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
#ifndef __INDEXLIB_INDEX_ACCESSORY_READER_H
#define __INDEXLIB_INDEX_ACCESSORY_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, PackageIndexConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, LegacySectionAttributeReaderImpl);

namespace indexlib { namespace index {

class LegacyIndexAccessoryReader
{
public:
    typedef std::map<std::string, size_t> SectionReaderPosMap;
    typedef std::vector<LegacySectionAttributeReaderImplPtr> SectionReaderVec;

    typedef std::vector<std::string> ShareSectionIndexNames;
    typedef std::map<std::string, ShareSectionIndexNames> ShareSectionMap;

public:
    LegacyIndexAccessoryReader(const config::IndexSchemaPtr& schema);
    LegacyIndexAccessoryReader(const LegacyIndexAccessoryReader& other);
    virtual ~LegacyIndexAccessoryReader();

public:
    bool Open(const index_base::PartitionDataPtr& partitionData);

    // virtual for ut
    virtual const LegacySectionAttributeReaderImplPtr GetSectionReader(const std::string& indexName) const;

    LegacyIndexAccessoryReader* Clone() const;

private:
    void CloneSectionReaders(const SectionReaderVec& srcReaders, SectionReaderVec& clonedReaders);

    bool CheckSectionConfig(const config::IndexConfigPtr& indexConfig);

    bool InitSectionReader(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData,
                           ShareSectionMap& shareMap);

    void AddSharedRelations(const config::PackageIndexConfigPtr& packIndexConfig, ShareSectionMap& shareMap);

    bool UseSharedSectionReader(const config::PackageIndexConfigPtr& packIndexConfig);

    // virtual for test
    virtual LegacySectionAttributeReaderImplPtr OpenSectionReader(const config::PackageIndexConfigPtr& indexConfig,
                                                                  const index_base::PartitionDataPtr& partitionData);

private:
    config::IndexSchemaPtr mIndexSchema;

    SectionReaderPosMap mSectionReaderMap;
    SectionReaderVec mSectionReaderVec;

    // for unit test
    friend class LegacyIndexAccessoryReaderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LegacyIndexAccessoryReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_INDEX_ACCESSORY_READER_H
