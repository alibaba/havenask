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
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"

#include <memory>

#include "autil/TimeUtility.h"
#include "fslib/fslib.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {

IE_LOG_SETUP(index, LegacySectionAttributeReaderImpl);

LegacySectionAttributeReaderImpl::LegacySectionAttributeReaderImpl()
    : SectionAttributeReaderImpl(indexlibv2::config::sp_nosort)
{
}

LegacySectionAttributeReaderImpl::~LegacySectionAttributeReaderImpl() {}

void LegacySectionAttributeReaderImpl::Open(const config::PackageIndexConfigPtr& indexConfig,
                                            const index_base::PartitionDataPtr& partitionData)
{
    autil::ScopedTime2 timer;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> config = indexConfig->ConstructConfigV2();
    assert(config);
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(config);
    assert(_indexConfig);
    IE_LOG(INFO, "Start opening section attribute(%s).", indexConfig->GetIndexName().c_str());

    SectionAttributeConfigPtr sectionAttrConf = indexConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    mFormatter.reset(new common::SectionAttributeFormatter(sectionAttrConf));

    AttributeConfigPtr attrConf = sectionAttrConf->CreateAttributeConfig(indexConfig->GetIndexName());

    mSectionDataReader.reset(new legacy::SectionDataReader);
    mSectionDataReader->Open(attrConf, partitionData, PatchApplyStrategy::PAS_APPLY_NO_PATCH);

    IE_LOG(INFO, "Finish opening section attribute(%s), used[%.3f]s", indexConfig->GetIndexName().c_str(),
           timer.done_sec());
}

SectionAttributeReader* LegacySectionAttributeReaderImpl::Clone() const
{
    return new LegacySectionAttributeReaderImpl(*this);
}
}} // namespace indexlib::index
