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
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeReaderImpl.h"

#include "autil/TimeUtility.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/index/inverted_index/section_attribute/InDocMultiSectionMeta.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, SectionAttributeReaderImpl);

Status SectionAttributeReaderImpl::Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                        const indexlibv2::framework::TabletData* tabletData)
{
    autil::ScopedTime2 timer;
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(indexConfig);
    assert(_indexConfig != nullptr);
    AUTIL_LOG(INFO, "Start opening section attribute(%s).", _indexConfig->GetIndexName().c_str());

    auto sectionAttrConf = _indexConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    _formatter.reset(new SectionAttributeFormatter(sectionAttrConf));

    _sectionDataReader = std::make_shared<SectionDataReader>(_sortType);
    auto status = _sectionDataReader->Open(_indexConfig, tabletData);
    RETURN_IF_STATUS_ERROR(status, "open section attribute reader fail. error [%s].", status.ToString().c_str());
    AUTIL_LOG(INFO, "Finish opening section attribute(%s), used[%.3f]s", _indexConfig->GetIndexName().c_str(),
              timer.done_sec());
    return Status::OK();
}

SectionAttributeReader* SectionAttributeReaderImpl::Clone() const { return new SectionAttributeReaderImpl(*this); }

std::shared_ptr<InDocSectionMeta> SectionAttributeReaderImpl::GetSection(docid_t docId) const
{
    auto inDocSectionMeta = std::make_shared<InDocMultiSectionMeta>(_indexConfig);
    assert(HasFieldId() == _indexConfig->GetSectionAttributeConfig()->HasFieldId());
    assert(HasSectionWeight() == _indexConfig->GetSectionAttributeConfig()->HasSectionWeight());

    if (Read(docId, inDocSectionMeta->GetDataBuffer(), MAX_SECTION_BUFFER_LEN) < 0) {
        return nullptr;
    }
    inDocSectionMeta->UnpackInDocBuffer(HasFieldId(), HasSectionWeight());
    return inDocSectionMeta;
}

} // namespace indexlib::index
