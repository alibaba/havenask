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
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/ann/aitheta2/impl/DocMapWrapperBase.h"

namespace indexlibv2::index::ann {
class DocMapperWrapper : public DocMapWrapperBase
{
public:
    DocMapperWrapper(const std::shared_ptr<DocMapper>& docMapper, docid_t baseDocId)
        : _docMapper(docMapper)
        , _baseDocId(baseDocId)
    {
    }
    ~DocMapperWrapper() {}

public:
    docid_t GetNewDocId(docid_t docId) const override
    {
        docid_t globalDocId = _baseDocId + docId;
        return _docMapper->GetNewId(globalDocId);
    }

private:
    std::shared_ptr<DocMapper> _docMapper;
    docid_t _baseDocId;
};

} // namespace indexlibv2::index::ann