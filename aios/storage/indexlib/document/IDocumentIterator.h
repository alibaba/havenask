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

#include "autil/NoCopyable.h"
#include "indexlib/document/IDocument.h"

namespace indexlibv2::document {

class IDocumentIterator : private autil::NoCopyable
{
public:
    IDocumentIterator() = default;
    virtual ~IDocumentIterator() = default;

public:
    virtual bool HasNext() const = 0;
    virtual std::shared_ptr<IDocument> Next() = 0;
    virtual int64_t GetDocIdx() const = 0;
};

} // namespace indexlibv2::document
