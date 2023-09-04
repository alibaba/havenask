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
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Status.h"

namespace indexlibv2 { namespace document {
class DocumentRewriteChain;
}} // namespace indexlibv2::document
namespace indexlibv2 { namespace config {
class ITabletSchema;
}} // namespace indexlibv2::config
namespace indexlibv2::table {

class NormalDocumentRewriteChainCreator
{
public:
    static std::pair<Status, std::shared_ptr<document::DocumentRewriteChain>>
    Create(const std::shared_ptr<config::ITabletSchema>& schema);

private:
    static Status AppendDocInfoRewriterIfNeed(const std::shared_ptr<config::ITabletSchema>& schema,
                                              document::DocumentRewriteChain* rewriteChain);
    static Status AppendTTLSetterIfNeed(const std::shared_ptr<config::ITabletSchema>& schema,
                                        document::DocumentRewriteChain* rewriteChain);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
