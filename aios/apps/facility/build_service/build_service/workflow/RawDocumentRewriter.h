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
#include <string>

#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"

namespace indexlibv2 { namespace framework {
class ITablet;
}} // namespace indexlibv2::framework

namespace build_service { namespace workflow {

class RawDocumentRewriter
{
public:
    class Rewriter;

    RawDocumentRewriter();
    virtual ~RawDocumentRewriter();

    bool init(std::shared_ptr<indexlibv2::framework::ITablet> tablet);
    /*retry if rewrite failed*/
    virtual bool rewrite(build_service::document::RawDocument* rawDoc);

    static bool tableTypeUpdatable(const std::string& tableType);

private:
    bool selectRewriter();

private:
    std::unique_ptr<Rewriter> _rewriter;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocumentRewriter);

}} // namespace build_service::workflow
