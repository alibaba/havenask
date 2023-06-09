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

#include "indexlib/document/RawDocumentDefine.h"

namespace indexlib { namespace document {

using indexlibv2::document::CMD_ADD;
using indexlibv2::document::CMD_ALTER;
using indexlibv2::document::CMD_CHECKPOINT;
using indexlibv2::document::CMD_DELETE;
using indexlibv2::document::CMD_DELETE_SUB;
using indexlibv2::document::CMD_SKIP;
using indexlibv2::document::CMD_TAG;
using indexlibv2::document::CMD_UNKNOWN;
using indexlibv2::document::CMD_UPDATE_FIELD;

using indexlibv2::document::BUILTIN_KEY_TRACE_ID;
using indexlibv2::document::DOC_FLAG_FOR_METRIC;
}} // namespace indexlib::document
