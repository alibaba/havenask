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
#include "indexlib/codegen/codegen_object.h"

#include "autil/ConstString.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace codegen {
IE_LOG_SETUP(codegen, CodegenObject);

CodegenObject::CodegenObject() {}

CodegenObject::~CodegenObject() { mCodegenHint.reset(); }

std::string CodegenObject::getTargetFunctionCall(std::string funcName, int32_t lineNum) const
{
    stringstream ss;
    ss << mCodegenInfo.GetTargetClassName() << "_"
       << "CALL_" << funcName << "_" << lineNum;
    return ss.str();
}

const std::string& CodegenObject::getCodeHash()
{
    if (!mCodeHash.empty()) {
        return mCodeHash;
    }
    AllCodegenInfo codegenInfos;
    codegenInfos.allCodegenInfos = mSubCodegenInfos;
    codegenInfos.allCodegenInfos.push_back(mCodegenInfo);
    mCodeHash = codegenInfos.GetCodeHash();
    return mCodeHash;
}

bool CodegenObject::collectAllCode()
{
    if (!doCollectAllCode()) {
        return false;
    }
    string codeHash = getCodeHash();
    mCodegenInfo.Rename(codeHash);
    return true;
}

void CodegenObject::combineCodegenInfos(CodegenObject* other)
{
    if (!other) {
        return;
    }
    mSubCodegenInfos.push_back(other->mCodegenInfo);
    for (size_t i = 0; i < other->mSubCodegenInfos.size(); i++) {
        mSubCodegenInfos.push_back(other->mSubCodegenInfos[i]);
    }
}

AllCodegenInfo CodegenObject::getAllCodegenInfo()
{
    AllCodegenInfo codegenInfos;
    codegenInfos.allCodegenInfos = mSubCodegenInfos;
    codegenInfos.allCodegenInfos.push_back(mCodegenInfo);
    return codegenInfos;
}

std::string CodegenObject::getAllCollectCode()
{
    AllCodegenInfo codegenInfos;
    codegenInfos.allCodegenInfos = mSubCodegenInfos;
    codegenInfos.allCodegenInfos.push_back(mCodegenInfo);
    // vector<CodegenInfo> allCodegenInfos = mSubCodegenInfos;
    // allCodegenInfos.push_back(mCodegenInfo);
    return ToJsonString(codegenInfos);
}
}} // namespace indexlib::codegen
