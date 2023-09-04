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
#ifndef NAVI_KERNELDEFBUILDER_H
#define NAVI_KERNELDEFBUILDER_H

#include "navi/common.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {

class KernelDef;

class KernelDefBuilder
{
public:
    KernelDefBuilder(KernelDef *def);
    ~KernelDefBuilder();
private:
    KernelDefBuilder(const KernelDefBuilder &);
    KernelDefBuilder& operator=(const KernelDefBuilder &);
public:
    KernelDefBuilder &name(const std::string &name);
    KernelDefBuilder &input(const std::string &name,
                            const std::string &typeId,
                            InputTypeDef inputType = IT_REQUIRE);
    KernelDefBuilder &inputGroup(const std::string &name,
                                 const std::string &typeId,
                                 InputTypeDef channelInputType = IT_REQUIRE,
                                 InputTypeDef groupInputType = IT_REQUIRE);
    KernelDefBuilder &output(const std::string &name,
                             const std::string &typeId);
    KernelDefBuilder &outputGroup(const std::string &name,
                                  const std::string &typeId);
    KernelDefBuilder &dependOn(const std::string &name, bool require, const StaticResourceBindFunc &binder);
    KernelDefBuilder &resourceConfigKey(const std::string &name, const std::string &configKey);
    KernelDefBuilder &selectR(const std::vector<std::string> &candidates,
                              const ResourceSelectFunc &selectorFunc,
                              bool require,
                              const StaticResourceBindFunc &binder);
    KernelDefBuilder &enableBuildR(const std::vector<std::string> &candidates);
    KernelDefBuilder &dynamicResourceGroup(
            const std::string &groupName,
            const DynamicResourceBindFunc &binder);
    KernelDefBuilder &namedData(const std::string &name, bool require,
                                const NamedDataBindFunc &binder);
    KernelDefBuilder &jsonAttrs(const std::string &attrs);
private:
    KernelDef *def() const;
    const BindInfos &getBindInfos() const;
    friend class KernelCreator;
private:
    KernelDef *_def;
    BindInfos _bindInfos;
};

}

#endif //NAVI_KERNELDEFBUILDER_H
