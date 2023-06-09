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
#ifndef NAVI_KERNEL_H
#define NAVI_KERNEL_H

#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/KernelCreator.h"
#include "navi/engine/KernelInitContext.h"
#include "navi/log/NaviLogger.h"
#include <map>

namespace navi {

class NodeDef;
class Biz;

class Kernel
{
public:
    Kernel();
    virtual ~Kernel();
private:
    Kernel(const Kernel &);
    Kernel& operator=(const Kernel &);
public:
    virtual void def(KernelDefBuilder &builder) const = 0;
    virtual bool config(KernelConfigContext &ctx) {
        return true;
    }
    virtual ErrorCode init(KernelInitContext &ctx) {
        return EC_NONE;
    }
    virtual ErrorCode compute(KernelComputeContext &ctx) = 0;
    virtual bool isExpensive() const {
        return true;
    }
public:
    const std::string &getNodeName() const;
    const std::string &getKernelName() const;
    const std::string &getDeviceName() const;
private:
    void setNodeDef(const NodeDef *def);
    friend class Node;
private:
    const NodeDef *_def;
};

NAVI_TYPEDEF_PTR(Kernel);

#define REGISTER_KERNEL(NAME) REGISTER_KERNEL_HELPER(__COUNTER__, NAME)
#define REGISTER_KERNEL_HELPER(ctr, NAME) REGISTER_KERNEL_UNIQ(ctr, NAME)
#define REGISTER_KERNEL_UNIQ(ctr, NAME)                                        \
    __attribute__((constructor)) static void Register##Kernel##ctr() {         \
        ::navi::CreatorRegistry::current(navi::RT_KERNEL)                      \
            ->addCreatorFunc([]() -> ::navi::Creator * {                       \
                auto def = new navi::KernelDef();                              \
                navi::KernelDefBuilder builder(def);                           \
                NAME().def(builder);                                           \
                return new ::navi::KernelCreator(                              \
                    builder, [](autil::mem_pool::Pool *pool) {                 \
                        return NAVI_POOL_NEW_CLASS(pool, NAME);                \
                    });                                                        \
            });                                                                \
    }

#define REGISTER_KERNEL_INIT(FUNC)                                             \
    REGISTER_KERNEL_INIT_HELPER(__COUNTER__, FUNC)
#define REGISTER_KERNEL_INIT_HELPER(ctr, FUNC)                                 \
    REGISTER_KERNEL_INIT_UNIQ(ctr, FUNC)
#define REGISTER_KERNEL_INIT_UNIQ(ctr, FUNC)                                   \
    __attribute__((constructor)) static void Register##KernelInit##ctr() {     \
        ::navi::CreatorRegistry::current(navi::RT_KERNEL)                      \
            ->addModuleInitFunc([]() {                                         \
                if (!FUNC()) {                                                 \
                    NAVI_KERNEL_LOG(ERROR, "module init failed");              \
                    return false;                                              \
                }                                                              \
                return true;                                                   \
            });                                                                \
    }
}

#endif //NAVI_KERNEL_H
