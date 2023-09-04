#ifndef NAVI_TESTRESOURCE_H
#define NAVI_TESTRESOURCE_H

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/example/TestData.h"
#include "navi/resource/QuerySessionR.h"
#include "autil/TimeUtility.h"

namespace navi {

#define DECLARE_RESOURCE(NAME, STAGE, KEY)                                                                             \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override { builder.name(#NAME, navi::STAGE); }                     \
        bool config(ResourceConfigContext &ctx) override {                                                             \
            std::string key(KEY);                                                                                      \
            std::string value;                                                                                         \
            if (!key.empty()) {                                                                                        \
                NAVI_JSONIZE(ctx, KEY, value, value);                                                                  \
                NAVI_KERNEL_LOG(DEBUG, "key [%s] value [%s]", key.c_str(), value.c_str());                             \
            }                                                                                                          \
            return true;                                                                                               \
        }                                                                                                              \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }                                          \
    };

#define DECLARE_RESOURCE_ALL(NAME, STAGE, KEY, DEPEND)                                                                 \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override {                                                         \
            builder.name(#NAME, navi::STAGE).dependOn(#DEPEND, true, nullptr);                                         \
        }                                                                                                              \
        bool config(ResourceConfigContext &ctx) override {                                                             \
            std::string key(KEY);                                                                                      \
            if (!key.empty()) {                                                                                        \
                NAVI_JSONIZE(ctx, KEY, _value);                                                                        \
                NAVI_KERNEL_LOG(DEBUG, "key [%s] value [%s]", key.c_str(), _value.c_str());                            \
            }                                                                                                          \
            return true;                                                                                               \
        }                                                                                                              \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }                                          \
                                                                                                                       \
    private:                                                                                                           \
        std::string _value;                                                                                            \
    };

#define DECLARE_RESOURCE_WITH_ENABLE(NAME, STAGE, DEPEND, BUILD)                                                       \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override {                                                         \
            builder.name(#NAME, navi::STAGE).dependOn(#DEPEND, true, nullptr).enableBuildR({#BUILD});                  \
        }                                                                                                              \
        bool config(ResourceConfigContext &ctx) override { return true; }                                              \
        ErrorCode init(ResourceInitContext &ctx) override {                                                            \
            ResourceMap inputResourceMap;                                                                              \
            auto build = ctx.buildR(#BUILD, ctx.getConfigContext(), inputResourceMap);                                 \
            if (!build) {                                                                                              \
                return EC_ABORT;                                                                                       \
            }                                                                                                          \
            auto pipe = ctx.createRequireKernelAsyncPipe(AS_OPTIONAL);                                                 \
            if (!pipe) {                                                                                               \
                NAVI_KERNEL_LOG(DEBUG, "build kernel async pipe failed");                                              \
                return EC_ABORT;                                                                                       \
            }                                                                                                          \
            NAVI_KERNEL_LOG(DEBUG, "build [%s] success", #BUILD);                                                      \
            return EC_NONE;                                                                                            \
        }                                                                                                              \
    };

#define DECLARE_RESOURCE_WITH_REPLACE(NAME, REPLACE, STAGE, DEPEND)                                                    \
    class NAME : public REPLACE {                                                                                      \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override {                                                         \
            builder.name(#NAME, navi::STAGE).dependOn(#DEPEND, true, nullptr).enableReplaceR({#REPLACE});              \
        }                                                                                                              \
        bool config(ResourceConfigContext &ctx) override { return true; }                                              \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }                                          \
    };

#define DECLARE_RESOURCE_WITH_REPLACE_2(NAME, REPLACE1, REPLACE2, STAGE, DEPEND)                                       \
    class NAME : public REPLACE1 {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override {                                                         \
            builder.name(#NAME, navi::STAGE)                                                                           \
                .dependOn(#DEPEND, true, nullptr)                                                                      \
                .enableReplaceR({#REPLACE1})                                                                           \
                .enableReplaceR({#REPLACE2});                                                                          \
        }                                                                                                              \
        bool config(ResourceConfigContext &ctx) override { return true; }                                              \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }                                          \
    };

#define DECLARE_FAILED_RESOURCE(NAME, STAGE)                                                                           \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override { builder.name(#NAME, navi::STAGE); }                     \
        bool config(ResourceConfigContext &ctx) override { return false; }                                             \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_ABORT; }                                         \
    };

#define DECLARE_RESOURCE_DEPEND1(NAME, DEPEND)                                                                         \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override {                                                         \
            builder.name(#NAME, navi::RS_BIZ).dependOn(#DEPEND, true, nullptr);                                        \
        }                                                                                                              \
        ErrorCode init(ResourceInitContext &ctx) override {                                                            \
            auto pipe = ctx.createRequireKernelAsyncPipe(AS_OPTIONAL);                                                 \
            if (pipe) {                                                                                                \
                NAVI_KERNEL_LOG(DEBUG, "BUG! stage RS_BIZ resource create async pipe success");                        \
                return EC_ABORT;                                                                                       \
            }                                                                                                          \
            if (ctx.getDependResource(#DEPEND)) {                                                                      \
                return EC_NONE;                                                                                        \
            } else {                                                                                                   \
                return EC_LACK_RESOURCE;                                                                               \
            }                                                                                                          \
        }                                                                                                              \
    };

#define DECLARE_RESOURCE_DEPEND1_PUBLISH(NAME, DEPEND, META)                                                           \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override {                                                         \
            builder.name(#NAME, navi::RS_BIZ).dependOn(#DEPEND, true, nullptr);                                        \
        }                                                                                                              \
        bool getMeta(std::string &meta) const override {                                                               \
            meta = std::string(#META) + "_" + "ok";                                                                    \
            return true;                                                                                               \
        }                                                                                                              \
        ErrorCode init(ResourceInitContext &ctx) override {                                                            \
            if (ctx.getDependResource(#DEPEND)) {                                                                      \
                return EC_NONE;                                                                                        \
            } else {                                                                                                   \
                return EC_LACK_RESOURCE;                                                                               \
            }                                                                                                          \
        }                                                                                                              \
    };

#define DECLARE_RESOURCE_DEPEND2(NAME, DEPEND1, DEPEND2)                       \
    class NAME : public Resource {                                             \
    public:                                                                    \
        ~NAME() {                                                              \
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed",                 \
                            getName().c_str());                                \
        }                                                                      \
                                                                               \
    public:                                                                    \
        void def(ResourceDefBuilder &builder) const override {                 \
            builder.name(#NAME, navi::RS_SUB_GRAPH)                            \
                .dependOn(#DEPEND1, true, nullptr)                             \
                .dependOn(#DEPEND2, true, nullptr);                            \
        }                                                                      \
        ErrorCode init(ResourceInitContext &ctx) override {                    \
            if (ctx.getDependResource<DEPEND1>(#DEPEND1) &&                    \
                ctx.getDependResource<DEPEND2>(#DEPEND2)) {                    \
                return EC_NONE;                                                \
            } else {                                                           \
                return EC_LACK_RESOURCE;                                       \
            }                                                                  \
        }                                                                      \
    };

#define DECLARE_RESOURCE_DEPEND_OPTIONAL(NAME, DEPEND, OPTIONAL)               \
    class NAME : public Resource {                                             \
    public:                                                                    \
        ~NAME() {                                                              \
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed",                 \
                            getName().c_str());                                \
        }                                                                      \
                                                                               \
    public:                                                                    \
        void def(ResourceDefBuilder &builder) const override {                 \
            builder.name(#NAME, navi::RS_SUB_GRAPH)                            \
                .dependOn(#DEPEND, true, nullptr)                              \
                .dependOn(#OPTIONAL, false, nullptr);                          \
        }                                                                      \
        ErrorCode init(ResourceInitContext &ctx) override {                    \
            if (ctx.getDependResource<DEPEND>(#DEPEND)) {                      \
                return EC_NONE;                                                \
            } else {                                                           \
                return EC_LACK_RESOURCE;                                       \
            }                                                                  \
        }                                                                      \
    };

#define DECLARE_DYNAMIC_RESOURCE_DEPEND2(NAME, GROUP1, DEPEND1, GROUP2,        \
                                         DEPEND2)                              \
    class NAME : public Resource {                                             \
    public:                                                                    \
        ~NAME() {                                                              \
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed",                 \
                            getName().c_str());                                \
        }                                                                      \
                                                                               \
    public:                                                                    \
        void def(ResourceDefBuilder &builder) const override {                 \
            builder.name(#NAME, navi::RS_SUB_GRAPH)                            \
                .dynamicResourceGroup(#GROUP1,                                 \
                                      BIND_DYNAMIC_RESOURCE_TO(_depend1))      \
                .dynamicResourceGroup(#GROUP2,                                 \
                                      BIND_DYNAMIC_RESOURCE_TO(_depend2));     \
        }                                                                      \
        ErrorCode init(ResourceInitContext &ctx) override {                    \
            if (1u != _depend1.size()) {                                       \
                NAVI_KERNEL_LOG(ERROR, "depend1 size is not 1 [%lu]",          \
                                _depend1.size());                              \
                return EC_LACK_RESOURCE;                                       \
            }                                                                  \
            if (1u != _depend2.size()) {                                       \
                NAVI_KERNEL_LOG(ERROR, "depend2 size is not 1 [%lu]",          \
                                _depend2.size());                              \
                return EC_LACK_RESOURCE;                                       \
            }                                                                  \
            for (auto depend : _depend1) {                                     \
                if (!dynamic_cast<DEPEND1 *>(depend)) {                        \
                    NAVI_KERNEL_LOG(ERROR, "resource type is not [%s] [%p]",   \
                                    #DEPEND1, depend);                         \
                    return EC_LACK_RESOURCE;                                   \
                }                                                              \
                NAVI_KERNEL_LOG(DEBUG, "bind dynamic resource [%s] success",   \
                                depend->getName().c_str());                    \
            }                                                                  \
            for (auto depend : _depend2) {                                     \
                if (!dynamic_cast<DEPEND2 *>(depend)) {                        \
                    NAVI_KERNEL_LOG(ERROR, "resource type is not [%s] [%p]",   \
                                    #DEPEND2, depend);                         \
                    return EC_LACK_RESOURCE;                                   \
                }                                                              \
                NAVI_KERNEL_LOG(DEBUG, "bind dynamic resource [%s] success",   \
                                depend->getName().c_str());                    \
            }                                                                  \
            return EC_NONE;                                                    \
        }                                                                      \
                                                                               \
    private:                                                                   \
        std::set<DEPEND1 *> _depend1;                                          \
        std::set<DEPEND2 *> _depend2;                                          \
    };

#define DECLARE_DEPEND_BY_RESOURCE(NAME, BY, GROUP)                            \
    class NAME : public Resource {                                             \
    public:                                                                    \
        ~NAME() {                                                              \
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed",                 \
                            getName().c_str());                                \
        }                                                                      \
                                                                               \
    public:                                                                    \
        void def(ResourceDefBuilder &builder) const override {                 \
            builder.name(#NAME, navi::RS_SUB_GRAPH)                            \
                .dependByResource(#BY, #GROUP);                                \
        }                                                                      \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }  \
    };

#define DECLARE_DEPEND_BY_KERNEL(NAME, BY, GROUP)                              \
    class NAME : public Resource {                                             \
    public:                                                                    \
        ~NAME() {                                                              \
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed",                 \
                            getName().c_str());                                \
        }                                                                      \
                                                                               \
    public:                                                                    \
        void def(ResourceDefBuilder &builder) const override {                 \
            builder.name(#NAME, navi::RS_SUB_GRAPH)                            \
                .dependByKernel(#BY, #GROUP);                                  \
        }                                                                      \
        ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }  \
    };

#define DECLARE_RESOURCE_DEPEND_NAMED_DATA(NAME, DATA)                                                                 \
    class NAME : public Resource {                                                                                     \
    public:                                                                                                            \
        ~NAME() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }                             \
                                                                                                                       \
    public:                                                                                                            \
        void def(ResourceDefBuilder &builder) const override { builder.name(#NAME, navi::RS_SUB_GRAPH); }              \
        ErrorCode init(ResourceInitContext &ctx) override {                                                            \
            if (!_data) {                                                                                              \
                return EC_INIT_RESOURCE;                                                                               \
            }                                                                                                          \
            const auto &dataVec = _data->getData();                                                                    \
            if (!dataVec.empty()) {                                                                                    \
                NAVI_KERNEL_LOG(DEBUG, "named data index 0: [%s]", dataVec[0].c_str());                                \
            }                                                                                                          \
            return EC_NONE;                                                                                            \
        }                                                                                                              \
                                                                                                                       \
    private:                                                                                                           \
        RESOURCE_DEPEND_DECLARE();                                                                                     \
                                                                                                                       \
    private:                                                                                                           \
        RESOURCE_NAMED_DATA(HelloData, _data, #DATA);                                                                  \
    };

//       ------------------> D
//      /    --> K --> G     ^
//    F     |(O)             |
//     \    |                |
// -----\-> E ->------------>|
// |           |             |
// |           |             |
// A --------> B ----------> C

class A;
class B;
class C;
class D;
class ER;
class F;
class K;
class G;

DECLARE_RESOURCE_DEPEND2(A, B, ER);
DECLARE_RESOURCE_DEPEND2(B, C, D);
DECLARE_RESOURCE_DEPEND1(C, D);
DECLARE_RESOURCE_DEPEND1(D, G);
DECLARE_RESOURCE_DEPEND_OPTIONAL(ER, D, K);
DECLARE_RESOURCE_DEPEND1(K, G);
DECLARE_RESOURCE_DEPEND2(F, D, ER);
DECLARE_RESOURCE(G, RS_BIZ, "");
DECLARE_RESOURCE(W, RS_BIZ_PART, "");
DECLARE_RESOURCE_DEPEND2(X, W, C);
DECLARE_DEPEND_BY_RESOURCE(M, H, depend1);
DECLARE_DEPEND_BY_RESOURCE(NR, H, depend2);
DECLARE_DYNAMIC_RESOURCE_DEPEND2(H, depend1, M, depend2, NR);
DECLARE_DEPEND_BY_KERNEL(U, WorldKernel, group1);
DECLARE_RESOURCE_DEPEND_OPTIONAL(DependOnBizResource, D, C);
DECLARE_RESOURCE_DEPEND1_PUBLISH(ProduceTest, navi.buildin.gig_meta.r, produce_meta);
DECLARE_RESOURCE(External, RS_EXTERNAL, "");
DECLARE_RESOURCE_ALL(KernelTestR1, RS_KERNEL, "biz_name", KernelTestR2);
DECLARE_RESOURCE_ALL(KernelTestR2, RS_KERNEL, "biz_name", A);
DECLARE_RESOURCE_ALL(KernelTestR3, RS_KERNEL, "biz_name", A);
DECLARE_RESOURCE_ALL(KernelTestR4, RS_KERNEL, "biz_name", B);
DECLARE_RESOURCE_WITH_ENABLE(KernelTestBuildR, RS_KERNEL, A, KernelTestR4);
DECLARE_FAILED_RESOURCE(KernelTestRFailed, RS_KERNEL);
DECLARE_RESOURCE_ALL(KernelTestR5, RS_KERNEL, "r5_key", A);
DECLARE_RESOURCE_WITH_REPLACE(ReplaceA, A, RS_SUB_GRAPH, B);
DECLARE_RESOURCE_WITH_REPLACE_2(ReplaceAA, ReplaceA, A, RS_SUB_GRAPH, K);
DECLARE_RESOURCE_WITH_REPLACE(ReplaceB, B, RS_SUB_GRAPH, C);

DECLARE_RESOURCE_DEPEND_NAMED_DATA(V, test_named_data);

// DECLARE_RESOURCE_DEPEND_OPTIONAL(DependOnBizResource, D, TEST_BIZ_RESOURCE_RUN_GRAPH);

class TestStaticGraphLoaderR : public Resource {
public:
    ~TestStaticGraphLoaderR() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }

public:
    void def(ResourceDefBuilder &builder) const override { builder.name("test_static_graph_loader", navi::RS_BIZ); }
    bool config(ResourceConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(ResourceInitContext &ctx) override { return EC_NONE; }
    bool getStaticGraphInfo(std::vector<StaticGraphInfoPtr> &graphInfos) const override {
        auto info1 = std::make_shared<StaticGraphInfo>();
        info1->name = "test_graph_1";
        info1->meta = "graph_1_meta";
        graphInfos.push_back(info1);
        auto info2 = std::make_shared<StaticGraphInfo>();
        info2->name = "test_graph_2";
        info2->meta = "graph_2_meta";
        graphInfos.push_back(info2);
        return true;
    }
private:
    RESOURCE_DEPEND_DECLARE();
private:
    RESOURCE_DEPEND_ON_ID(G, _g, "G", true);
};

class TestAsyncPipeCreateR : public Resource {
public:
    ~TestAsyncPipeCreateR() { NAVI_KERNEL_LOG(DEBUG, "resource [%s] destructed", getName().c_str()); }

public:
    void def(ResourceDefBuilder &builder) const override { builder.name("test_async_pipe_create_r", navi::RS_KERNEL); }
    bool config(ResourceConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(ResourceInitContext &ctx) override {
        {
            auto pipe = ctx.createRequireKernelAsyncPipe(AS_OPTIONAL);
            if (!pipe) {
                NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
                return EC_ABORT;
            }
            pipe->terminate();
        }
        {
            auto pipe = ctx.createRequireKernelAsyncPipe(AS_REQUIRED);
            if (!pipe) {
                NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
                return EC_ABORT;
            }
            pipe->terminate();
        }
        {
            auto pipe = ctx.createRequireKernelAsyncPipe(AS_INDEPENDENT);
            if (!pipe) {
                NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
                return EC_ABORT;
            }
            pipe->terminate();
        }
        return EC_NONE;
    }
private:
    RESOURCE_DEPEND_DECLARE();
private:
    RESOURCE_DEPEND_ON_ID(G, _g, "G", true);
    RESOURCE_DEPEND_ON_ID(External, _ex, "External", false);
    RESOURCE_DEPEND_ON(QuerySessionR, _querySessionR);
};

}

#endif //NAVI_TESTRESOURCE_H
