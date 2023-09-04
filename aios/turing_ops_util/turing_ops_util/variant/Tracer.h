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

#include <stddef.h>
#include <stdint.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/EnvUtil.h"

namespace autil {
class DataBuffer;
class ThreadMutex;
}  // namespace autil

namespace kv_tracer{
class UploadTracer;
}

namespace suez {
namespace turing {

extern const size_t ISEARCH_TRACE_MAX_LEN;
extern const size_t ISEARCH_TRACE_MAX_SIZE;

#define ISEARCH_TRACE_ALL 0
#define ISEARCH_TRACE_TRACE3 100
#define ISEARCH_TRACE_TRACE2 200
#define ISEARCH_TRACE_TRACE1 300
#define ISEARCH_TRACE_DEBUG 400
#define ISEARCH_TRACE_INFO 500
#define ISEARCH_TRACE_WARN 600
#define ISEARCH_TRACE_ERROR 700
#define ISEARCH_TRACE_FATAL 800
#define ISEARCH_TRACE_NONE 10000
#define ISEARCH_TRACE_DISABLE 10100

#define RANK_TRACER_NAME "_sys_reserved_rank_tracer"

class Tracer {
public:
    enum TraceMode { TM_VECTOR, TM_TREE };

public:
    Tracer(TraceMode mode = TM_VECTOR);
    virtual ~Tracer();
public:
    Tracer(const Tracer &other);
public:
    Tracer *clone();
    Tracer *cloneWithoutTraceInfo();
    static int32_t convertLevel(const std::string &level);
    void setLevel(int32_t level);
    void setLevel(const std::string &level);
    int32_t getLevel() { return _level; }

    virtual void trace(const char *traceInfo); // virtual for navi trace adapter
    void trace(const std::string &traceInfo);

    void simpleTrace(const char *format, ...)
        __attribute__((format(printf, 2, 3)));

    void rankTrace(const char *levelStr, const char *fileName, int line, const char *format, ...)
        __attribute__((format(printf, 5, 6)));
    void requestTrace(const char *levleStr, const char *format, ...)
        __attribute__((format(printf, 3, 4)));

    void addTrace(const std::string &trace);

    const std::vector<std::string>& getTraceInfoVec() const {
        return _traceInfo;
    }

    std::string getTraceInfo() const;
    inline bool isLevelEnabled(int32_t level) {
        return (_level <= level);
    }

    void setPartitionId(const std::string &partitionId) {
        _partitionId = partitionId;
    }

    void setAddress(const std::string &address) {
        _address = address;
    }

    const std::string &getAddress() const {
        return _address;
    }

    const std::string &getPartitionId() const {
        return _partitionId;
    }

    void merge(Tracer *tracer);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    std::string toXMLString() const;

    void setTracePosFront(bool tag) {
        _tracePosFront = tag;
    }

    bool isKVTrace() const;

    static std::string getFormatTime();

    Tracer &operator=(const Tracer &tracer);
    kv_tracer::UploadTracer *getKVTracer();

private:
    kv_tracer::UploadTracer *createKVTracer();
    void deleteKVTracer(kv_tracer::UploadTracer* kvTracer);
    std::string kvTracerSerialize() const;
    void kvTracerDeserialize(const std::string& traceInfo);
    void kvTracerInsertInfo(const std::string& traceInfo, bool uniq);
    std::string kvTracerGetJson() const;
    void kvTracerAppendUploadTracer(const std::string&,
                                    kv_tracer::UploadTracer &kvTracer);

public:
    static const std::string FATAL;
    static const std::string ERROR;
    static const std::string WARN;
    static const std::string INFO;
    static const std::string DEBUG;
    static const std::string TRACE1;
    static const std::string TRACE2;
    static const std::string TRACE3;
    static const std::string ALL;
    static const std::string DISABLE;
    static const std::string NONE;
private:
    kv_tracer::UploadTracer *_kvTracer;
    std::vector<std::string> _traceInfo;
    std::string _partitionId;
    std::string _address;
    int32_t _level;
    bool _tracePosFront;
    autil::ThreadMutex *_mutex;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Tracer> TracerPtr;

inline std::ostream &operator<<(std::ostream &os, const Tracer &tracer) {
    os << tracer.getTraceInfo();
    return os;
}

#define SET_TRACE_POS_FRONT()                \
    {                                        \
        if (_tracer != NULL) {               \
            _tracer->setTracePosFront(true); \
        }                                    \
    }

#define UNSET_TRACE_POS_FRONT()               \
    {                                         \
        if (_tracer != NULL) {                \
            _tracer->setTracePosFront(false); \
        }                                     \
    }

#define ENABLE_TRACE(level)                     \
    ((_tracer != NULL) && (_tracer->isLevelEnabled(ISEARCH_TRACE_##level)))

#define ENABLE_WITH_TRACE(tracer, level)                                      \
    ((tracer != NULL) && (tracer->isLevelEnabled(ISEARCH_TRACE_##level)))


#define TRACE(level, format, args...)                                                  \
    if (unlikely(_tracer != NULL && _tracer->isLevelEnabled(ISEARCH_TRACE_##level))) { \
        _tracer->simpleTrace(format, ##args);                                          \
    }

#define TRACE_WITH_TRACER(tracer, level, format, args...)                              \
    if (unlikely(tracer != NULL && tracer->isLevelEnabled(ISEARCH_TRACE_##level))) {   \
        tracer->simpleTrace(format, ##args);                                           \
    }

#define TRACE_F(level, format, args...)                                                \
    if (unlikely(_tracer != NULL && _tracer->isLevelEnabled(ISEARCH_TRACE_##level))) { \
        _tracer->rankTrace(#level, __FILE__, __LINE__, format, ##args);                \
    }

#define TRACE_F_WITH_TRACER(tracer, level, format, args...)                            \
    if (unlikely(tracer != NULL && tracer->isLevelEnabled(ISEARCH_TRACE_##level))) {   \
        tracer->rankTrace(#level, __FILE__, __LINE__, format, ##args);                 \
    }

#define MATCHDOC_TRACE_LEVEL_ENABLED(level, matchdoc) \
    (unlikely(_traceRefer == NULL                     \
                  ? false                             \
                  : (_traceRefer->getPointer(matchdoc)->isLevelEnabled(ISEARCH_TRACE_##level))))

#define MATCHDOC_TRACE(level, matchdoc, format, args...)                       \
    {                                                                          \
        if (unlikely(_traceRefer != NULL)) {                                   \
            suez::turing::Tracer *tracer = _traceRefer->getPointer(matchdoc);  \
            if (unlikely(tracer->isLevelEnabled(ISEARCH_TRACE_##level))) {     \
                tracer->rankTrace(#level, __FILE__, __LINE__, format, ##args); \
            }                                                                  \
        }                                                                      \
    }

#define INNER_REQUEST_TRACE_WITH_TRACER(tracer, level, levelStr, format, args...) \
    {                                                                             \
        if (unlikely(tracer != NULL && tracer->isLevelEnabled(level))) {          \
            tracer->requestTrace(levelStr, format, ##args);                       \
        }                                                                         \
    }

#define REQUEST_TRACE_WITH_TRACER(tracer, level, format, args...) \
    INNER_REQUEST_TRACE_WITH_TRACER(tracer, ISEARCH_TRACE_##level, #level, format, ##args)

#define REQUEST_TRACE(level, format, args...) \
    INNER_REQUEST_TRACE_WITH_TRACER(_tracer, ISEARCH_TRACE_##level, #level, format, ##args)

#define KVTRACE_MODULE_SCOPE(module_name) KVTRACE_MODULE_SCOPE_WITH_TRACE(_trace, module_name)

////////////
#define SUEZ_RANK_TRACE_LEVEL_ENABLED(level, matchdoc) \
    (unlikely(_traceRefer == NULL                      \
                  ? false                              \
                  : (_traceRefer->getPointer(matchdoc)->isLevelEnabled(ISEARCH_TRACE_##level))))

#define SUEZ_RANK_TRACE(level, matchdoc, format, args...)                      \
    {                                                                          \
        if (unlikely(_traceRefer != NULL)) {                                   \
            suez::turing::Tracer *tracer = _traceRefer->getPointer(matchdoc);  \
            if (unlikely(tracer->isLevelEnabled(ISEARCH_TRACE_##level))) {     \
                tracer->rankTrace(#level, __FILE__, __LINE__, format, ##args); \
            }                                                                  \
        }                                                                      \
    }

#define SUEZ_CAVA_RANK_TRACE(level, matchdoc, format, args...)          \
    {                                                                   \
        if (unlikely(_traceRefer != NULL)) {                            \
            suez::turing::Tracer *tracer = _traceRefer->getPointer(matchdoc); \
            if (unlikely(tracer->isLevelEnabled(ISEARCH_TRACE_##level))) { \
                tracer->simpleTrace(format, ##args);                    \
            }                                                           \
        }                                                               \
    }

#define SUEZ_INNER_REQUEST_TRACE_WITH_TRACER(tracer, level, levelStr, format, args...) \
    {                                                                                  \
        if (unlikely(tracer != NULL && tracer->isLevelEnabled(level))) {               \
            tracer->requestTrace(levelStr, format, ##args);                            \
        }                                                                              \
    }

#define SUEZ_REQUEST_TRACE_WITH_TRACER(tracer, level, format, args...) \
    SUEZ_INNER_REQUEST_TRACE_WITH_TRACER(tracer, ISEARCH_TRACE_##level, #level, format, ##args)

#define SUEZ_REQUEST_TRACE(level, format, args...) \
    SUEZ_INNER_REQUEST_TRACE_WITH_TRACER(_tracer, ISEARCH_TRACE_##level, #level, format, ##args)
}  // namespace turing
}  // namespace suez
