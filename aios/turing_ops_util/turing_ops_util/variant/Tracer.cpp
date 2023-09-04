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
#include "turing_ops_util/variant/Tracer.h"

#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <ostream>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "matchdoc/ReferenceTypesWrapper.h"

using namespace std;
using namespace autil;
namespace suez {
namespace turing {

const size_t ISEARCH_TRACE_MAX_LEN = autil::EnvUtil::getEnv("ISEARCH_TRACE_MAX_LEN", 1*1024*1024UL);
const size_t ISEARCH_TRACE_MAX_SIZE = autil::EnvUtil::getEnv("ISEARCH_TRACE_MAX_SIZE", 100*1024UL);

AUTIL_LOG_SETUP(turing, Tracer);

const std::string Tracer::FATAL = "FATAL";
const std::string Tracer::ERROR = "ERROR";
const std::string Tracer::WARN = "WARN";
const std::string Tracer::INFO = "INFO";
const std::string Tracer::DEBUG = "DEBUG";
const std::string Tracer::TRACE1 = "TRACE1";
const std::string Tracer::TRACE2 = "TRACE2";
const std::string Tracer::TRACE3 = "TRACE3";
const std::string Tracer::ALL = "ALL";
const std::string Tracer::DISABLE = "DISABLE";
const std::string Tracer::NONE = "NONE";

Tracer::Tracer(TraceMode mode)
    : _kvTracer(nullptr)
    , _level(ISEARCH_TRACE_NONE)
    , _tracePosFront(false)
    , _mutex(new ThreadMutex())
{
    if (mode == TM_TREE) {
        _kvTracer = createKVTracer();
    }
}

Tracer::~Tracer() {
    deleteKVTracer(_kvTracer);
    _kvTracer = nullptr;
    DELETE_AND_SET_NULL(_mutex);
}

Tracer::Tracer(const Tracer &other) {
    if (other._kvTracer) {
        _kvTracer = createKVTracer();
        string traceInfo = other.kvTracerSerialize();
        kvTracerDeserialize(traceInfo);
    } else {
        _kvTracer = NULL;
    }
    _traceInfo = other._traceInfo;
    _partitionId = other._partitionId;
    _address = other._address;
    _level = other._level;
    _tracePosFront = other._tracePosFront;
    _mutex = new ThreadMutex();
}

Tracer *Tracer::clone() {
    Tracer *tracer = new Tracer();
    if (_kvTracer != NULL) {
        tracer->_kvTracer = tracer->createKVTracer();
    }
    tracer->_traceInfo = _traceInfo;
    tracer->_partitionId = _partitionId;
    tracer->_address = _address;
    tracer->_level = _level;
    tracer->_tracePosFront = _tracePosFront;
    return tracer;
}

Tracer *Tracer::cloneWithoutTraceInfo() {
    Tracer *tracer = new Tracer();
    if (_kvTracer != NULL) {
        tracer->_kvTracer = tracer->createKVTracer();
    }
    tracer->_partitionId = _partitionId;
    tracer->_address = _address;
    tracer->_level = _level;
    return tracer;
}

bool Tracer::isKVTrace() const {
    return _kvTracer != NULL;
}

kv_tracer::UploadTracer * Tracer::getKVTracer() {
    return _kvTracer;
}

void Tracer::setLevel(int32_t level) {
    _level = level;
}

void Tracer::setLevel(const std::string &level) {
    _level = Tracer::convertLevel(level);
}

int32_t Tracer::convertLevel(const std::string &trlevel) {
    int32_t traceLevel = ISEARCH_TRACE_NONE;
    string level;
    autil::StringUtil::toUpperCase(trlevel.c_str(), level);

    if (level == FATAL) {
        traceLevel = ISEARCH_TRACE_FATAL;
    } else if (level == ERROR) {
        traceLevel = ISEARCH_TRACE_ERROR;
    } else if (level == WARN) {
        traceLevel = ISEARCH_TRACE_WARN;
    } else if (level == INFO) {
        traceLevel = ISEARCH_TRACE_INFO;
    } else if (level == DEBUG) {
        traceLevel = ISEARCH_TRACE_DEBUG;
    } else if (level == TRACE1) {
        traceLevel = ISEARCH_TRACE_TRACE1;
    } else if (level == TRACE2) {
        traceLevel = ISEARCH_TRACE_TRACE2;
    } else if (level == TRACE3) {
        traceLevel = ISEARCH_TRACE_TRACE3;
    } else if (level == ALL) {
        traceLevel = ISEARCH_TRACE_ALL;
    } else if (level == DISABLE) {
        traceLevel = ISEARCH_TRACE_DISABLE;
    } else if (level == NONE) {
        traceLevel = ISEARCH_TRACE_NONE;
    }

    return traceLevel;
}

void Tracer::trace(const char *traceInfo) {
    trace(string(traceInfo));
}

void Tracer::trace(const string &traceInfo) {
    autil::ScopedLock lock(*_mutex);
    if (isKVTrace()) {
        kvTracerInsertInfo(traceInfo, true);
    } else {
        if (_traceInfo.size() >= ISEARCH_TRACE_MAX_SIZE) {
            return;
        }
        if (!_tracePosFront) {
            _traceInfo.push_back(traceInfo);
        } else {
            _traceInfo.insert(_traceInfo.begin(), traceInfo);
        }
    }
    AUTIL_LOG(DEBUG, "trace info: %s", traceInfo.c_str());
}

void Tracer::simpleTrace(const char *format, ...) {
    char buffer[ISEARCH_TRACE_MAX_LEN];
    va_list ap;
    va_start(ap, format);
    (void)vsnprintf(buffer, ISEARCH_TRACE_MAX_LEN, format, ap);
    va_end(ap);
    trace(buffer);
}

void Tracer::rankTrace(const char *levelStr, const char *fileName, int line, const char *format,
                       ...) {
    char buffer[ISEARCH_TRACE_MAX_LEN];
    uint32_t len =
        snprintf(buffer, ISEARCH_TRACE_MAX_LEN, "[%s] [%s:%d]", levelStr, fileName, line);

    va_list ap;
    va_start(ap, format);
    (void)vsnprintf(buffer + len, ISEARCH_TRACE_MAX_LEN - len, format, ap);
    va_end(ap);
    trace(buffer);
}

void Tracer::requestTrace(const char *levelStr, const char *format, ...) {
    char __ha3_sys_reserved_buffer[ISEARCH_TRACE_MAX_LEN];
    char *buffer = __ha3_sys_reserved_buffer;
    uint32_t leftSize = ISEARCH_TRACE_MAX_LEN;
    if (!isKVTrace()) {
        int size = snprintf(buffer, leftSize, "[%-6s] [%s] [%s] [%s]", levelStr, _address.c_str(),
                            Tracer::getFormatTime().c_str(), _partitionId.c_str());
        leftSize -= size;
        buffer += size;
    }
    va_list ap;
    va_start(ap, format);
    (void)vsnprintf(buffer, leftSize, format, ap);
    va_end(ap);

    trace(__ha3_sys_reserved_buffer);
}

void Tracer::addTrace(const std::string &trace) {
    autil::ScopedLock lock(*_mutex);
    if (_traceInfo.size() >= ISEARCH_TRACE_MAX_SIZE) {
        return;
    }
    _traceInfo.push_back(trace);
}

string Tracer::getTraceInfo() const {
    autil::ScopedLock lock(*_mutex);
    if (isKVTrace()) {
        return kvTracerGetJson();
    }
    stringstream ss;
    for (vector<string>::const_iterator it = _traceInfo.begin(); it != _traceInfo.end(); ++it) {
        ss << (*it) << endl;
    }

    return ss.str();
}

void Tracer::serialize(autil::DataBuffer &dataBuffer) const {
    autil::ScopedLock lock(*_mutex);
    if (_kvTracer) {
        dataBuffer.write(true);
        string kvTrace = kvTracerSerialize();
        dataBuffer.write(kvTrace);
    } else {
        dataBuffer.write(false);
    }
    dataBuffer.write(_level);
    dataBuffer.write(_traceInfo);
    dataBuffer.write(_partitionId);
    dataBuffer.write(_address);
}

void Tracer::deserialize(autil::DataBuffer &dataBuffer) {
    bool hasKVTrace;
    dataBuffer.read(hasKVTrace);
    if (hasKVTrace) {
        deleteKVTracer(_kvTracer);
        _kvTracer = createKVTracer();
        string kvTrace;
        dataBuffer.read(kvTrace);
        kvTracerDeserialize(kvTrace);
    } else {
        _kvTracer = NULL;
    }
    dataBuffer.read(_level);
    dataBuffer.read(_traceInfo);
    dataBuffer.read(_partitionId);
    dataBuffer.read(_address);
}

std::string Tracer::toXMLString() const {
    string info = getTraceInfo();
    if (info.size() <= 0) {
        return string();
    }
    stringstream ss;
    ss << "<trace_info>" << endl;
    ss << "\t<![CDATA[" << endl;
    ss << "\t\t" << info;
    ss << "\t]]>" << endl;
    ss << "</trace_info>" << endl;
    return ss.str();
}

void Tracer::merge(Tracer *tracer) {
    autil::ScopedLock lock(*_mutex);
    if (tracer) {
        if (!tracer->_traceInfo.empty()) {
            _traceInfo.insert(_traceInfo.end(), tracer->_traceInfo.begin(),
                              tracer->_traceInfo.end());
        }
        if (tracer->_kvTracer && _kvTracer) {
            string nodeStr = tracer->_partitionId + '[' + tracer->_address + ']';
            kvTracerAppendUploadTracer(nodeStr, *(tracer->_kvTracer));
        }
    }
}

Tracer &Tracer::operator=(const Tracer &tracer) {
    if (tracer._kvTracer) {
        if (!_kvTracer) {
            _kvTracer = createKVTracer();
        }
        string traceInfo = tracer.kvTracerSerialize();
        kvTracerDeserialize(traceInfo.c_str());
    } else {
        _kvTracer = NULL;
    }
    _traceInfo = tracer._traceInfo;
    _partitionId = tracer._partitionId;
    _address = tracer._address;
    _level = tracer._level;
    _tracePosFront = tracer._tracePosFront;
    return *this;
}

string Tracer::getFormatTime() {
    timeval timev;
    timev.tv_sec = timev.tv_usec = 0;
    gettimeofday(&timev, NULL);
    time_t t = timev.tv_sec;
    tm tim;
    localtime_r(&t, &tim);
    char buf[128];
    snprintf(buf, 128, "%04d-%02d-%02d %02d:%02d:%02d.%03d", tim.tm_year + 1900, tim.tm_mon + 1,
             tim.tm_mday, tim.tm_hour, tim.tm_min, tim.tm_sec, (int)timev.tv_usec / 1000);
    return string(buf);
}

kv_tracer::UploadTracer *Tracer::createKVTracer() {
    return nullptr;
}

void Tracer::deleteKVTracer(kv_tracer::UploadTracer* kvTracer) {
}

string Tracer::kvTracerSerialize() const {
    return "";
}

void Tracer::kvTracerDeserialize(const string& traceInfo) {
}

void Tracer::kvTracerInsertInfo(const string& traceInfo, bool uniq)
{
}

string Tracer::kvTracerGetJson() const {
    return "";
}

void Tracer:: kvTracerAppendUploadTracer(const std::string& traceInfo,
                                kv_tracer::UploadTracer &kvTracer)
{
}

REGISTER_MATCHDOC_TYPE(Tracer);

}
}
