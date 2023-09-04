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
/**
*@file Appender.h
*@brief the file to declare Appender class and its children class.
*
*@version 1.0.0
*@date 2008.12.19
*@author jinhui.li
*/
#ifndef _NAVI_NAVI_APPENDER_H_
#define _NAVI_NAVI_APPENDER_H_

#include "navi/common.h"
#include "navi/log/Sync.h"
#include "navi/log/Layout.h"
#include "navi/log/LogBtFilter.h"
#include <string>
#include <list>
#include <set>
#include <map>
#include <pthread.h>
#include <stdint.h>

namespace navi
{
class LoggingEvent;
class NaviLogManager;

/**
*@class Appender
*@brief the class to represent output destination
*
*@version 1.0.0
*@date 2008.12.19
*@author jinhui.li
*@warning
*/
class Appender
{
public:
    Appender(const std::string &levelStr, NaviLogManager *manager);
    virtual ~Appender();
public:
    /**
    *@brief the actual output function.
    */
    virtual int append(LoggingEvent& event) = 0;
    /**
    *@brief flush message to destination.
    */
    virtual void flush() = 0;
    /**
    *@brief layout set function.
    */
    virtual void setLayout(Layout* layout = NULL);
    /**
    *@brief release all the static Appender object.
    *@warning can only be called in Logger::shutdown() method
    */
    static void release();
    /**
    *@brief check if this appender auto flush.
    */
    bool isAutoFlush();
    /**
    *@brief set method of m_bAutoFlush.
    *@param bAutoFlush is the set value.
    */
    void setAutoFlush(bool bAutoFlush);
    void setLevel(const std::string &levelStr);
    LogLevel getLevel() const;
protected:
    //the lock for append() function
    Mutex m_appendMutex;
    Layout* m_layout;
    bool m_bAutoFlush;
    LogLevel _level;
    NaviLogManager *_manager;
};

/**
*@class FileAppender
*@brief the appender of file type
*
*@version 1.0.0
*@date 2008.12.19
*@author jinhui.li
*@warning
*/
#define CHUNK 16384

typedef struct parameter
{
    char *fileName;
    uint64_t cacheLimit;
} param;

class FileAppender: public Appender
{
public:
    FileAppender(const std::string &levelStr,
                 NaviLogManager *manager,
                 const char * filename);
    ~FileAppender() override;
public:
    /**
    *@brief the actual output function.
    *@param level log level
    *@param message user log message
    *@param loggerName the logger's name from caller
    */
    virtual int append(LoggingEvent& event) override;
    /**
    *@brief flush message to destination.
    */
    virtual void flush() override;
    /**
    *@brief get the static FileAppender pointer.
    *@return the pointer of static FileAppender object
    */
    static Appender *getAppender(const char *filename);
    /**
    *@brief release all the static Appender object.
    *@warning can only be called in Logger::shutdown() method
    */
    static void release();
    /**
    *@brief set max file size
    */
    void setMaxSize(uint32_t maxSize);
    /**
    *@brief set log file delay time (hour)
    */
    void setDelayTime(uint32_t hour); 
    /**
    *@brief set rotated log file compress flag
    */
    void setCompress(bool bCompress); 
    /**
    *@brief set log file cache limit size
    */
    void setCacheLimit(uint32_t cacheLimit);
    /**
    *@brief set history log file keep count
    */
    void setHistoryLogKeepCount(uint32_t keepCount);
    /**
    *@brief get history log file keep count
    */
    uint32_t getHistoryLogKeepCount() const;
    /**
    *@brief remove history log file beyond 'm_keepFileCount'.
    */
    size_t removeHistoryLogFile(const char *dir);
    /**
    *@brief set method of m_bAsyncFlush.
    */
    void setAsyncFlush(bool bAsyncFlush);
    /**
    *@brief check whether async flush is enabled.
    */
    bool isAsyncFlush();
    /**
    *@brief set method of m_nFlushThreshold.
    */
    void setFlushThreshold(uint32_t nFlushThreshold);
    /**
    *@brief get flush threshold
    */
    uint32_t getFlushThreshold();
    /**
    *@brief set method of m_nFlushIntervalInMS.
    */
    void setFlushIntervalInMS(uint64_t nFlushIntervalInMS);
    /**
    *@brief get flush threshold
    */
    uint64_t getFlushIntervalInMS();
    /**
    *@brief write buf to the underlying file. 
    */
    int write(std::string &buf, uint32_t nLogLevel, bool bAsyncFlush = false);
    /**
    *@brief write and flush the pending buffer list in the I/O thread asynchronously.
    */
    int64_t asyncFlush(int64_t now, bool force);

    //only for test.
    void setMaxSizeByBytes(int bytes) { m_nMaxSize = bytes;}

    void addBtFilter(const LogBtFilterParam &param);
public:
    static const uint32_t MAX_KEEP_COUNT = 1024; 
    static const uint32_t MAX_CACHED_BUF_SIZE = 200*1024*1024; // 200M
    static const uint32_t DEFAULT_FLUSH_THRESHOLD_IN_KB = 1024; // 1K
    static const uint64_t DEFAULT_FLUSH_INTERVAL_IN_MS = 1000; // 1S
private:
    int open();
    int close();
    void rotateLog();
    void computeLastLogFileName(char *lastLogFileName, size_t length);
    void compressLog(char *logFileName);
    static void* compressHook(void *p);
    static std::string getParentDir(const std::string &fileName);
    static std::string getPureFileName(const std::string &fileName);
    static void freeNameList(struct dirent **namelist, int32_t entryCount);
    static int createDir(const std::string &dirPath);
        
    FILE* m_file;
    std::string m_fileName;
    std::string m_patentDir;
    std::string m_pureFileName;
    time_t m_delayTime;
    uint32_t m_nMaxDelayTime;
    uint32_t m_keepFileCount;
    uint64_t m_nCurSize;
    uint64_t m_nMaxSize;
    uint64_t m_nPos;
    uint64_t m_nCacheLimit;
    bool m_bCompress;

    bool m_bAsyncFlush;
    uint32_t m_nFlushThreshold;
    uint32_t m_nAcculatedBytes;
    uint32_t m_nAcculatedBytesSinceLastSignal;
    uint32_t m_nFlushIntervalInMS;
    uint64_t m_nLastFlushTimeInMS;

    LogBtFilter m_btFilter;

    Mutex m_pendingBufferListMutex;
    std::list<std::string> *m_pPendingBufferList;
    std::list<std::string> *m_pBufferList;
    volatile bool m_needFlush;
};

}
#endif

