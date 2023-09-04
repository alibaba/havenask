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
#include <time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <syslog.h>
#include <stdlib.h>
#include <zlib.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include <algorithm>
#include <iosfwd>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "alog/Appender.h"
#include "alog/Logger.h"
#include "EventBase.h"
#include "LoggingEvent.h"
#include "Clock.h"
#include "aios/alog/src/cpp/Sync.h"
#include "alog/Layout.h"

namespace alog {

using namespace std;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Appender
///////////////////////////////////////////////////////////////////////////////////////////////////////////
std::set<Appender *> Appender :: s_appenders;

Appender::Appender()
{
    m_appendMutex.reset(new Mutex);
    m_layout = new BasicLayout();
    m_bAutoFlush = false;
}

Appender::~Appender()
{
    if(m_layout) {
    	delete m_layout;
        m_layout = NULL;
    }
}

void Appender::setLayout(Layout* layout)
{
    if (layout != m_layout)
    {
        Layout *oldLayout = m_layout;
        m_layout = (layout == NULL) ? new BasicLayout() : layout;
        if(oldLayout)
            delete oldLayout;
    }
}

void Appender::release()
{
    for (std::set<Appender *>::iterator i = s_appenders.begin(); i != s_appenders.end(); i++)
    {
        if (NULL != *i)
            delete (*i);
    }
    s_appenders.clear();
    ConsoleAppender::release();
    FileAppender::release();
    SyslogAppender::release();
}

bool Appender::isAutoFlush()
{
    return m_bAutoFlush;
}

void Appender::setAutoFlush(bool bAutoFlush)
{
    m_bAutoFlush = bAutoFlush;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ConsoleAppender
///////////////////////////////////////////////////////////////////////////////////////////////////////////
Appender* ConsoleAppender::s_appender = NULL;
unique_ptr<Mutex> ConsoleAppender::s_appenderMutex(new Mutex);

Appender* ConsoleAppender::getAppender()
{
    ScopedLock lock(*s_appenderMutex);
    if (!s_appender)
    {
        s_appender = new ConsoleAppender();
        s_appenders.insert(s_appender);
    }
    return s_appender;
}

int ConsoleAppender::append(LoggingEvent& event)
{
    if(m_layout == NULL)
    	m_layout = new BasicLayout();
    std::string formatedStr = m_layout->format(event);
    size_t wCount = 0;
    {
        ScopedLock lock(*m_appendMutex);
        wCount = fwrite(formatedStr.data(), 1, formatedStr.length(), stdout);
        if (m_bAutoFlush || event.level < LOG_LEVEL_INFO)
            fflush(stdout);
    }
    return (wCount == formatedStr.length())? formatedStr.length() : 0;
}

void ConsoleAppender::flush()
{
    fflush(stdout);
}

void ConsoleAppender::release()
{
    s_appender = NULL;
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////
// FileAppender
///////////////////////////////////////////////////////////////////////////////////////////////////////////
std::map<std::string, Appender *>  FileAppender::s_fileAppenders;
unique_ptr<Mutex> FileAppender::s_appenderMapMutex(new Mutex);
const int32_t FILE_NAME_MAX_SIZE = 511;
const char DIR_DELIM = '/';

FileAppender::FileAppender(const char * fileName) : Appender(), m_fileName(fileName)
{
    m_delayTime = 0;
    m_nMaxDelayTime = 0;
    m_keepFileCount = 0;
    m_nMaxSize = 0;
    m_nCurSize = 0;
    m_bCompress = false;
    m_file = NULL;
    m_nPos = 0;
    m_nCacheLimit = 0;
    m_patentDir = getParentDir(m_fileName);
    m_pureFileName = getPureFileName(m_fileName);


    m_bAsyncFlush = false;
    m_nFlushThreshold = DEFAULT_FLUSH_THRESHOLD_IN_KB * 1024;
    m_nFlushIntervalInMS = DEFAULT_FLUSH_INTERVAL_IN_MS;
    m_nAcculatedBytes = 0;
    m_nAcculatedBytesSinceLastSignal = 0;

    m_nLastFlushTimeInMS = Clock::nowMS();

    m_pPendingBufferList = new std::list<std::string>();
    m_pBufferList = new std::list<std::string>();

    createDir(m_patentDir);
    open();
    m_needFlush = false;
    if(m_layout == NULL)
        m_layout = new BasicLayout();
}

FileAppender::~FileAppender()
{
    delete m_pPendingBufferList;
    delete m_pBufferList;

    if (m_file)
        close();
    m_file = NULL;
}

Appender* FileAppender::getAppender(const char *filename)
{
    std::string file = filename;
    ScopedLock lock(*s_appenderMapMutex);
    std::map<std::string, Appender *>::iterator i = s_fileAppenders.find(file);
    if (s_fileAppenders.end() == i)
    {
        Appender *newAppender = new FileAppender(filename);
        s_fileAppenders[file] = newAppender;
        s_appenders.insert(newAppender);
        return newAppender;
    }
    else
    {
        return (*i).second;
    }
}

int FileAppender::append(LoggingEvent& event)
{
    std::string formatedStr = m_layout->format(event);

    if (m_bAsyncFlush == false)
    {
        ScopedLock lockGuard(*m_appendMutex);
        return write(formatedStr, event.level);
    }

    uint64_t nLen = formatedStr.length();
    if (nLen > 0)
    {
        ScopedLock lockGuard(*m_appendMutex);
        // If for whatever reason the cached bytes have exceeded the limit,
        // we'll drop the log entry immediately for security concern.
        // Note that this is *NOT* supposed to happen!
        if (m_nAcculatedBytes >= MAX_CACHED_BUF_SIZE)
        {
            return 0;
        }

        m_pPendingBufferList->push_back(formatedStr);
        m_nAcculatedBytes += nLen;
        m_nAcculatedBytesSinceLastSignal += nLen;

        if (m_nAcculatedBytes >= MAX_CACHED_BUF_SIZE)
        {
            fprintf(stderr,
                    "The cached buffer size (%u) has exceed the limit: %u, logStr[%s]\n",
                    m_nAcculatedBytes,
                    MAX_CACHED_BUF_SIZE,
                    formatedStr.c_str());
        }

        if ((m_nAcculatedBytes >= m_nFlushThreshold) &&
            (m_nAcculatedBytesSinceLastSignal >= m_nFlushThreshold))
        {
            m_needFlush = true;
            gEventBase.notify();
            m_nAcculatedBytesSinceLastSignal = 0;
        }
    }
    return nLen;
}

int64_t FileAppender::asyncFlush(int64_t now, bool force)
{
    bool needFlush = force || m_needFlush;
    if (!needFlush) {
        int64_t nextFlushTime = m_nLastFlushTimeInMS + m_nFlushIntervalInMS;
        if (now >= nextFlushTime) {
            needFlush = true;
        } else {
            return nextFlushTime;
        }
    }

    // Swap out the pending buffer list for write and flush
    std::list<std::string> *pTempBufferList = NULL;
    {
        ScopedLock lockGuard(*m_appendMutex);
        pTempBufferList = m_pBufferList;
        m_pBufferList = m_pPendingBufferList;
        m_pPendingBufferList = pTempBufferList;

        m_nAcculatedBytes = 0;
        m_nAcculatedBytesSinceLastSignal = 0;
        m_needFlush = false;
    }

    std::list<std::string>::iterator itor = m_pBufferList->begin();
    for (; itor != m_pBufferList->end(); ++itor)
    {
        // Here the log level isn't needed actually, since we'll always
        // do the flush operation later.
        write(*itor, LOG_LEVEL_INFO, true);
    }

    //rotate file may fail.
    if (m_file)
        fflush(m_file);

    m_nLastFlushTimeInMS = Clock::nowMS();

    // Clear the buffer list for usage in the next time.
    m_pBufferList->clear();
    return m_nLastFlushTimeInMS + m_nFlushIntervalInMS;
}

// Note that this method should be called under *m_appendMutex, or
// in the dedicated I/O worker thread asynchronously.
int FileAppender::write(std::string &buf, uint32_t nLogLevel, bool bAsyncFlush)
{
    size_t wCount = 0;
    if((m_nMaxSize > 0 && m_nCurSize + buf.length() >= m_nMaxSize) ||
       (m_delayTime > 0 && time(NULL) >= m_delayTime))
    {
        rotateLog();
        //m_file may always is non-NULL, here we lost log.
        if (m_file == NULL) {
            fprintf(stderr, "rotate may fail, log is lost:%s\n", buf.c_str());
            return 0;
        }
    }

    //when switch logfile open may fail, so need open again.
    if(!m_file && open() == 0) {
        fprintf(stderr, "log lost for open file fail:%s\n", buf.c_str());
        return 0;
    }

    wCount = fwrite(buf.data(), 1, buf.length(), m_file);

    // We won't flush the file when async flush is enabled for each buffer
    // write, since we'll flush the file when all the ready buffers are
    // written in one go.
    if (bAsyncFlush == false)
    {
        if (m_bAutoFlush || m_nMaxSize > 0 || nLogLevel < LOG_LEVEL_INFO)
            fflush(m_file);
    }

    m_nCurSize += wCount;
    if(m_nCacheLimit > 0 && (m_nCurSize - m_nPos) > m_nCacheLimit)
    {
        posix_fadvise(fileno(m_file), 0, 0, POSIX_FADV_DONTNEED);
        m_nPos = m_nCurSize;
    }

    return (wCount == buf.length())? buf.length() : 0;
}

void FileAppender::flush()
{
    gEventBase.flushFile(this);
}

int FileAppender :: open()
{
    m_file = fopen64(m_fileName.c_str(), "a+");
    if (m_file)
    {
        struct stat64 stbuf;
        if (fstat64(fileno(m_file), &stbuf) == 0)
        {
            m_nCurSize = stbuf.st_size;
            m_nPos = 0;
        }
        return 1;
    }
    else
    {
        fprintf(stderr, "Open log file error : %s\n",  m_fileName.c_str());
        return 0;
    }
}

void FileAppender::release()
{
    s_fileAppenders.clear();
}

int FileAppender :: close()
{
    int ret = -1;
    if(m_file)
        ret = fclose(m_file);
    m_file = NULL;
    return ret;
}

int FileAppender::createDir(const string &dirPath) {
    string parentDir = getParentDir(dirPath);
    if (parentDir != "." && access(parentDir.c_str(), F_OK) < 0) {
        if (0 != createDir(parentDir)) {
            return 1;
        }
    }
    if (mkdir(dirPath.c_str(), 0755) < 0) {
        if (errno != EEXIST) {
            fprintf(stderr, "mkdir failed: %s\n", dirPath.c_str());
            return 1;
        }
    }
    return 0;
}

void FileAppender :: rotateLog()
{
    if(m_nCacheLimit > 0)
        posix_fadvise(fileno(m_file), 0, 0, POSIX_FADV_DONTNEED);
    close();

    char *lastLogFileName = new char[FILE_NAME_MAX_SIZE + 1];
    computeLastLogFileName(lastLogFileName, FILE_NAME_MAX_SIZE);
    rename(m_fileName.c_str(), lastLogFileName);

    if(m_delayTime > 0 && time(NULL) >= m_delayTime) {
        setDelayTime(m_nMaxDelayTime);
    }
    if (m_keepFileCount > 0) {
        removeHistoryLogFile(m_patentDir.c_str());
    }
    if(m_bCompress) {
        compressLog(lastLogFileName);
    }
    else
      delete []lastLogFileName;

    open();
}

void FileAppender :: computeLastLogFileName(char *lastLogFileName,
					    size_t length)
{
    char oldLogFile[FILE_NAME_MAX_SIZE + 1];
    oldLogFile[FILE_NAME_MAX_SIZE] = '\0';
    char oldZipFile[FILE_NAME_MAX_SIZE + 1];
    oldZipFile[FILE_NAME_MAX_SIZE] = '\0';

    time_t t;
    time(&t);
    struct tm tim;
    ::localtime_r((const time_t*)&t, &tim);

    snprintf(oldLogFile, FILE_NAME_MAX_SIZE, "%s.%04d%02d%02d%02d%02d%02d",
	     m_fileName.c_str(), tim.tm_year+1900, tim.tm_mon+1, tim.tm_mday,
	     tim.tm_hour, tim.tm_min, tim.tm_sec);
    int32_t suffixStartPos = strlen(oldLogFile);
    int32_t suffixCount = 1;
    do {
        snprintf(oldLogFile + suffixStartPos, FILE_NAME_MAX_SIZE - suffixStartPos,
		 "-%03d", suffixCount);
        snprintf(oldZipFile, FILE_NAME_MAX_SIZE, "%s.gz", oldLogFile);
        suffixCount++;
    } while(access(oldLogFile, R_OK) == 0 || access(oldZipFile, R_OK) == 0);

    snprintf(lastLogFileName, length, "%s", oldLogFile);
}

size_t FileAppender :: removeHistoryLogFile(const char *dirName)
{
    size_t removedFileCount = 0;
    struct dirent **namelist = NULL;
    int32_t entryCount = scandir(dirName, &namelist, NULL, alphasort);
    if (entryCount < 0) {
        fprintf(stderr, "scandir error, errno:%d!\n", errno);
	return 0 ;
    } else if (entryCount <= (int32_t)m_keepFileCount) {
        freeNameList(namelist, entryCount);
	return 0;
    }

    //filter by entry's name and entry's st_mode, remain all history log file
    std::vector<std::string> filteredEntrys;
    size_t prefixLen = m_pureFileName.size();
    for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
      const struct dirent *logEntry = namelist[entryIndex];
      assert(logEntry);
      if (0 == strncmp(m_pureFileName.c_str(), logEntry->d_name, prefixLen)
	  && (size_t)strlen(logEntry->d_name) > prefixLen)
      {
	    std::string logFileName = std::string(dirName) + std::string("/") + std::string(logEntry->d_name);
	    struct stat st;
	    errno = 0;
	    int statRet = lstat(logFileName.c_str(), &st);
	    //	    fprintf(stdout, "statRet:%d, S_ISREG(st.st_mode):%d, errno:%d\n",
	    //		    statRet, S_ISREG(st.st_mode), errno);
	    if (!statRet && S_ISREG(st.st_mode)) {
	        //correct log file
	        filteredEntrys.push_back(logEntry->d_name);
	    }
	}
    }

    //fprintf(stdout, "filteredEntrys.size():%zd\n", filteredEntrys.size());
    if (filteredEntrys.size() > m_keepFileCount)
    {
        sort(filteredEntrys.begin(), filteredEntrys.end()); //sort by string's lexical order
        removedFileCount = filteredEntrys.size() - m_keepFileCount;
        filteredEntrys.erase(filteredEntrys.begin() + removedFileCount, filteredEntrys.end());

        std::string trashDir = Logger::getTrashDir();

        //actually remove the history log file beyond the limit of 'm_keepFileCount'
        for (std::vector<std::string>::const_iterator it = filteredEntrys.begin();
                it != filteredEntrys.end(); ++it)
        {
            std::string fileName = std::string(dirName) + "/" + it->c_str();
            if (trashDir == "")
                unlink(fileName.c_str());
            else {
                char buf[32];
                snprintf(buf, 31, ".%d", (int)time(NULL));
                std::string dest = trashDir + "/" + *it + std::string(buf);
                int err = rename(fileName.c_str(), dest.c_str());
                if (err != 0) {
                    fprintf(stderr, "rename %s to %s fail, errno:%d\n"
                        , fileName.c_str(), dest.c_str(), errno);
                    unlink(fileName.c_str());
                }
            }
        }
    }

    freeNameList(namelist, entryCount);
    return removedFileCount;
}

void FileAppender::freeNameList(struct dirent **namelist, int32_t entryCount)
{
    if(namelist == NULL) {
      return;
    }
    //delete the dirent
    for (int32_t i = 0; i < entryCount; i++) {
        free(namelist[i]);
    }
    free(namelist);
}


void FileAppender :: setMaxSize(uint32_t maxSize)
{
    m_nMaxSize = (uint64_t)maxSize * 1024 * 1024;
}

void FileAppender :: setCacheLimit(uint32_t cacheLimit)
{
    m_nCacheLimit = (uint64_t)cacheLimit * 1024 * 1024;
}

void FileAppender :: setHistoryLogKeepCount(uint32_t keepCount)
{
    m_keepFileCount = keepCount;
}

uint32_t FileAppender :: getHistoryLogKeepCount() const
{
    return m_keepFileCount;
}

string FileAppender::getParentDir(const string &fileName)
{
  if(fileName.empty()) {
    return std::string(".");
  }
  size_t delimPos = fileName.rfind(DIR_DELIM, fileName.length() - 2);
  if(std::string::npos == delimPos) {
    return std::string(".");
  } else {
    return fileName.substr(0, delimPos + 1);
  }
}

string FileAppender :: getPureFileName(const string &fileName)
{
  size_t delimPos = fileName.rfind(DIR_DELIM);
  if(std::string::npos == delimPos) {
    return fileName;
  } else {
    return fileName.substr(delimPos + 1);
  }
}

void FileAppender :: setDelayTime(uint32_t delayTime)
{
    m_nMaxDelayTime = delayTime;
    if(delayTime > 0)
        m_delayTime = time(NULL) + m_nMaxDelayTime;
    else
        m_delayTime = 0;
}

void FileAppender :: setCompress(bool bCompress)
{
    m_bCompress = bCompress;
}

bool FileAppender::isAsyncFlush()
{
    return m_bAsyncFlush;
}

void FileAppender::setAsyncFlush(bool bAsyncFlush)
{
    m_bAsyncFlush = bAsyncFlush;
}

void FileAppender::setFlushThreshold(uint32_t nFlushThreshold)
{
    m_nFlushThreshold = nFlushThreshold;
}

uint32_t FileAppender::getFlushThreshold()
{
    return m_nFlushThreshold;
}

void FileAppender::setFlushIntervalInMS(uint64_t nFlushIntervalInMS)
{
    m_nFlushIntervalInMS = nFlushIntervalInMS;
}

uint64_t FileAppender::getFlushIntervalInMS()
{
    return m_nFlushIntervalInMS;
}

void FileAppender :: compressLog(char *logFileName)
{
    param *p = new param();
    p->fileName = logFileName;
    p->cacheLimit = m_nCacheLimit;

    pthread_t pThread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&pThread, &attr, FileAppender::compressHook, (void*)p);
    pthread_attr_destroy(&attr);
}

void* FileAppender :: compressHook(void *p)
{
    char* fileName = ((param*)p)->fileName;
    uint64_t cacheLimit = ((param*)p)->cacheLimit;
    int source;
    gzFile gzFile;
    struct stat64 stbuf;
    char *in = NULL;
    void *tmp = NULL;
    char gzFileName[512];
    ssize_t leftSize = 0;
    sprintf(gzFileName, "%s.gz",fileName);
    int flags = O_LARGEFILE;
    flags = flags | O_RDONLY;
    if(cacheLimit > 0)
        flags = flags | O_DIRECT;
    source = ::open(fileName, flags, 0644);
    if(source == -1)
    {
        fprintf(stderr, "Compress log file %s error: open file failed!\n", fileName);
        delete [] fileName;
        delete (param*)p;
        return NULL;
    }
    if (fstat64(source, &stbuf) != 0)
    {
        fprintf(stderr, "Compress log file %s error: access file info error!\n", fileName);
        ::close(source);
        delete [] fileName;
        delete (param*)p;
        return NULL;
    }
    leftSize = stbuf.st_size;
    gzFile = gzopen(gzFileName, "wb");
    if(!gzFile)
    {
        fprintf(stderr, "Compress log file %s error: open compressed file failed!\n", gzFileName);
        ::close(source);
        delete []fileName;
        delete (param*)p;
        return NULL;
    }
    if(cacheLimit > 0)
    {
        if(posix_memalign(&tmp, stbuf.st_blksize, CHUNK) != 0)
        {
            fprintf(stderr, "posix_memalignes error: %s\n", strerror(errno));
            ::close(source);
            delete []fileName;
            delete (param*)p;
            return NULL;
        }
        in = static_cast<char *>(tmp);
    }
    else
    {
        tmp = (void *)malloc(CHUNK);
        if(tmp == NULL)
        {
            fprintf(stderr, "malloc mem error: %s\n", strerror(errno));
            ::close(source);
            delete []fileName;
            delete (param*)p;
            return NULL;
        }
        in = static_cast<char *>(tmp);
    }
    while(leftSize > 0)
    {
        ssize_t readLen = ::read(source, in, CHUNK);
        if(readLen < 0)
        {
            fprintf(stderr, "Read log file %s error: %s\n", fileName, strerror(errno));
            break;
        }
        ssize_t writeLen = gzwrite(gzFile, in, readLen);
        if(readLen != writeLen)
        {
            fprintf(stderr, "Compress log file %s error: gzwrite error!\n", gzFileName);
            break;
        }
        if(leftSize < writeLen)
            leftSize = 0;
        else
            leftSize -= writeLen;
    }
    ::close(source);
    gzclose(gzFile);
    unlink(fileName);
    delete []fileName;
    delete (param*)p;
    if(tmp)
        free(tmp);
    if(cacheLimit > 0)
    {
        int fd = ::open(gzFileName, O_LARGEFILE | O_RDONLY, 0644);
        if(fd == -1)
        {
            fprintf(stderr, "Open %s error: %s!\n", gzFileName, strerror(errno));
        }
        else
        {
            if(posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0)
                fprintf(stderr, "Clear cache %s error: %s!\n", gzFileName, strerror(errno));
            ::close(fd);
        }
    }
    return NULL;
}



///////////////////////////////////////////////////////////////////////////////////////////////////////////
// SyslogAppender
///////////////////////////////////////////////////////////////////////////////////////////////////////////
std::map<std::string, Appender *>  SyslogAppender::s_syslogAppenders;
unique_ptr<Mutex> SyslogAppender::s_appenderMapMutex(new Mutex);

SyslogAppender :: SyslogAppender(const char *ident,  int facility) :
        m_ident(ident), m_facility(facility)
{
    open();
}

SyslogAppender :: ~SyslogAppender()
{
    close();
}

Appender* SyslogAppender :: getAppender(const char *ident, int facility)
{
    std::string name = ident;
    ScopedLock lock(*s_appenderMapMutex);
    std::map<std::string, Appender *>::iterator i = s_syslogAppenders.find(name);
    if (s_syslogAppenders.end() == i)
    {
        Appender *newAppender = new SyslogAppender(ident, facility);
        s_syslogAppenders[name] = newAppender;
        s_appenders.insert(newAppender);
        return newAppender;
    }
    else
    {
        return (*i).second;
    }
}

int SyslogAppender :: append(LoggingEvent& event)
{
    if (event.level >= alog::LOG_LEVEL_COUNT)
        return -1;
    if(m_layout == NULL)
    	m_layout = new BasicLayout();
    std::string formatedStr = m_layout->format(event);
    ScopedLock lock(*m_appendMutex);
    int priority = toSyslogLevel(event.level);
    syslog(priority | m_facility, "%s", formatedStr.c_str());
    return 0;
}

void SyslogAppender::flush()
{
    return;
}

int SyslogAppender :: open()
{
    openlog(m_ident.c_str(), LOG_PID, m_facility);
    return 0;
}

int SyslogAppender :: close()
{
    closelog();
    return 0;
}

void SyslogAppender::release()
{
    s_syslogAppenders.clear();
}

int SyslogAppender :: toSyslogLevel(uint32_t level)
{
    int result;

    if (level <= 1)
    {
        result = LOG_EMERG;
    }
    else if (level == 2)
    {
        result = LOG_ERR;
    }
    else if (level == 3)
	{
        result = LOG_WARNING;
	}
    else if (level == 4)
    {
        result = LOG_INFO;
    }
    else
    {
        result = LOG_DEBUG;
    }

    return result;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// PipeAppender
///////////////////////////////////////////////////////////////////////////////////////////////////////////
std::map<std::string, Appender *>  PipeAppender::s_pipeAppenders;
unique_ptr<Mutex> PipeAppender::s_appenderMapMutex(new Mutex);

PipeAppender::PipeAppender(const char* fileName) : m_fileName(fileName)
{
    m_fileno = -1;
    delete m_layout;
    m_layout = NULL;
    open();
}

int PipeAppender::open()
{
    struct stat file_stat;
    bool exist = stat(m_fileName.c_str(), &file_stat) == 0;
    if (exist) {
        if (!S_ISFIFO(file_stat.st_mode)) {
            fprintf(stderr, "file %s already exist and is not fifo file\n", m_fileName.c_str());
            return 0;
        }
    } else {
        int ret = mkfifo(m_fileName.c_str(), 0666);
        if (ret < 0) {
            fprintf(stderr, "Mkfifo file %s error : %s\n", m_fileName.c_str(), strerror(errno));
            return 0;
        }
    }

    int fd = ::open(m_fileName.c_str(), O_RDWR|O_NONBLOCK);
    if(fd < 0){
        fprintf(stderr, "Open log file %s error : %s\n",  m_fileName.c_str(), strerror(errno));
        return 0;
    }

    m_fileno = fd;
    return 1;
}

int PipeAppender::close()
{
    if(m_fileno != -1)
        ::close(m_fileno);
    m_fileno = -1;
    return 0;
}

PipeAppender::~PipeAppender()
{
    close();
}

void PipeAppender::flush()
{
    return;
}

Appender* PipeAppender::getAppender(const char *filename)
{
    std::string file = filename;
    ScopedLock lock(*s_appenderMapMutex);
    std::map<std::string, Appender *>::iterator i = s_pipeAppenders.find(file);
    if (s_pipeAppenders.end() == i)
    {
        Appender *newAppender = new PipeAppender(filename);
        s_pipeAppenders[file] = newAppender;
        s_appenders.insert(newAppender);
        return newAppender;
    }
    else
    {
        return (*i).second;
    }
}

int PipeAppender::append(LoggingEvent& event)
{
    if(m_fileno == -1)
        return 0;
    if(m_layout == NULL)
        m_layout = new BasicLayout();
    std::string formatedStr = m_layout->format(event);

    int wSize = formatedStr.length();
    int wCount = write(m_fileno, formatedStr.c_str(), wSize);
    return wCount;
}

}
