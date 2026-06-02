#include "LogFile.h"
#include "FileUtil.h"

#include <assert.h>
#include <stdio.h>
#include <time.h>

using namespace reactor;

LogFile::LogFile(const string& basename,
                off_t rollsize,
                bool threadSafe,
                int flushInterval,
                int checkEveryN)
    : basename_(basename),
      rollSize_(rollsize),
      flushInterval_(flushInterval),
      checkEveryN_(checkEveryN),
      count_(0),
      mutex_(threadSafe ? new MutexLock : NULL),
      startOfPeriod(0),
      lastRoll_(0),
      lastFlush_(0)
{
    assert(basename_.find('/') == string::npos);
    rollFile();
}

LogFile::~LogFile() = default;

void LogFile::append(const char* logfile, int len)
{
    if (mutex_)
    {
        MutexLockGuard lock(*mutex_);
        append_unlock(logfile, len);
    }   
    else
    {
        append_unlock(logfile, len);
    }
}

void LogFile::flush()
{
    if (mutex_)
    {
        MutexLockGuard lock(*mutex_);
        file_->flush();
    }
    else
    {
        file_->flush();
    }
}

void LogFile::append_unlock(const char* logline, int len)
{
    file_->append(logline, len);
    if (file_->writtenBytes() >= rollSize_)
    {
        rollFile();
    }
    else
    {
        count_++;
        if (count_ >= checkEveryN_)
        {
            count_ = 0;

            //利用整数除法截断的特性，求出当前所在时间的零点所对应的秒数
            time_t now = ::time(NULL);
            time_t thisPeriod = now / kRollPerSeconds_ * kRollPerSeconds_; 
            
            if (thisPeriod > startOfPeriod)
            {
                rollFile();
            }
            else if(now - lastFlush_ >= flushInterval_)
            {
                lastFlush_ = now;
                file_->flush();
            }
        }
    }
}


bool LogFile::rollFile()
{
    time_t now = 0;
    string filename = getLogFileName(basename_, &now);
    time_t start = now / kRollPerSeconds_ * kRollPerSeconds_;

    if (now > lastRoll_)
    {
        lastRoll_ = now;
        lastFlush_ = now;
        startOfPeriod = start;
        file_.reset(new FileUtil::AppendFile(filename));  //获得新的指针
        return true;
    }
    return false;
}

string LogFile::getLogFileName(const string& basename, time_t *now)
{
    string filename;
    filename.reserve(basename.length() + 64);
    filename = basename;

    char timebuf[32];
    struct tm tm_t;
    *now = ::time(NULL);
    localtime_r(now, &tm_t);
    strftime(timebuf, sizeof timebuf, ".%Y-%m-%d %H:%M:%S", &tm_t);
    filename += timebuf;

    filename += ".log";

    return filename;
}