#include "CurrentThread.h"

#include <cxxabi.h>
#include <execinfo.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <string>
#include <string.h>

namespace reactor
{
    namespace CurrentThread
    {
        __thread int t_cachedTid = 0;
        __thread char t_tidString[32];
        __thread int t_tidStringLength;
        __thread const char *t_threadName = "unknown";
        static_assert(std::is_same<int, pid_t>::value, "pid should be int");

        std::string stackTrace(bool demangle)
        {
            std::string stack;
            const int max_frames = 200;
            void* frame[max_frames];
            int nptrs = ::backtrace(frame, max_frames);
            char** strings = ::backtrace_symbols(frame, max_frames);  //The array of string pointers is allocated via malloc, and must be freed by the caller.

            if (strings)
            {
                size_t len = 256;
                //提前分配好内存，abi::__cxa_demangle的时候，不用反复malloc
                char* demangled = demangle ? static_cast<char*>(::malloc(len)) : nullptr;
                for (int i = 1; i < nptrs; i++)
                {
                    if (demangle)
                    {
                        char* left_par = nullptr;
                        char* plus = nullptr;
                        for (char* p = strings[i]; *p; p++)
                        {
                            if (*p == '('){
                                left_par = p;
                            }
                            if (*p == '+'){
                                plus = p;
                            }
                        }

                        if (left_par && plus){
                            *plus = '\0';  //给abi::__cxa_demangle()准备字符串
                            int status = 0;
                            char* ret = abi::__cxa_demangle(left_par + 1, demangled, &len, &status);
                            *plus = '+';
                            if (status == 0){
                                demangled = ret;
                                stack.append(strings[i], left_par + 1);
                                stack.append(demangled);
                                stack.append(plus);
                                stack.push_back('\n');
                                continue;
                            }
                        }
                    }
                    stack.append(strings[i]);
                    stack.push_back('\n');
                }
                //释放内存
                free(demangled); 
                free(strings);   
            }
            return stack;
        }
        void cacheTid()
        {
            int gettid = static_cast<pid_t>(::syscall(SYS_gettid));
            if (t_cachedTid == 0)
            {
                t_cachedTid = gettid;
                t_tidStringLength = snprintf(t_tidString, sizeof t_tidString, "%5d", t_cachedTid);
            }
        }

    }
}