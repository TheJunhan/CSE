#ifndef TPRINTF_H
#define TPRINTF_H

#define tprintf(args...) do { \
        struct timeval tv;     \
        gettimeofday(&tv, 0); \
        printf("%ld:\t", tv.tv_sec * 1000 + tv.tv_usec / 1000);\
        printf(args);   \
        } while (0);
#endif
// #ifndef TPRINTF_H
// #define TPRINTF_H

// #define tprintf(args...) do { \
//         struct timeval tv;     \
//         gettimeofday(&tv, 0); \
//         printf("Line %d ",__LINE__);           \
//         printf("%ld:\t", tv.tv_sec * 1000 + tv.tv_usec / 1000);\
//         printf(args);   \
//         } while (0);
// #endif