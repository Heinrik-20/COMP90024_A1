#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <sys/types.h>
#include "time_utils.h"

#define MAX_CHAR 10000

typedef struct {

    time_t * time;
    long double sentiment;
    data_struct *next;

} data_struct;


typedef struct {
    time_t *sentiment_time;
    long double sentiment;
    
    time_t *happy_hour_time;
    long double happy_hour_sentiment;

    time_t *active_hour_time;
    int most_tweets_hour_count;

    time_t *active_day_time;
    int most_tweets_day_count;

} ret_struct;

ssize_t getline_clean(char **line_ptr, size_t *n, FILE *stream);

#endif
