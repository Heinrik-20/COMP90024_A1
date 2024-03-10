#ifndef DATA_H
#define DATA_H

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <sys/types.h>
#include "time_utils.h"

#define MAX_CHAR 10000

typedef struct data_struct {

    time_struct_t * time;
    long double sentiment;
    struct data_struct *next;

} data_struct;


typedef struct {
    time_struct_t *sentiment_time;
    long double sentiment;
    
    time_struct_t *happy_hour_time;
    long double happy_hour_sentiment;

    time_struct_t *active_hour_time;
    int most_tweets_hour_count;

    time_struct_t *active_day_time;
    int most_tweets_day_count;

} ret_struct;

ssize_t getline_clean(char **line_ptr, size_t *n, FILE *stream);
data_struct *alloc_data_struct();
data_struct *read_data(FILE *fp, int max_reads);
void free_data_struct_list(data_struct **list);

#endif
