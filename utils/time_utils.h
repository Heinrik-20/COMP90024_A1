#ifndef TREE_UTILS_H
#define TREE_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define DATE_STR_LEN 9 // Add one for the null-byte
#define HOUR_STR_LEN 18 // Add one for the null-byte

typedef struct
{
    int year;
    int month;
    int day;

    int hour;
    int minute;
    int seconds;
} time_struct_t;

void clear_and_realloc(void **ptr, size_t alloc_size);
void free_time_struct(time_struct_t **time_struct);
time_struct_t *make_time(const char* buf);
char **time_struct_to_str(time_struct_t *time);

#endif