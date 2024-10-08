#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <ctype.h>
#include <string.h>
#include "time_utils.h"

void clear_and_realloc(void **ptr, size_t alloc_size) {
    free(*ptr);
    *ptr = NULL;
    assert(*ptr == NULL);
    if (alloc_size > 0) {
        *ptr = malloc(sizeof(char) * alloc_size);
    } 
    return;
}

void free_time_struct(time_struct_t **time_struct) {
    free(*time_struct);
    *time_struct = NULL;
    return;
}

time_struct_t *make_time(const char* buf) {

    time_struct_t *new_time = (time_struct_t *) malloc(sizeof(time_struct_t));
    assert(new_time);
    char* int_storage = (char *) malloc(sizeof(char) * 5);

    for (int i=0; i < strlen(buf); i++) {
        if (i < 4) {
            int_storage[i % 4] = buf[i];
            if (i == 3) {
                int_storage[4] = '\0';
                new_time->year = atoi(int_storage);
                clear_and_realloc((void *)(&int_storage), 3);
            }
        }
        else if ((i >= 5) && (i < 7)) {
            int_storage[(i-5) % 2] = buf[i];
            if (i == 6) {
                int_storage[2] = '\0';
                new_time->month = atoi(int_storage);
                clear_and_realloc((void *)(&int_storage), 3);
            }
        }
        else if ((i >= 8) && (i < 10)) {
            int_storage[(i-8) % 2] = buf[i];
            if (i == 9) {
                int_storage[2] = '\0';
                new_time->day = atoi(int_storage);
                clear_and_realloc((void *)(&int_storage), 3);
            }
        }
        else if ((i >= 11) && (i < 13)) {
            int_storage[(i-11) % 2] = buf[i];
            if (i == 12) {
                int_storage[2] = '\0';
                new_time->hour = atoi(int_storage);
                clear_and_realloc((void *)(&int_storage), 3);
            }
        }
        else if ((i >= 14) && (i < 16)) {
            int_storage[(i-14) % 2] = buf[i];
            if (i == 15) {
                int_storage[2] = '\0';
                new_time->minute = atoi(int_storage);
                clear_and_realloc((void *)(&int_storage), 3);
            }
        }
        else if ((i >= 17) && (i < 19)){
            int_storage[(i-17) % 2] = buf[i];
            if (i == 18) {
                int_storage[2] = '\0';
                new_time->seconds = atoi(int_storage);
                clear_and_realloc((void *)(&int_storage), 3);
            }
        }
    }
    free(int_storage);
    int_storage = NULL;
    return new_time;

}

char **time_struct_to_str(time_struct_t *time) {

    char **ret_arr = (char **)malloc(sizeof(char *) * 2);
    char *hour_str = (char *)malloc(sizeof(char) * HOUR_STR_LEN);
    char *date_str = (char *)malloc(sizeof(char) * DATE_STR_LEN);
    assert(hour_str);
    assert(date_str);

    sprintf(hour_str, "%d%.2d%.2d %.2d:00:00", time->year, time->month, time->day, time->hour);
    sprintf(date_str, "%d%.2d%.2d", time->year, time->month, time->day);

    hour_str[HOUR_STR_LEN - 1] = '\0';
    date_str[DATE_STR_LEN - 1] = '\0';

    ret_arr[0] = hour_str;
    ret_arr[1] = date_str;

    return ret_arr;
    
}